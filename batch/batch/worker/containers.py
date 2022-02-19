from typing import Optional, Dict, Callable, Tuple, Awaitable, Any, List, Union

import os
import json
import logging
import asyncio
import traceback
import async_timeout
import concurrent

from hailtop.utils import time_msecs, check_shell, CalledProcessError, check_exec_output
from hailtop import httpx
from hailtop.aiotools.fs import AsyncFS

from .config import IP_ADDRESS, CORES
from .network import NetworkNamespace, get_network_allocator, get_port_allocator
from .images import Image
from .exceptions import ImageCannotBePulled, ImageNotFound, ContainerDeletedError, ContainerTimeoutError
from .utils import Timings, StepManager, user_error


log = logging.getLogger('containers')


async def send_signal_and_wait(proc, signal, timeout=None):
    try:
        if signal == 'SIGTERM':
            proc.terminate()
        else:
            assert signal == 'SIGKILL'
            proc.kill()
    except ProcessLookupError:
        pass
    else:
        await asyncio.wait_for(proc.wait(), timeout=timeout)


def worker_fraction_in_1024ths(cpu_in_mcpu):
    return 1024 * cpu_in_mcpu // (CORES * 1000)


class Container:
    def __init__(self,
                 name: str,
                 image: Image,
                 scratch_dir: str,
                 command: List[str],
                 cpu: int,
                 memory: int,
                 network: Optional[Union[bool, str]] = None,
                 port: Optional[int] = None,
                 timeout: Optional[int] = None,
                 unconfined: Optional[bool] = None,
                 volume_mounts: Optional[List[dict]] = None,
                 env: Optional[List[Dict[str, str]]] = None):
        self.name = name
        self.image = image
        self.command = command
        self.cpu = cpu
        self.memory = memory
        self.network = network
        self.port = port
        self.timeout = timeout
        self.unconfined = unconfined
        self.volume_mounts = volume_mounts or []
        self.env = env or []

        self.deleted_event = asyncio.Event()

        self.host_port = None

        self.state = 'pending'
        self.error: Optional[str] = None
        self.short_error: Optional[str] = None
        self.container_status: Optional[dict] = None
        self.started_at: Optional[int] = None
        self.finished_at: Optional[int] = None

        self.timings = Timings(self.deleted_event.is_set)

        self.overlay_path = None

        self.image_config = None
        self.rootfs_path = None

        self.container_scratch = f'{scratch_dir}/{self.name}'
        self.container_overlay_path = f'{self.container_scratch}/rootfs_overlay'
        self.config_path = f'{self.container_scratch}/config'
        self.log_path = f'{self.container_scratch}/container.log'

        self.overlay_mounted = False

        self.netns: Optional[NetworkNamespace] = None
        # regarding no-member: https://github.com/PyCQA/pylint/issues/4223
        self.process: Optional[asyncio.subprocess.Process] = None  # pylint: disable=no-member

    async def run(self, client_session: httpx.ClientSession, pool: concurrent.futures.ThreadPoolExecutor):
        try:
            with self._step('pulling'):
                await self._run_until_done_or_deleted(self.image.pull, client_session, pool)

            with self._step('setting up overlay'):
                await self._run_until_done_or_deleted(self._setup_overlay)

            with self._step('setting up network'):
                await self._run_until_done_or_deleted(self._setup_network_namespace)

            with self._step('running'):
                timed_out = await self._run_until_done_or_deleted(self._run_container)

            self.container_status = await self.get_container_status()
            assert self.container_status is not None

            if timed_out:
                self.short_error = 'timed out'
                raise ContainerTimeoutError(f'timed out after {self.timeout}s')

            if self.container_status['exit_code'] == 0:
                self.state = 'succeeded'
            else:
                if self.container_status['out_of_memory']:
                    self.short_error = 'out of memory'
                self.state = 'failed'
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if not isinstance(e, (ContainerDeletedError, ContainerTimeoutError)) and not user_error(e):
                log.exception(f'while running {self}')

            if isinstance(e, ImageNotFound):
                self.short_error = 'image not found'
            elif isinstance(e, ImageCannotBePulled):
                self.short_error = 'image cannot be pulled'

            self.state = 'error'
            self.error = traceback.format_exc()
        finally:
            try:
                await self.delete_container()
            finally:
                self.image.prune()

    async def _run_until_done_or_deleted(self, f: Callable[..., Awaitable[Any]], *args, **kwargs):
        step = asyncio.ensure_future(f(*args, **kwargs))
        deleted = asyncio.ensure_future(self.deleted_event.wait())
        try:
            await asyncio.wait([deleted, step], return_when=asyncio.FIRST_COMPLETED)
            if deleted.done():
                raise ContainerDeletedError()
            assert step.done()
            return step.result()
        finally:
            for t in (step, deleted):
                if t.done():
                    e = t.exception()
                    if e and not user_error(e):
                        log.exception(e)
                else:
                    t.cancel()

    def _step(self, name: str, ignore_cancellation: bool = False) -> StepManager:
        return self.timings.step(name, ignore_cancellation=ignore_cancellation)

    async def _setup_overlay(self):
        lower_dir = self.rootfs_path
        upper_dir = f'{self.container_overlay_path}/upper'
        work_dir = f'{self.container_overlay_path}/work'
        merged_dir = f'{self.container_overlay_path}/merged'
        for d in (upper_dir, work_dir, merged_dir):
            os.makedirs(d)
        await check_shell(
            f'mount -t overlay overlay -o lowerdir={lower_dir},upperdir={upper_dir},workdir={work_dir} {merged_dir}'
        )
        self.overlay_mounted = True

    async def _setup_network_namespace(self):
        network = self.network
        if network is None or network is True:  # FIXME: what about public as in validator
            self.netns = await get_network_allocator().allocate_public()
        else:
            assert network == 'private'
            self.netns = await get_network_allocator().allocate_private()
        if self.port is not None:
            self.host_port = await get_port_allocator().allocate()
            await self.netns.expose_port(self.port, self.host_port)

    async def _run_container(self) -> bool:
        self.started_at = time_msecs()
        try:
            await self._write_container_config()
            async with async_timeout.timeout(self.timeout):
                with open(self.log_path, 'w') as container_log:
                    log.info(f'Creating the crun run process for {self}')
                    self.process = await asyncio.create_subprocess_exec(
                        'crun',
                        'run',
                        '--bundle',
                        f'{self.container_overlay_path}/merged',
                        '--config',
                        f'{self.config_path}/config.json',
                        self.name,
                        stdout=container_log,
                        stderr=container_log,
                    )
                    await self.process.wait()
                    log.info(f'crun process completed for {self}')
        except asyncio.TimeoutError:
            return True
        finally:
            self.finished_at = time_msecs()

        return False

    async def _write_container_config(self):
        os.makedirs(self.config_path)
        with open(f'{self.config_path}/config.json', 'w') as f:
            f.write(json.dumps(await self.container_config()))

    # https://github.com/opencontainers/runtime-spec/blob/master/config.md
    async def container_config(self):
        uid, gid = await self._get_in_container_user()
        weight = worker_fraction_in_1024ths(self.cpu)
        workdir = self.image_config['Config']['WorkingDir']
        default_docker_capabilities = [
            'CAP_CHOWN',
            'CAP_DAC_OVERRIDE',
            'CAP_FSETID',
            'CAP_FOWNER',
            'CAP_MKNOD',
            'CAP_NET_RAW',
            'CAP_SETGID',
            'CAP_SETUID',
            'CAP_SETFCAP',
            'CAP_SETPCAP',
            'CAP_NET_BIND_SERVICE',
            'CAP_SYS_CHROOT',
            'CAP_KILL',
            'CAP_AUDIT_WRITE',
        ]
        config = {
            'ociVersion': '1.0.1',
            'root': {
                'path': '.',
                'readonly': False,
            },
            'hostname': self.netns.hostname,
            'mounts': self._mounts(uid, gid),
            'process': {
                'user': {  # uid/gid *inside the container*
                    'uid': uid,
                    'gid': gid,
                },
                'args': self.command,
                'env': self._env(),
                'cwd': workdir if workdir != "" else "/",
                'capabilities': {
                    'bounding': default_docker_capabilities,
                    'effective': default_docker_capabilities,
                    'inheritable': default_docker_capabilities,
                    'permitted': default_docker_capabilities,
                },
            },
            'linux': {
                'namespaces': [
                    {'type': 'pid'},
                    {
                        'type': 'network',
                        'path': f'/var/run/netns/{self.netns.network_ns_name}',
                    },
                    {'type': 'mount'},
                    {'type': 'ipc'},
                    {'type': 'uts'},
                    {'type': 'cgroup'},
                ],
                'uidMappings': [],
                'gidMappings': [],
                'resources': {
                    'cpu': {'shares': weight},
                    'memory': {
                        'limit': self.memory,
                        'reservation': self.memory,
                    },
                    # 'blockIO': {'weight': min(weight, 1000)}, FIXME blkio.weight not supported
                },
                'maskedPaths': [
                    '/proc/asound',
                    '/proc/acpi',
                    '/proc/kcore',
                    '/proc/keys',
                    '/proc/latency_stats',
                    '/proc/timer_list',
                    '/proc/timer_stats',
                    '/proc/sched_debug',
                    '/proc/scsi',
                    '/sys/firmware',
                ],
                'readonlyPaths': [
                    '/proc/bus',
                    '/proc/fs',
                    '/proc/irq',
                    '/proc/sys',
                    '/proc/sysrq-trigger',
                ],
            },
        }

        if self.unconfined:  # FIXME: what is the default for this?
            config['linux']['maskedPaths'] = []
            config['linux']['readonlyPaths'] = []
            config['process']['apparmorProfile'] = 'unconfined'
            config['linux']['seccomp'] = {'defaultAction': "SCMP_ACT_ALLOW"}

        return config

    async def _get_in_container_user(self):
        user = self.image_config['Config']['User']
        if not user:
            uid, gid = 0, 0
        elif ":" in user:
            uid, gid = user.split(":")
        else:
            uid, gid = await self._read_user_from_rootfs(user)
        return int(uid), int(gid)

    async def _read_user_from_rootfs(self, user) -> Tuple[str, str]:
        with open(f'{self.rootfs_path}/etc/passwd', 'r') as passwd:
            for record in passwd:
                if record.startswith(user):
                    _, _, uid, gid, _, _, _ = record.split(":")
                    return uid, gid
            raise ValueError("Container user not found in image's /etc/passwd")

    def _mounts(self, uid, gid):
        # Only supports empty volumes
        external_volumes = []
        volumes = self.image_config['Config']['Volumes']
        if volumes:
            for v_container_path in volumes:
                if not v_container_path.startswith('/'):
                    v_container_path = '/' + v_container_path
                v_host_path = f'{self.container_scratch}/volumes{v_container_path}'
                os.makedirs(v_host_path)
                if uid != 0 or gid != 0:
                    os.chown(v_host_path, uid, gid)
                external_volumes.append(
                    {
                        'source': v_host_path,
                        'destination': v_container_path,
                        'type': 'none',
                        'options': ['rbind', 'rw', 'shared'],
                    }
                )

        return (
            self.volume_mounts
            + external_volumes
            + [
                # Recommended filesystems:
                # https://github.com/opencontainers/runtime-spec/blob/master/config-linux.md#default-filesystems
                {
                    'source': 'proc',
                    'destination': '/proc',
                    'type': 'proc',
                    'options': ['nosuid', 'noexec', 'nodev'],
                },
                {
                    'source': 'tmpfs',
                    'destination': '/dev',
                    'type': 'tmpfs',
                    'options': ['nosuid', 'strictatime', 'mode=755', 'size=65536k'],
                },
                {
                    'source': 'sysfs',
                    'destination': '/sys',
                    'type': 'sysfs',
                    'options': ['nosuid', 'noexec', 'nodev', 'ro'],
                },
                {
                    'source': 'cgroup',
                    'destination': '/sys/fs/cgroup',
                    'type': 'cgroup',
                    'options': ['nosuid', 'noexec', 'nodev', 'ro'],
                },
                {
                    'source': 'devpts',
                    'destination': '/dev/pts',
                    'type': 'devpts',
                    'options': ['nosuid', 'noexec', 'nodev'],
                },
                {
                    'source': 'mqueue',
                    'destination': '/dev/mqueue',
                    'type': 'mqueue',
                    'options': ['nosuid', 'noexec', 'nodev'],
                },
                {
                    'source': 'shm',
                    'destination': '/dev/shm',
                    'type': 'tmpfs',
                    'options': ['nosuid', 'noexec', 'nodev', 'mode=1777', 'size=67108864'],
                },
                {
                    'source': f'/etc/netns/{self.netns.network_ns_name}/resolv.conf',
                    'destination': '/etc/resolv.conf',
                    'type': 'none',
                    'options': ['rbind', 'ro'],
                },
                {
                    'source': f'/etc/netns/{self.netns.network_ns_name}/hosts',
                    'destination': '/etc/hosts',
                    'type': 'none',
                    'options': ['rbind', 'ro'],
                },
            ]
        )

    def _env(self):
        env = self.image_config['Config']['Env'] + self.env
        if self.port is not None:
            assert self.host_port is not None
            env.append(f'HAIL_BATCH_WORKER_PORT={self.host_port}')
            env.append(f'HAIL_BATCH_WORKER_IP={IP_ADDRESS}')
        return env

    async def delete_container(self):
        if self.container_is_running():
            assert self.process is not None
            try:
                log.info(f'{self} container is still running, killing crun process')
                try:
                    await check_exec_output('crun', 'kill', '--all', self.name, 'SIGKILL')
                except CalledProcessError as e:
                    not_extant_message = (
                        b'error opening file `/run/crun/'
                        + self.name.encode()
                        + b'/status`: No such file or directory'
                    )
                    if not (e.returncode == 1 and not_extant_message in e.stderr):
                        log.exception(f'while deleting container {self}', exc_info=True)
            finally:
                try:
                    await send_signal_and_wait(self.process, 'SIGTERM', timeout=5)
                except asyncio.TimeoutError:
                    try:
                        await send_signal_and_wait(self.process, 'SIGKILL', timeout=5)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception(f'could not kill process for container {self}')
                finally:
                    self.process = None

        if self.overlay_mounted:
            try:
                await check_shell(f'umount -l {self.container_overlay_path}/merged')
                self.overlay_mounted = False
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(f'while unmounting overlay in {self}', exc_info=True)

        if self.host_port is not None:
            get_port_allocator().free(self.host_port)
            self.host_port = None

        if self.netns:
            get_network_allocator().free(self.netns)
            self.netns = None

    async def delete(self):
        log.info(f'deleting {self}')
        self.deleted_event.set()

    # {
    #   name: str,
    #   state: str, (pending, pulling, creating, starting, running, uploading_log, deleting, succeeded, error, failed)
    #   timing: dict(str, float),
    #   error: str, (optional)
    #   short_error: str, (optional)
    #   container_status: {
    #     state: str,
    #     started_at: int, (date)
    #     finished_at: int, (date)
    #     out_of_memory: bool,
    #     exit_code: int
    #   }
    # }
    async def status(self, state: Optional[str] = None) -> dict:
        if not state:
            state = self.state
        status = {'name': self.name, 'state': state, 'timing': self.timings.to_dict()}
        if self.error:
            status['error'] = self.error
        if self.short_error:
            status['short_error'] = self.short_error
        if self.container_status:
            status['container_status'] = self.container_status
        elif self.container_is_running():
            status['container_status'] = await self.get_container_status()
        return status

    async def get_container_status(self) -> Optional[dict]:
        if not self.process:
            return None

        status: dict

        status = {
            'started_at': self.started_at,
            'finished_at': self.finished_at,
        }
        if self.container_is_running():
            status['state'] = 'running'
            status['out_of_memory'] = False
        else:
            status['state'] = 'finished'
            status['exit_code'] = self.process.returncode
            status['out_of_memory'] = self.process.returncode == 137

        return status

    def container_is_running(self):
        return self.process is not None and self.process.returncode is None

    def container_finished(self):
        return self.process is not None and self.process.returncode is not None

    async def get_log(self, fs: AsyncFS):
        if os.path.exists(self.log_path):
            stream = await fs.open(self.log_path)
            async with stream:
                return (await stream.read()).decode()
        return ''

    def __str__(self):
        return f'container {self.name}'
