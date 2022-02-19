from typing import TYPE_CHECKING

import concurrent.futures
import logging
import json
import os
import asyncio
import shutil
import traceback

from hailtop import aiotools, httpx
from hailtop.utils import time_msecs, check_shell_output, check_shell, blocking_to_async

from ...cloud.resource_utils import storage_gib_to_bytes

from ..credentials import CloudUserCredentials
from ..utils import Timings, populate_secret_host_path, user_error
from ..config import instance_config, CLOUD_WORKER_API, NAME, NAMESPACE, BATCH_WORKER_IMAGE
from ..containers import Container
from ..images import Image

from .base import Job

if TYPE_CHECKING:
    from ..worker import Worker  # pylint: disable=cyclic-import

log = logging.getLogger('docker_job')


class Task:
    def __init__(self,
                 job: 'DockerJob',
                 name: str,
                 spec: dict,
                 client_session: httpx.ClientSession,
                 worker: 'Worker'):
        self.job = job
        self.name = name
        self.spec = spec
        self.client_session = client_session
        self.worker = worker

        container_name = f'batch-{self.job.batch_id}-job-{self.job.job_id}-{self.name}'
        image = Image(spec['image'], job.credentials)

        self.container = Container(
            container_name,
            image,
            self.spec['scratch'],
            self.spec['command'],
            self.spec['cpu'],
            self.spec['memory'],
            self.spec.get('network'),
            self.spec.get('port'),
            self.spec.get('timeout'),
            self.spec.get('unconfined'),
            self.spec.get('volume_mounts'),
            self.spec.get('env')
        )

    @property
    def state(self):
        return self.container.state

    async def run(self):
        try:
            await self.container.run(self.client_session, self.worker.pool)
        finally:
            with self.container._step('uploading_log', ignore_cancellation=True):
                await self.upload_log()

    async def delete(self):
        await self.container.delete()

    async def status(self, state=None):
        return await self.container.status(state=state)

    async def get_log(self):
        return await self.container.get_log(self.worker.fs)

    async def upload_log(self):
        await self.worker.file_store.write_log_file(
            self.job.format_version,
            self.job.batch_id,
            self.job.job_id,
            self.job.attempt_id,
            self.name,
            await self.get_log(),
        )


def copy_task(
    job: 'DockerJob',
    name: str,
    files,
    volume_mounts,
    cpu,
    memory,
    scratch: str,
    requester_pays_project: str,
    client_session: httpx.ClientSession,
    worker: 'Worker',
) -> Task:
    assert files
    copy_spec = {
        'image': BATCH_WORKER_IMAGE,
        'name': name,
        'command': [
            '/usr/bin/python3',
            '-m',
            'hailtop.aiotools.copy',
            json.dumps(requester_pays_project),
            json.dumps(files),
            '-v',
        ],
        'env': [f'{job.credentials.cloud_env_name}={job.credentials.mount_path}'],
        'cpu': cpu,
        'memory': memory,
        'scratch': scratch,
        'volume_mounts': volume_mounts,
    }
    return Task(job, name, copy_spec, client_session, worker)


class DockerJob(Job):
    def __init__(
        self,
        batch_id: int,
        user: str,
        credentials: CloudUserCredentials,
        job_spec,
        format_version,
        task_manager: aiotools.BackgroundTaskManager,
        pool: concurrent.futures.ThreadPoolExecutor,
        client_session: httpx.ClientSession,
        worker: 'Worker',
    ):
        super().__init__(batch_id, user, credentials, job_spec, format_version, task_manager, pool, worker)
        input_files = job_spec.get('input_files')
        output_files = job_spec.get('output_files')

        requester_pays_project = job_spec.get('requester_pays_project')

        self.timings = Timings(lambda: False)

        if self.secrets:
            for secret in self.secrets:
                volume_mount = {
                    'source': self.secret_host_path(secret),
                    'destination': secret["mount_path"],
                    'type': 'none',
                    'options': ['rbind', 'rw'],
                }
                self.main_volume_mounts.append(volume_mount)
                # this will be the user credentials
                if secret.get('mount_in_copy', False):
                    self.input_volume_mounts.append(volume_mount)
                    self.output_volume_mounts.append(volume_mount)

        # create tasks
        tasks = {}

        if input_files:
            tasks['input'] = copy_task(
                self,
                'input',
                input_files,
                self.input_volume_mounts,
                self.cpu_in_mcpu,
                self.memory_in_bytes,
                self.scratch,
                requester_pays_project,
                client_session,
                worker,
            )

        # main container
        main_spec = {
            'command': job_spec['process']['command'],
            'image': job_spec['process']['image'],
            'name': 'main',
            'env': [f'{var["name"]}={var["value"]}' for var in self.env],
            'cpu': self.cpu_in_mcpu,
            'memory': self.memory_in_bytes,
            'volume_mounts': self.main_volume_mounts,
        }
        port = job_spec.get('port')
        if port:
            main_spec['port'] = port
        timeout = job_spec.get('timeout')
        if timeout:
            main_spec['timeout'] = timeout
        network = job_spec.get('network')
        if network:
            assert network in ('public', 'private')
            main_spec['network'] = network
        unconfined = job_spec.get('unconfined')
        if unconfined:
            main_spec['unconfined'] = unconfined
        main_spec['scratch'] = self.scratch
        tasks['main'] = Task(self, 'main', main_spec, client_session, worker)

        if output_files:
            tasks['output'] = copy_task(
                self,
                'output',
                output_files,
                self.output_volume_mounts,
                self.cpu_in_mcpu,
                self.memory_in_bytes,
                self.scratch,
                requester_pays_project,
                client_session,
                worker,
            )

        self.tasks = tasks

    def step(self, name: str):
        return self.timings.step(name)

    async def setup_io(self):
        if not instance_config.job_private:
            if self.worker.data_disk_space_remaining.value < self.external_storage_in_gib:
                log.info(
                    f'worker data disk storage is full: {self.external_storage_in_gib}Gi requested and {self.worker.data_disk_space_remaining}Gi remaining'
                )

                # disk name must be 63 characters or less
                # https://cloud.google.com/compute/docs/reference/rest/v1/disks#resource:-disk
                # under the information for the name field
                uid = self.token[:20]
                self.disk = CLOUD_WORKER_API.create_disk(
                    instance_name=NAME,
                    disk_name=f'batch-disk-{uid}',
                    size_in_gb=self.external_storage_in_gib,
                    mount_path=self.io_host_path(),
                )
                labels = {'namespace': NAMESPACE, 'batch': '1', 'instance-name': NAME, 'uid': uid}
                await self.disk.create(labels=labels)
                log.info(f'created disk {self.disk.name} for job {self.id}')
                return

            self.worker.data_disk_space_remaining.value -= self.external_storage_in_gib
            log.info(
                f'acquired {self.external_storage_in_gib}Gi from worker data disk storage with {self.worker.data_disk_space_remaining}Gi remaining'
            )

        assert self.disk is None, self.disk
        os.makedirs(self.io_host_path())

    async def run(self):
        async with self.worker.cpu_sem(self.cpu_in_mcpu):
            self.start_time = time_msecs()

            try:
                self.task_manager.ensure_future(self.worker.post_job_started(self))

                log.info(f'{self}: initializing')
                self.state = 'initializing'

                os.makedirs(f'{self.scratch}/')

                with self.step('setup_io'):
                    await self.setup_io()

                if not self.disk:
                    data_disk_storage_in_bytes = storage_gib_to_bytes(
                        self.external_storage_in_gib + self.data_disk_storage_in_gib
                    )
                else:
                    data_disk_storage_in_bytes = storage_gib_to_bytes(self.data_disk_storage_in_gib)

                with self.step('configuring xfsquota'):
                    # Quota will not be applied to `/io` if the job has an attached disk mounted there
                    await check_shell_output(f'xfs_quota -x -c "project -s -p {self.scratch} {self.project_id}" /host/')
                    await check_shell_output(
                        f'xfs_quota -x -c "limit -p bsoft={data_disk_storage_in_bytes} bhard={data_disk_storage_in_bytes} {self.project_id}" /host/'
                    )

                with self.step('populating secrets'):
                    if self.secrets:
                        for secret in self.secrets:
                            populate_secret_host_path(self.secret_host_path(secret), secret['data'])

                with self.step('adding cloudfuse support'):
                    if self.cloudfuse:
                        os.makedirs(self.cloudfuse_base_path())

                        await check_shell_output(
                            f'xfs_quota -x -c "project -s -p {self.cloudfuse_base_path()} {self.project_id}" /host/'
                        )

                        for config in self.cloudfuse:
                            bucket = config['bucket']
                            assert bucket

                            credentials = self.credentials.cloudfuse_credentials(config)
                            credentials_path = CLOUD_WORKER_API.write_cloudfuse_credentials(
                                self.scratch, credentials, bucket
                            )

                            os.makedirs(self.cloudfuse_data_path(bucket), exist_ok=True)
                            os.makedirs(self.cloudfuse_tmp_path(bucket), exist_ok=True)

                            await CLOUD_WORKER_API.mount_cloudfuse(
                                credentials_path,
                                self.cloudfuse_data_path(bucket),
                                self.cloudfuse_tmp_path(bucket),
                                config,
                            )
                            config['mounted'] = True

                self.state = 'running'

                input = self.tasks.get('input')
                if input:
                    log.info(f'{self}: running input')
                    await input.run()
                    log.info(f'{self} input: {input.state}')

                if not input or input.state == 'succeeded':
                    log.info(f'{self}: running main')

                    main = self.tasks['main']
                    await main.run()

                    log.info(f'{self} main: {main.state}')

                    output = self.tasks.get('output')
                    if output:
                        log.info(f'{self}: running output')
                        await output.run()
                        log.info(f'{self} output: {output.state}')

                    if main.state != 'succeeded':
                        self.state = main.state
                    elif output:
                        self.state = output.state
                    else:
                        self.state = 'succeeded'
                else:
                    self.state = input.state
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if not user_error(e):
                    log.exception(f'while running {self}')

                self.state = 'error'
                self.error = traceback.format_exc()
            finally:
                with self.step('post-job finally block'):
                    if self.disk:
                        try:
                            await self.disk.delete()
                            log.info(f'deleted disk {self.disk.name} for {self.id}')
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            log.exception(f'while detaching and deleting disk {self.disk.name} for {self.id}')
                        finally:
                            await self.disk.close()
                    else:
                        self.worker.data_disk_space_remaining.value += self.external_storage_in_gib

                    await self.cleanup()

    async def cleanup(self):
        self.end_time = time_msecs()

        if not self.deleted:
            log.info(f'{self}: marking complete')
            self.task_manager.ensure_future(self.worker.post_job_complete(self))

        log.info(f'{self}: cleaning up')
        try:
            if self.cloudfuse:
                for config in self.cloudfuse:
                    if config['mounted']:
                        bucket = config['bucket']
                        assert bucket
                        mount_path = self.cloudfuse_data_path(bucket)
                        await CLOUD_WORKER_API.unmount_cloudfuse(mount_path)
                        log.info(f'unmounted fuse blob storage {bucket} from {mount_path}')
                        config['mounted'] = False

            await check_shell(f'xfs_quota -x -c "limit -p bsoft=0 bhard=0 {self.project_id}" /host')

            await blocking_to_async(self.pool, shutil.rmtree, self.scratch, ignore_errors=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('while deleting volumes')

    async def get_log(self):
        return {name: await t.get_log() for name, t in self.tasks.items()}

    async def delete(self):
        await super().delete()
        await asyncio.wait([t.delete() for t in self.tasks.values()])

    async def status(self):
        status = await super().status()
        cstatuses = {name: await t.status() for name, t in self.tasks.items()}
        status['container_statuses'] = cstatuses
        status['timing'] = self.timings.to_dict()

        return status

    def __str__(self):
        return f'job {self.id}'
