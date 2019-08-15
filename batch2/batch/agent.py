import os
from shlex import quote as shq
import time
import random
import logging
import asyncio
import aiohttp
import base64
import uuid
from aiohttp import web
import uvloop
import aiodocker
import dateutil.parser
from aiodocker.exceptions import DockerError

# from hailtop import gear

from .utils import check_shell, check_shell_output, CalledProcessError, jsonify, abort
from .semaphore import NullWeightedSemaphore, WeightedSemaphore
from .log_store import LogStore

# gear.configure_logging()
log = logging.getLogger('batch2-agent')

uvloop.install()

docker = aiodocker.Docker()

app = web.Application()
routes = web.RouteTableDef()

batch_pods = {}


class Container:
    def __init__(self, spec):
        self._container = None
        self.name = spec['name']
        self.spec = spec
        self.cores = 1
        self.exit_code = None
        self.started = False

    async def create(self, secret_paths):
        image = self.spec['image']
        command = self.spec['command']

        volume_mounts = []
        for mount in self.spec['volume_mounts']:
            mount_name = mount['name']
            mount_path = mount['mount_path']
            if mount_name in secret_paths:
                secret_path = secret_paths[mount_name]
                volume_mounts.append(f'{secret_path}:{mount_path}')
            else:
                raise Exception(f'unknown secret {mount_name} specified in volume_mounts')

        config = {
            "AttachStdin": False,
            "AttachStdout": False,
            "AttachStderr": False,
            "Tty": False,
            'OpenStdin': False,
            'Binds': volume_mounts,
            'Cmd': command,
            'Image': image,
        }

        try:
            start = time.time()
            self._container = await docker.containers.create(config)
            print(f'took {time.time() - start} seconds to create container')
        except DockerError as err:
            if err.status == 404:
                try:
                    start = time.time()
                    await docker.pull(config['Image'])  # FIXME: if image not able to be pulled make ImagePullBackOff
                    print(f'took {time.time() - start} seconds to pull image')

                    start = time.time()
                    self._container = await docker.containers.create(config)
                    print(f'took {time.time() - start} seconds to create container')
                except DockerError as err:
                    raise err
            else:
                raise err

        start = time.time()
        self._container = await docker.containers.get(self._container._id)
        print(f'took {time.time() - start} seconds to get container')

    async def run(self, log_directory):
        start = time.time()
        await self._container.start()

        self.started = True
        await self._container.wait()
        self._container = await docker.containers.get(self._container._id)
        self.exit_code = self._container['State']['ExitCode']

        log_path = LogStore.container_log_path(log_directory, self.name)
        status_path = LogStore.container_status_path(log_directory, self.name)

        await check_shell(f'docker logs {self._container._id} 2>&1 | gsutil cp - {shq(log_path)}')  # WHY did this work without permissions?
        await check_shell(f'docker inspect {self._container._id} | gsutil cp - {shq(status_path)}')
        print(f'took {time.time() - start} seconds to run container')

    async def delete(self):
        if self._container is not None:
            await self._container.stop()
            await self._container.delete()

    @property
    def status(self):
        return self._container._container

    async def log(self):
        logs = await self._container.log(stderr=True, stdout=True)
        return "".join(logs)

    def to_dict(self):
        assert self._container is not None

        state = {}
        if self.status['State']['Status'] == 'created':
            state['waiting'] = {}
        elif self.status['State']['Status'] == 'running':
            state['running'] = {
                'started_at': self.status['State']['StartedAt']
            }
        elif self.status['State']['Status'] == 'exited':  # FIXME: there's other docker states such as dead
            state['terminated'] = {
                'exitCode': self.status['State']['ExitCode'],
                'finishedAt': self.status['State']['FinishedAt'],
                'message': self.status['State']['Error'],
                'startedAt': self.status['State']['StartedAt']
            }
        else:
            raise Exception(f'unknown docker state {self.status["State"]["Status"]}')

        return {
            'containerID': f'docker://{self.status["Id"]}',
            'image': self.spec['image'],
            'imageID': self.status['Image'],
            'name': self.name,
            'ready': False,
            'restartCount': self.status['RestartCount'],
            'state': state
        }


class BatchPod:
    def _create_secrets(self):
        secret_paths = {}
        for secret_name, secret in self.secrets.items():
            path = f'/batch/pods/{self.name}/{self.token}/secrets/{secret_name}'
            os.makedirs(path)

            for file_name, data in secret.items():
                with open(f'{path}/{file_name}', 'w') as f:
                    f.write(base64.b64decode(data).decode())
            secret_paths[secret_name] = path
        return secret_paths

    def _cleanup_secrets(self):
        for name, path in self.secret_paths.items():
            if os.path.exists(path):
                os.remove(path)
            del self.secret_paths[name]

    def __init__(self, parameters):
        self.spec = parameters['spec']
        self.secrets = parameters['secrets']
        self.secret_paths = {}
        self.output_directory = parameters['output_directory']

        self.metadata = self.spec['metadata']
        self.name = self.metadata['name']
        self.token = uuid.uuid4().hex

        self.containers = {cspec['name']: Container(cspec) for cspec in self.spec['spec']['containers']}
        self.volumes = []
        self.phase = 'Pending'
        self._run_task = asyncio.ensure_future(self.run())

    async def run(self, semaphore=None):
        try:
            # volume = await docker.volumes.create()
            # volume = await docker.volumes.create({}) # {'DriverOpts': {'o': 'size=100M', 'type': 'btrfs', 'device': '/dev/sda2'}}

            self.secret_paths = self._create_secrets()

            for cname, container in self.containers.items():
                await container.create(self.secret_paths)

            self.phase = 'Running'

            if not semaphore:
                semaphore = NullWeightedSemaphore()

            last_ec = None
            for _, container in self.containers.items():
                async with semaphore(container.cores):
                    await container.run(self.output_directory)
                    last_ec = container.exit_code
                    if last_ec != 0:
                        break

            self.phase = 'Succeeded' if last_ec == 0 else 'Failed'

            # FIXME: send message back to driver
        except asyncio.CancelledError:
            print(f'pod {self.name} was cancelled')

    async def cleanup(self):
        await asyncio.gather(*[asyncio.shield(c.delete()) for _, c in self.containers.items()])
        await self._cleanup_secrets()
        # await self.volume.delete()

    async def delete(self):
        self._run_task.cancel()
        try:
            await self._run_task
        finally:
            await asyncio.shield(self.cleanup())

    async def log(self, container_name):
        c = self.containers[container_name]
        return await c.log()

    def container_status(self, container_name):
        c = self.containers[container_name]
        return c.status

    def to_dict(self):
        if self.phase == 'Pending':
            container_statuses = None
        else:
            container_statuses = [c.to_dict() for _, c in self.containers.items()]

        return {
            'metadata': self.metadata,
            'status': {
                'containerStatuses': container_statuses,
                # 'hostIP': None,
                'phase': self.phase
                # 'startTime': None
            }
        }


@routes.post('/api/v1alpha/pods/create')
async def create_pod(request):
    parameters = await request.json()
    try:
        bp = BatchPod(parameters)
        batch_pods[bp.name] = bp
        # asyncio.ensure_future(bp.run())
    except DockerError as err:
        print(err)
        return web.Response(body=err.message, status=err.status)
    except Exception as err:
        print(err)
        raise err
    return jsonify({})


@routes.post('/api/v1alpha/pods/{pod_name}/containers/{container_name}/log')
async def get_container_log(request):
    pod_name = request.match_info['pod_name']
    container_name = request.match_info['container_name']

    if pod_name not in batch_pods:
        abort(404, 'unknown pod name')
    bp = batch_pods[pod_name]

    if container_name not in bp.containers:
        abort(404, 'unknown container name')
    result = await bp.log(container_name)

    return jsonify(result)


@routes.post('/api/v1alpha/pods/{pod_name}/containers/{container_name}/status')
async def get_container_status(request):
    pod_name = request.match_info['pod_name']
    container_name = request.match_info['container_name']

    if pod_name not in batch_pods:
        abort(404, 'unknown pod name')
    bp = batch_pods[pod_name]

    if container_name not in bp.containers:
        abort(404, 'unknown container name')
    result = bp.container_status(container_name)

    return jsonify(result)


@routes.post('/api/v1alpha/pods/{pod_name}')
async def get_pod(request):
    pod_name = request.match_info['pod_name']
    if pod_name not in batch_pods:
        abort(404, 'unknown pod name')
    bp = batch_pods[pod_name]
    return jsonify(bp.to_dict())


@routes.delete('/api/v1alpha/pods/{pod_name}/delete')
async def delete_pod(request):
    pod_name = request.match_info['pod_name']

    if pod_name not in batch_pods:
        abort(404, 'unknown pod name')
    bp = batch_pods[pod_name]

    asyncio.ensure_future(bp.delete())
    del batch_pods[pod_name]
    return jsonify({})


@routes.get('/api/v1alpha/pods')
async def list_pods(request):
    pods = [bp.to_dict() for _, bp in batch_pods.items()]
    return jsonify(pods)


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)