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
        # self.log_path = log_path
        # self.status_path = status_path
        self.exit_code = None
        self.duration = None

    async def run(self, secrets):
        image = self.spec['image']
        command = self.spec['command']

        volume_mounts = []
        for mount in self.spec['volume_mounts']:
            mount_name = mount['name']
            mount_path = mount['mount_path']
            if mount_name in secrets:
                secret_path = secrets[mount_name]
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

        self._container = await docker.containers.run(config)
        await self._container.wait()
        self._container = await docker.containers.get(self._container._id)

        self.exit_code = self._container['State']['ExitCode']

        started = dateutil.parser.parse(self._container['State']['StartedAt'])
        finished = dateutil.parser.parse(self._container['State']['FinishedAt'])
        self.duration = (finished - started).total_seconds()

        # await check_shell(f'docker logs {self.container._id} 2>&1 | gsutil cp - {shq(self.log_path)}')
        # await check_shell(f'docker inspect {self.container._id} | gsutil cp - {shq(self.status_path)}')

    async def delete(self):
        await self._container.stop()
        await self._container.delete()

    def status(self):
        return self._container._container

    async def log(self):
        logs = await self._container.log(stderr=True, stdout=True)
        return "".join(logs)

    def to_dict(self):
        status = self.status()

        state = {}
        if status['State']['running']:
            state['running'] = {
                'started_at': status['State']['StartedAt']
            }
        elif status['State']['exited']:
            state['terminated'] = {
                'container_id': status['Id'],
                'exit_code': status['State']['ExitCode'],
                'finished_at': status['State']['FinishedAt'],
                'message': status['State']['Error'],
                # 'reason': None,
                # 'signal': None,
                'started_at': status['State']['StartedAt']
            }
        else:
            state['waiting'] = {
                'message': None,
                'reason': None
            }

        return {
            # 'container_id': status['Id'],
            # 'image': status['Image'],
            # 'image_id': None,
            # 'last_state': None,
            'name': self.name,
            # 'ready': None,
            # 'restart_count': status['RestartCount'],
            'state': state
        }
        # return {
        #     'status': self.status(),
        #     'name': self.name,
        #     'exit_code': self.exit_code,
        #     'duration': self.duration
        # }


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

    def __init__(self, parameters):
        self.spec = parameters['spec']
        self.secrets = parameters['secrets']
        self.name = self.spec['metadata']['name']
        self.token = uuid.uuid4().hex

        self.containers = {cspec['name']: Container(cspec) for cspec in self.spec['spec']['containers']}
        self.volumes = []
        self.exit_codes = [None for _ in self.containers]
        self.durations = [None for _ in self.containers]
        self.running = False

    async def run(self, semaphore=None):
        # volume = await docker.volumes.create()
        # volume = await docker.volumes.create({}) # {'DriverOpts': {'o': 'size=100M', 'type': 'btrfs', 'device': '/dev/sda2'}}
        self.running = True

        secrets = self._create_secrets()

        if not semaphore:
            semaphore = NullWeightedSemaphore()

        for idx, (_, container) in enumerate(self.containers.items()):
            async with semaphore(container.cores):
                await container.run(secrets)

                self.exit_codes.append(container.exit_code)
                self.durations.append(container.duration)

                if container.exit_code != 0:
                    break

        self.running = False

        # FIXME: send success message back to driver

    async def delete(self):
        await asyncio.gather(*[c.delete() for _, c in self.containers.items()])
        # await self.volume.delete()

    async def log(self, container_name):
        c = self.containers[container_name]
        return await c.log()

    async def container_status(self, container_name):
        c = self.containers[container_name]
        return await c.status()

    def to_dict(self):
        if self.running:
            phase = 'Running'
        elif all([c.exit_code == 0 for _, c in self.containers.items()]):
            phase = 'Succeeded'
        else:
            phase = 'Failed'

        return {
            'metadata': {
                'name': self.name
            },
            'status': {
                'container_statuses': [c.to_dict() for _, c in self.containers.items()],
                'phase': phase
                # 'start_time': None
            }
        }


@routes.post('/api/v1alpha/pods/create')
async def create_pod(request):
    parameters = await request.json()
    try:
        bp = BatchPod(parameters)
        batch_pods[bp.name] = bp
        asyncio.ensure_future(bp.run())
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

#
# @routes.post('/api/v1alpha/pods/{pod_name}/status')
# async def get_pod_status(request):
#     pod_name = request.match_info['pod_name']
#     bp = batch_pods[pod_name]
#     await bp.log()
#     return web.Response()


@routes.post('/api/v1alpha/pods/{pod_name}/delete')
async def delete_pod(request):
    pod_name = request.match_info['pod_name']

    if pod_name not in batch_pods:
        abort(404, 'unknown pod name')
    bp = batch_pods[pod_name]

    asyncio.ensure_future(bp.delete())
    del batch_pods[pod_name]
    return web.Response()


@routes.get('/api/v1alpha/pods')
async def list_pods(request):
    pods = [bp.to_dict() for _, bp in batch_pods.items()]
    return jsonify(pods)


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)