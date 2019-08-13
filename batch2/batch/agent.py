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
    @staticmethod
    async def create(config, volume, pod_name, secrets):
        name = pod_name + '-' + config['name']
        image = config['image']
        command = config['command']
        cores = 1

        volume_mounts = []
        for mount in config['volume_mounts']:
            mount_name = mount['name']
            mount_path = mount['mount_path']
            if mount_name in secrets:
                secret_path = secrets['mount_name']
                volume_mounts.append(f'{secret_path}:{mount_path}')
            else:
                raise Exception(f'unknown secret {mount_name} specified in volume_mounts')

        # cores = config['resources']['requests']['cpu']
        # if cores is None:
        #     cores = '1'

        spec = {
            "AttachStdin": False,
            "AttachStdout": False,
            "AttachStderr": False,
            "Tty": False,
            'OpenStdin': False,
            'Binds': [f'{volume}:/io'],
            'name': name,
            'Cmd': command,
            'Image': image,
        }
        # spec.update(extra_params)
        print(f'creating container {name}')

        try:
            c = await docker.containers.create(config=spec)
        except DockerError as err:
            if err.status == 404 and 'Image' in spec:
                await docker.pull(spec['Image'])  # FIXME: figure out errors
                c = await docker.containers.create(config=spec)
            else:
                raise err
        return Container(c, name, cores)

    def __init__(self, container, name, cores, log_path=None):
        self.container = container
        self.name = name
        self.cores = cores
        self.log_path = log_path
        # self.status_path = status_path
        self.exit_code = None
        self.duration = None

    async def run(self):
        start = time.time()
        await self.container.start()
        await self.reload()
        if self.container['State']['Status'] != 'exited':
            await self.container.wait()
            await self.reload()

        # FIXME
        self.exit_code = self.container['State']['ExitCode']

        started = dateutil.parser.parse(self.container['State']['StartedAt'])
        finished = dateutil.parser.parse(self.container['State']['FinishedAt'])
        self.duration = (finished - started).total_seconds()

        # await check_shell(f'docker logs {self.container._id} 2>&1 | gsutil cp - {shq(self.log_path)}')
        # await check_shell(f'docker ... | gsutil cp - {shq(self.status_path)}')

    async def delete(self):
        await self.container.delete()

    async def reload(self):
        self.container = await docker.containers.get(self.container._id)

    def status(self):
        return self.container._container

    async def log(self):
        logs = await self.container.log(stderr=True, stdout=True)
        return "".join(logs)

    def to_dict(self):
        return {
            'name': self.name,
            'exit_code': self.exit_code,
            'duration': self.duration,
            # 'status': self.status()
        }


class BatchPod:
    @staticmethod
    async def create(config, secrets):
        name = config['metadata']['name']
        token = uuid.uuid4().hex

        print(f'creating batch pod {name}')

        secret_paths = {}
        for secret_name, secret in secrets.items():
            print(f'creating secret {secret_name}')

            path = f'/batch/pods/{name}/{token}/secrets/{secret_name}'
            os.makedirs(path)
            
            for file_name, data in secret.items():
                print(f'creating secret {secret_name} at path {path}/{file_name}')
                with open(f'{path}/{file_name}', 'w') as f:
                    f.write(base64.b64decode(data))
                    print(f'wrote secret')
            secret_paths[secret_name] = path

        volume = await docker.volumes.create({}) # {'DriverOpts': {'o': 'size=100M', 'type': 'btrfs', 'device': '/dev/sda2'}}

        containers = await asyncio.gather(*[Container.create(container_config, volume.name, name, secret_paths)
                                            for container_config in config['spec']['containers']])
        return BatchPod(name, containers, volume, secrets)

    def __init__(self, name, containers, volume, secrets):
        # self.config = config
        self.name = name
        # self.volumes = {Volume(vol_config) for vol_config in config['volumes']}
        # self.secrets = {Secret(secret_config) for secret_config in config['secrets']}
        self.secrets = secrets
        self.containers = {c.name: c for c in containers}
        self.volume = volume

        self.exit_codes = [None for _ in self.containers]
        self.durations = [None for _ in self.containers]

        self.running = False

    async def run(self, semaphore=None):
        # volume = await docker.volumes.create()
        self.running = True

        if not semaphore:
            semaphore = NullWeightedSemaphore()

        for idx, (_, container) in enumerate(self.containers.items()):
            async with semaphore(container.cores):
                await container.run()
                ec = container.exit_code

                self.exit_codes.append(container.exit_code)
                self.durations.append(container.duration)

                if container.exit_code != 0:
                    break

        self.running = False
        return

    async def delete(self):
        await asyncio.gather(*[c.delete() for _, c in self.containers.items()])
        await self.volume.delete()

    async def log(self, container_name):
        c = self.containers[container_name]
        return await c.log()

    async def container_status(self, container_name):
        c = self.containers[container_name]
        return await c.status()

    async def to_dict(self):
        return {
            'name': self.name,
            'containers': [c.to_dict() for _, c in self.containers.items()]
        }


@routes.post('/api/v1alpha/pods/create')
async def create_pod(request):
    parameters = await request.json()
    config = parameters['spec']
    secrets = parameters['secrets']
    print(config)
    try:
        bp = await BatchPod.create(config, secrets)  # FIXME: this is blocking
        batch_pods[bp.name] = bp
        asyncio.ensure_future(bp.run())
    except DockerError as err:
        print(err)
        return web.Response(body=err.message, status=err.status)
    except Exception as err:
        print(err)
        raise err
    return web.Response()


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


@routes.post('/api/v1alpha/pods/{pod_name}/status')
async def get_pod_status(request):
    pod_name = request.match_info['pod_name']
    bp = batch_pods[pod_name]
    await bp.log()
    return web.Response()


@routes.post('/api/v1alpha/pods/{pod_name}/delete')
async def delete_pod(request):
    pod_name = request.match_info['pod_name']

    if pod_name not in batch_pods:
        abort(404, 'unknown pod name')
    bp = batch_pods[pod_name]

    asyncio.ensure_future(bp.delete())
    del batch_pods[pod_name]
    return web.Response()


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return web.Response()


app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)