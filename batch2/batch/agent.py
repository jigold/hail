import abc
import os
from shlex import quote as shq
import time
import random
import logging
import asyncio
import json
import aiohttp
import base64
import uuid
import shutil
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
    def __init__(self, spec, pod):
        self._container = None
        self.name = spec['name']
        self.spec = spec
        self.cores = 1
        self.exit_code = None
        self.started = False
        self.id = pod.name + '-' + self.name

    async def create(self, volumes):
        print(f'creating container {self.id}')

        image = self.spec['image']
        command = self.spec['command']

        volume_mounts = []
        for mount in self.spec['volume_mounts']:
            mount_name = mount['name']
            mount_path = mount['mount_path']
            if mount_name in volumes:
                volume_path = volumes[mount_name].path
                volume_mounts.append(f'{volume_path}:{mount_path}')
            else:
                raise Exception(f'unknown volume {mount_name} specified in volume_mounts')

        config = {
            "AttachStdin": False,
            "AttachStdout": False,
            "AttachStderr": False,
            "Tty": False,
            'OpenStdin': False,
            'Binds': volume_mounts,
            'Cmd': command,
            'Image': image
        }

        try:
            start = time.time()
            self._container = await docker.containers.create(config, name=self.id)
            print(f'took {time.time() - start} seconds to create container {self.id}')
        except DockerError as err:
            if err.status == 404:
                try:
                    start = time.time()
                    await docker.pull(config['Image'])  # FIXME: if image not able to be pulled make ImagePullBackOff
                    print(f'took {time.time() - start} seconds to pull image {image} for {self.id}')

                    start = time.time()
                    self._container = await docker.containers.create(config)
                    print(f'took {time.time() - start} seconds to create container for {self.id}')
                except DockerError as err:
                    raise err
            else:
                raise err

        start = time.time()
        self._container = await docker.containers.get(self._container._id)
        print(f'took {time.time() - start} seconds to get container for {self.id}')

    async def run(self, log_directory):
        start = time.time()

        start2 = time.time()
        await self._container.start()
        print(f'took {time.time() - start2} seconds to start container for {self.id}')

        self.started = True
        start2 = time.time()
        await self._container.wait()
        print(f'took {time.time() - start2} seconds to wait on container for {self.id}')
        self._container = await docker.containers.get(self._container._id)
        self.exit_code = self._container['State']['ExitCode']

        log_path = LogStore.container_log_path(log_directory, self.name)
        status_path = LogStore.container_status_path(log_directory, self.name)

        start2 = time.time()
        upload_log = check_shell(f'docker logs {self._container._id} 2>&1 | gsutil -q cp - {shq(log_path)}')  # WHY did this work without permissions?
        upload_status = check_shell(f'docker inspect {self._container._id} | gsutil -q cp - {shq(status_path)}')
        await asyncio.gather(upload_log, upload_status)
        print(f'took {time.time() - start2} seconds to upload to gcs for {self.id}')

        print(f'took {time.time() - start} seconds to run container for {self.id}')

    async def delete(self):
        print(f'deleting container {self.id}')
        if self._container is not None:
            print(f'container is not None -- deleting')
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


class Volume:
    @staticmethod
    @abc.abstractmethod
    def create(*args):
        return

    @abc.abstractmethod
    def path(self):
        return

    @abc.abstractmethod
    def delete(self):
        return


class Secret(Volume):
    @staticmethod
    async def create(name, file_path, secret_data):
        assert secret_data is not None
        os.makedirs(file_path)
        for file_name, data in secret_data.items():
            with open(f'{file_path}/{file_name}', 'w') as f:
                f.write(base64.b64decode(data).decode())
        return Secret(name, file_path)

    def __init__(self, name, file_path):
        print(f'created secret {name} with file path {file_path}')
        self.name = name
        self.file_path = file_path

    @property
    def path(self):
        return self.file_path

    async def delete(self):
        shutil.rmtree(self.path, ignore_errors=True)


class EmptyDir(Volume):
    @staticmethod
    async def create(name, size=None):
        print(f'creating empty dir volume {name}')
        config = {
            'name': name  # FIXME: add size
        }
        volume = await docker.volumes.create(config)
        print(f'created docker volume')
        return EmptyDir(name, volume)

    def __init__(self, name, volume):
        print(f'created empty dir volume {name}')
        self.name = name
        self.volume = volume

    @property
    def path(self):
        return self.name

    async def delete(self):
        await self.volume.delete()


class BatchPod:
    # def _create_secrets(self, secrets_data):
    #     secrets = {}
    #     for name, data in secrets_data.items():
    #         path = f'/batch/pods/{self.name}/{self.token}/secrets/{name}'
    #         secret = Secret.create(name, path, data)
    #         secrets[name] = secret
    #     return secrets
    #
    # def _cleanup_secrets(self):
    #     for _, secret in self.secrets_data.items():
    #         secret.delete()
    #     self.secrets_data = {}

    # async def _volume_from_empty_dir(self, volume_spec):
    #     config = {
    #         'name': volume_spec['name']  # FIXME add size
    #     }
    #     return await docker.volumes.create(config)

    async def _create_volumes(self):
        print(f'creating volumes for pod {self.name}')
        volumes = {}
        for volume_spec in self.spec['spec']['volumes']:
            name = volume_spec['name']
            print(f'name={name}')
            if volume_spec['empty_dir'] is not None:
                print(f'creating empty dir')
                volume = await EmptyDir.create(name)
                volumes[name] = volume
            elif volume_spec['secret'] is not None:
                print(f'creating secret...')
                secret_name = volume_spec['secret']['secret_name']
                path = f'/batch/pods/{self.name}/{self.token}/secrets/{secret_name}'
                secret = await Secret.create(name, path, self.secrets_data.get(secret_name))
                volumes[name] = secret
            else:
                raise Exception(f'Unsupported volume type for {volume_spec}')
        print("done creating volumes")
        return volumes

    def __init__(self, parameters):
        print(json.dumps(parameters['spec'], indent=4))
        self.spec = parameters['spec']
        self.secrets_data = parameters['secrets']
        # self.secrets = self._create_secrets(parameters['secrets'])
        # self.empty_dir_volumes = []
        self.output_directory = parameters['output_directory']

        self.metadata = self.spec['metadata']
        self.name = self.metadata['name']
        self.token = uuid.uuid4().hex
        self.volumes = {}

        self.containers = {cspec['name']: Container(cspec, self) for cspec in self.spec['spec']['containers']}
        self.phase = 'Pending'
        self._run_task = asyncio.ensure_future(self.run())

    async def _create(self):
        print(f'creating pod {self.name}')
        self.volumes = await self._create_volumes()
        print(f'created volumes')
        await asyncio.gather(*[container.create(self.volumes) for container in self.containers.values()])
        print(f'created containers')

    async def _cleanup(self):
        print(f'cleaning up pod {self.name}')
        await asyncio.gather(*[asyncio.shield(c.delete()) for _, c in self.containers.items()])
        # self._cleanup_secrets()
        await asyncio.gather(*[volume.delete() for volume in self.volumes])
        # await self._cleanup_volumes()
        # await self.volume.delete()

    async def run(self, semaphore=None):
        create_task = None
        try:
            create_task = asyncio.ensure_future(self._create())
            await asyncio.shield(create_task)

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
            if create_task is not None:
                await create_task
            raise

    async def delete(self):
        print(f'deleting pod {self.name}')
        self._run_task.cancel()
        try:
            await self._run_task
        finally:
            await asyncio.shield(self._cleanup())

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
    del batch_pods[pod_name]

    asyncio.ensure_future(bp.delete())

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