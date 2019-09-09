import abc
import os
import time
import logging
import asyncio
import random
import aiohttp
import base64
import uuid
import shutil
from aiohttp import web
import uvloop
import concurrent
import aiodocker
from aiodocker.exceptions import DockerError

# from hailtop import gear
from hailtop.gear import get_deploy_config

from .utils import jsonify, abort, parse_cpu
from .semaphore import NullWeightedSemaphore, WeightedSemaphore
from .log_store import LogStore
from .google_storage import GCS

# gear.configure_logging()
log = logging.getLogger('batch2-agent')

uvloop.install()

docker = aiodocker.Docker()

# app = web.Application()
# routes = web.RouteTableDef()

# batch_pods = {}

MAX_IDLE_TIME_WITH_PODS = 60 * 5  # seconds
MAX_IDLE_TIME_WITHOUT_PODS = 60 * 5  # seconds


class Container:
    def __init__(self, spec, pod):
        self.pod = pod
        self._container = None
        self.name = spec['name']
        self.spec = spec
        self.cores = parse_cpu(spec['resources']['requests']['cpu'])
        self.exit_code = None
        self.id = pod.name + '-' + self.name
        self.image_pull_backoff = None

    async def create(self, volumes):
        print(f'creating container {self.id}')

        config = {
            "AttachStdin": False,
            "AttachStdout": False,
            "AttachStderr": False,
            "Tty": False,
            'OpenStdin': False,
            'Cmd': self.spec['command'],
            'Image': self.spec['image']
        }

        volume_mounts = []
        for mount in self.spec['volume_mounts']:
            mount_name = mount['name']
            mount_path = mount['mount_path']
            if mount_name in volumes:
                volume_path = volumes[mount_name].path
                volume_mounts.append(f'{volume_path}:{mount_path}')
            else:
                raise Exception(f'unknown volume {mount_name} specified in volume_mounts')

        if volume_mounts:
            config['Binds'] = volume_mounts

        try:
            self._container = await docker.containers.create(config, name=self.id)
        except DockerError as err:
            if err.status == 404:
                try:
                    await docker.pull(config['Image'])
                    self._container = await docker.containers.create(config, name=self.id)
                except DockerError as err:
                    if err.status == 404:
                        self.image_pull_backoff = err.message
                        return False
                    else:
                        raise err
            else:
                raise err

        self._container = await docker.containers.get(self._container._id)
        return True

    async def run(self, log_directory):
        assert self.image_pull_backoff is None

        await self._container.start()
        await self._container.wait()
        self._container = await docker.containers.get(self._container._id)
        self.exit_code = self._container['State']['ExitCode']

        log_path = LogStore.container_log_path(log_directory, self.name)
        status_path = LogStore.container_status_path(log_directory, self.name)

        upload_log = self.pod.worker.gcs_client.write_gs_file(log_path, await self.log())
        upload_status = self.pod.worker.gcs_client.write_gs_file(status_path, str(self._container._container))
        await asyncio.gather(upload_log, upload_status)

    async def delete(self):
        if self._container is not None:
            await self._container.stop()
            await self._container.delete()

    @property
    def status(self):
        if self._container is not None:
            return self._container._container

    async def log(self):
        logs = await self._container.log(stderr=True, stdout=True)
        return "".join(logs)

    def to_dict(self):
        if self._container is None:
            if self.image_pull_backoff is not None:
                waiting_reason = {
                    'reason': 'ImagePullBackOff',
                    'message': self.image_pull_backoff
                }
            else:
                waiting_reason = {}

            return {
                'image': self.spec['image'],
                'imageID': 'unknown',
                'name': self.name,
                'ready': False,
                'restartCount': 0,
                'state': {'waiting': waiting_reason}
            }

        state = {}
        if self.status['State']['Status'] == 'created':
            state['waiting'] = {}
        elif self.status['State']['Status'] == 'running':
            state['running'] = {
                'started_at': self.status['State']['StartedAt']
            }
        elif self.status['State']['Status'] == 'exited':  # FIXME: there's other docker states such as dead and oomed
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
        config = {
            'Name': name  # FIXME: add size
        }
        volume = await docker.volumes.create(config)
        return EmptyDir(name, volume)

    def __init__(self, name, volume):
        self.name = name
        self.volume = volume

    @property
    def path(self):
        return self.name

    async def delete(self):
        await self.volume.delete()


class BatchPod:
    async def _create_volumes(self):
        print(f'creating volumes for pod {self.name}')
        volumes = {}
        for volume_spec in self.spec['spec']['volumes']:
            name = volume_spec['name']
            if volume_spec['empty_dir'] is not None:
                volume = await EmptyDir.create(name)
                volumes[name] = volume
            elif volume_spec['secret'] is not None:
                secret_name = volume_spec['secret']['secret_name']
                path = f'/batch/pods/{self.name}/{self.token}/secrets/{secret_name}'
                secret = await Secret.create(name, path, self.secrets_data.get(secret_name))
                volumes[name] = secret
            else:
                raise Exception(f'Unsupported volume type for {volume_spec}')
        return volumes

    def __init__(self, worker, parameters, cpu_sem):
        # print(json.dumps(parameters['spec'], indent=4))
        self.worker = worker
        self.spec = parameters['spec']
        self.secrets_data = parameters['secrets']
        self.output_directory = parameters['output_directory']

        self.metadata = self.spec['metadata']
        self.name = self.metadata['name']
        self.token = uuid.uuid4().hex
        self.volumes = {}

        self.containers = {cspec['name']: Container(cspec, self) for cspec in self.spec['spec']['containers']}
        self.phase = 'Pending'
        self._run_task = asyncio.ensure_future(self.run(cpu_sem))

    async def _create(self):
        print(f'creating pod {self.name}')
        self.volumes = await self._create_volumes()
        created = await asyncio.gather(*[container.create(self.volumes) for container in self.containers.values()])
        return all(created)

    async def _cleanup(self):
        print(f'cleaning up pod {self.name}')
        await asyncio.gather(*[asyncio.shield(c.delete()) for _, c in self.containers.items()])
        await asyncio.gather(*[v.delete() for _, v in self.volumes.items()])

    async def _mark_complete(self):
        body = {
            'inst_token': self.worker.token,
            'status': self.to_dict()
        }

        while True:
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post(self.worker.deploy_config.url('batch2', '/api/v1alpha/instances/pod_complete'), json=body) as resp:
                    if resp.status == 200 or resp.status == 404:
                        self.last_updated = time.time()
                        log.info(f'sent pod complete for {self.name}')
                        return
            await asyncio.sleep(15)

    async def run(self, semaphore=None):
        start = time.time()
        create_task = None
        try:
            create_task = asyncio.ensure_future(self._create())
            created = await asyncio.shield(create_task)
            if not created:
                print(f'unable to create all containers for {self.name}')
                await self._mark_complete()
                return

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

            await self._mark_complete()
            print(f'took {time.time() - start} seconds to run pod {self.name}')

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
        return {
            'metadata': self.metadata,
            'status': {
                'containerStatuses': [c.to_dict() for _, c in self.containers.items()],
                # 'hostIP': None,
                'phase': self.phase
                # 'startTime': None
            }
        }


class Worker:
    def __init__(self, cores, deploy_config, token, ip_address):
        self.cores = cores
        self.deploy_config = deploy_config
        self.token = token
        self.free_cores = cores
        self.last_updated = time.time()
        self.pods = {}
        self.cpu_sem = WeightedSemaphore(cores)
        self.ip_address = ip_address

        pool = concurrent.futures.ThreadPoolExecutor()
        self.gcs_client = GCS(pool)

    async def _create_pod(self, parameters):
        try:
            bp = BatchPod(self, parameters, self.cpu_sem)
            self.pods[bp.name] = bp
        except DockerError as err:
            print(err)
            raise err
            # return web.Response(body=err.message, status=err.status)
        except Exception as err:
            print(err)
            raise err

    async def create_pod(self, request):
        self.last_updated = time.time()
        parameters = await request.json()
        await asyncio.shield(self._create_pod(parameters))
        return jsonify({})

    async def get_container_log(self, request):
        pod_name = request.match_info['pod_name']
        container_name = request.match_info['container_name']

        if pod_name not in self.pods:
            abort(404, 'unknown pod name')
        bp = self.pods[pod_name]

        if container_name not in bp.containers:
            abort(404, 'unknown container name')
        result = await bp.log(container_name)

        return jsonify(result)

    async def get_container_status(self, request):
        pod_name = request.match_info['pod_name']
        container_name = request.match_info['container_name']

        if pod_name not in self.pods:
            abort(404, 'unknown pod name')
        bp = self.pods[pod_name]

        if container_name not in bp.containers:
            abort(404, 'unknown container name')
        result = bp.container_status(container_name)

        return jsonify(result)

    async def get_pod(self, request):
        pod_name = request.match_info['pod_name']
        if pod_name not in self.pods:
            abort(404, 'unknown pod name')
        bp = self.pods[pod_name]
        return jsonify(bp.to_dict())

    async def _delete_pod(self, request):
        pod_name = request.match_info['pod_name']

        if pod_name not in self.pods:
            abort(404, 'unknown pod name')
        bp = self.pods[pod_name]
        del self.pods[pod_name]

        asyncio.ensure_future(bp.delete())

    async def delete_pod(self, request):
        await asyncio.shield(self._delete_pod(request))
        return jsonify({})

    async def list_pods(self, request):
        pods = [bp.to_dict() for _, bp in self.pods.items()]
        return jsonify(pods)

    async def healthcheck(self, request):
        return jsonify({})

    async def run(self):
        app_runner = None
        site = None
        try:
            app = web.Application()
            app.add_routes([
                web.post('/api/v1alpha/pods/create', self.create_pod),
                web.get('/api/v1alpha/pods/{pod_name}/containers/{container_name}/log', self.get_container_log),
                web.get('/api/v1alpha/pods/{pod_name}/containers/{container_name}/status', self.get_container_status),
                web.get('/api/v1alpha/pods/{pod_name}', self.get_pod),
                web.post('/api/v1alpha/pods/{pod_name}/delete', self.delete_pod),
                web.get('/api/v1alpha/pods', self.list_pods),
                web.get('/healthcheck', self.healthcheck)
            ])

            app_runner = web.AppRunner(app)
            await app_runner.setup()
            site = web.TCPSite(app_runner, '0.0.0.0', 5000)
            await site.start()

            await self.register()

            last_ping = time.time() - self.last_updated
            while (self.pods and last_ping < MAX_IDLE_TIME_WITH_PODS) \
                    or last_ping < MAX_IDLE_TIME_WITHOUT_PODS:
                log.info(f'n_pods {len(self.pods)} free_cores {self.free_cores} age {last_ping}')
                await asyncio.sleep(15)
                last_ping = time.time() - self.last_updated

            log.info('idle 60s or no pods, exiting')

            body = {'inst_token': self.token}
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
                async with session.post(self.deploy_config.url('batch2', '/api/v1alpha/instances/deactivate'), json=body):
                    log.info('deactivated')
        finally:
            if site:
                await site.stop()
            if app_runner:
                await app_runner.cleanup()

    async def register(self):
        tries = 0
        while True:
            try:
                log.info('registering')
                body = {'inst_token': self.token,
                        'ip_address': self.ip_address}
                async with aiohttp.ClientSession(
                        raise_for_status=True, timeout=aiohttp.ClientTimeout(total=60)) as session:
                    async with session.post(self.deploy_config.url('batch2', '/api/v1alpha/instances/activate'), json=body) as resp:
                        if resp.status == 200:
                            self.last_updated = time.time()
                            log.info('registered')
                            return
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception as e:  # pylint: disable=broad-except
                log.exception('caught exception while registering')
                if tries == 12:
                    log.info('register: giving up')
                    raise e
                tries += 1
            await asyncio.sleep(5 * random.uniform(1, 1.25))


cores = int(os.environ['CORES'])
namespace = os.environ['NAMESPACE']
inst_token = os.environ['INST_TOKEN']
ip_address = os.environ['INTERNAL_IP']

config = {
    'location': 'gce',
    'default_namespace': namespace,
    'service_namespace': {}
}

deploy_config = get_deploy_config(_config=config)
worker = Worker(cores, deploy_config, inst_token, ip_address)

loop = asyncio.get_event_loop()
loop.run_until_complete(worker.run())
loop.run_until_complete(loop.shutdown_asyncgens())


# @routes.post('/api/v1alpha/pods/create')
# async def create_pod(request):
#     parameters = await request.json()
#     try:
#         bp = BatchPod(parameters)
#         batch_pods[bp.name] = bp
#     except DockerError as err:
#         print(err)
#         return web.Response(body=err.message, status=err.status)
#     except Exception as err:
#         print(err)
#         raise err
#     return jsonify({})
#
#
# @routes.post('/api/v1alpha/pods/{pod_name}/containers/{container_name}/log')
# async def get_container_log(request):
#     pod_name = request.match_info['pod_name']
#     container_name = request.match_info['container_name']
#
#     if pod_name not in batch_pods:
#         abort(404, 'unknown pod name')
#     bp = batch_pods[pod_name]
#
#     if container_name not in bp.containers:
#         abort(404, 'unknown container name')
#     result = await bp.log(container_name)
#
#     return jsonify(result)
#
#
# @routes.post('/api/v1alpha/pods/{pod_name}/containers/{container_name}/status')
# async def get_container_status(request):
#     pod_name = request.match_info['pod_name']
#     container_name = request.match_info['container_name']
#
#     if pod_name not in batch_pods:
#         abort(404, 'unknown pod name')
#     bp = batch_pods[pod_name]
#
#     if container_name not in bp.containers:
#         abort(404, 'unknown container name')
#     result = bp.container_status(container_name)
#
#     return jsonify(result)
#
#
# @routes.post('/api/v1alpha/pods/{pod_name}')
# async def get_pod(request):
#     pod_name = request.match_info['pod_name']
#     if pod_name not in batch_pods:
#         abort(404, 'unknown pod name')
#     bp = batch_pods[pod_name]
#     return jsonify(bp.to_dict())
#
#
# @routes.delete('/api/v1alpha/pods/{pod_name}/delete')
# async def delete_pod(request):
#     pod_name = request.match_info['pod_name']
#
#     if pod_name not in batch_pods:
#         abort(404, 'unknown pod name')
#     bp = batch_pods[pod_name]
#     del batch_pods[pod_name]
#
#     asyncio.ensure_future(bp.delete())
#
#     return jsonify({})
#
#
# @routes.get('/api/v1alpha/pods')
# async def list_pods(request):
#     pods = [bp.to_dict() for _, bp in batch_pods.items()]
#     return jsonify(pods)
#
#
# @routes.get('/healthcheck')
# async def get_healthcheck(request):  # pylint: disable=W0613
#     return web.Response()
#
#
# app.add_routes(routes)
# web.run_app(app, host='0.0.0.0', port=5000)