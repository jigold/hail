import asyncio
import docker
import uvloop
import os
import uuid
import aiohttp
from aiohttp import web

from .utils import abort, jsonify
from .batch_configuration import REFRESH_INTERVAL_IN_SECONDS, POD_VOLUME_SIZE

uvloop.install()

dc = docker.from_env()

app = web.Application(client_max_size=None)
routes = web.RouteTableDef()

session = aiohttp.ClientSession(raise_for_status=True,
                                timeout=aiohttp.ClientTimeout(total=60))

scheduler_url = os.environ.get('BATCH_SCHEDULER_URL', 'batch-scheduler.hail.is')

tasks = ('input', 'main', 'output')


class Container:
    def __init__(self, image, command, cpu, memory, secrets, volume):
        self.image = image
        self.command = command
        self.cpu = cpu
        self.memory = memory
        self.secrets = secrets
        self.volume = volume
        self._container = None

    def run(self):
        self._container = dc.containers.run(
            self.image,
            command=self.command,
            detach=True,
            volume={self.volume: {'bind': '/', 'mode': 'rw'}})

    @property
    def exit_code(self):
        self._container.reload()
        if self._container.status == 'exited':
            return self._container.attrs['ExitCode']

    @property
    def duration(self):
        self._container.reload()
        started_at = self._container.attrs['StartedAt']
        finished_at = self._container.attrs['FinishedAt']
        if started_at is not None and finished_at is not None:
            return finished_at - started_at

    @property
    def status(self):
        self._container.reload()
        return self._container.status


class StackedContainers:
    def __init__(self, image, command, cpu, memory, volume_size=POD_VOLUME_SIZE,
                 input_files=None, output_files=None, callback=None):
        self.id = uuid.uuid4().hex[:8]
        self.exit_codes = [None for _ in tasks]
        self.durations = [None for _ in tasks]
        self.status = [None for _ in tasks]
        self.task_idx = 0
        pass

    def _create_volume(self):
        pass

    def _run_container(self):
        pass

    def mark_complete(self):
        pass

    def cleanup(self):
        pass


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return jsonify({})


@routes.get('/api/v1alpha/containers')
async def list_containers(request):  # pylint: disable=W0613
    return jsonify(dc.containers.list(all=True))


@routes.get('/api/v1alpha/containers/{name}')
async def get_container(request):
    name = request.match_info['name']
    try:
        container = dc.containers.get(name)
    except docker.errors.NotFound:
        abort(404)
    return jsonify(container.attrs)


@routes.delete('/api/v1alpha/containers/{name}/delete')
async def delete_container(request):
    name = request.match_info['name']
    try:
        container = dc.containers.get(name)
    except docker.errors.NotFound:
        abort(404)
    container.stop()
    container.remove()
    return jsonify({})


@routes.post('/api/v1alpha/containers/create')
async def run_container(request):
    parameters = await request.json()
    container = dc.containers.run(parameters['image'], command=parameters['command'], detach=True)
    return jsonify({'name': container.name})


async def container_changed(container):
    if container.status == 'exited':
        await session.post(f'{scheduler_url}/api/v1alpha/container_callback',
                           json=container.attrs)


async def docker_event_loop():
    while True:
        for event in dc.events(decode=True, filter={'type': 'container'}):
            container = dc.containers.get(event['id'])
            await container_changed(container)
        await asyncio.sleep(5)


async def poll_containers():
    await asyncio.sleep(5)
    while True:
        for container in dc.containers.list(all=True):
            await container_changed(container)
        await asyncio.sleep(REFRESH_INTERVAL_IN_SECONDS)


app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)
