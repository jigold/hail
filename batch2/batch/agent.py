import asyncio
import docker
import uvloop
import os
import uuid
import aiohttp
from aiohttp import web

from .utils import abort, jsonify
from .batch_configuration import REFRESH_INTERVAL_IN_SECONDS

uvloop.install()

dc = docker.from_env()

app = web.Application()
routes = web.RouteTableDef()


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return jsonify({})


@routes.get('/helloworld')
async def run_helloworld(request):  # pylint: disable=W0613
    container = dc.containers.run('helloworld', detach=True)
    return jsonify({'name': container.name})


@routes.get('/api/v1alpha/containers')
async def list_containers(request):  # pylint: disable=W0613
    return jsonify(dc.containers.list(all=True))
#
#
# @routes.get('/api/v1alpha/containers/{name}')
# async def get_container(request):
#     name = request.match_info['name']
#     try:
#         container = dc.containers.get(name)
#     except docker.errors.NotFound:
#         abort(404)
#     return jsonify(container.attrs)
#
#
# @routes.delete('/api/v1alpha/containers/{name}/delete')
# async def delete_container(request):
#     name = request.match_info['name']
#     try:
#         container = dc.containers.get(name)
#         container.stop()
#         container.remove()
#     except docker.errors.NotFound:
#         abort(404)
#     return jsonify({})
#
#
# @routes.post('/api/v1alpha/containers/create')
# async def run_container(request):
#     parameters = await request.json()
#     container = dc.containers.run(parameters['image'], command=parameters['command'], detach=True)
#     return jsonify({'name': container.name})

#
# async def container_changed(container):
#     if container.status == 'exited':
#         await session.post(f'{scheduler_url}/api/v1alpha/container_callback',
#                            json=container.attrs)


# async def docker_event_loop():
#     while True:
#         for event in dc.events(decode=True, filter={'type': 'container'}):
#             container = dc.containers.get(event['id'])
#             await container_changed(container)
#         await asyncio.sleep(5)
#
#
# async def poll_containers():
#     await asyncio.sleep(5)
#     while True:
#         for container in dc.containers.list(all=True):
#             await container_changed(container)
#         await asyncio.sleep(REFRESH_INTERVAL_IN_SECONDS)


app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)
