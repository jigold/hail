import asyncio
import docker
import uvloop
import os
import uuid
import aiohttp
from aiohttp import web

from .utils import abort, jsonify

uvloop.install()

dc = docker.from_env()

app = web.Application()
routes = web.RouteTableDef()


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return jsonify({})


@routes.get('/helloworld')
async def run_helloworld(request):  # pylint: disable=W0613
    

app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)
