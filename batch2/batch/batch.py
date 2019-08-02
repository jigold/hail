import os
import asyncio
from aiohttp import web
import aiodocker
import googleapiclient

from .utils import abort, jsonify

#compute = googleapiclient.discovery.build('compute', 'v1')
#compute.instances().list(project=project, zone=zone)

AGENT_NAMESPACE = os.environ.get('AGENT_NAMESPACE', 'batch2-agent')
MIN_AGENTS = os.environ.get('MIN_AGENTS', 1)

app = web.Application(client_max_size=None)
routes = web.RouteTableDef()


@routes.get('/healthcheck')
async def get_healthcheck(request):  # pylint: disable=W0613
    return jsonify({})


async def get_nodes():
    

app.add_routes(routes)
web.run_app(app, host='0.0.0.0', port=5000)