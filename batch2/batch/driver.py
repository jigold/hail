import os
import asyncio
import aiohttp
import googleapiclient.discovery
import logging
import google.oauth2.service_account
import requests
import random
import sortedcontainers
from aiohttp import web
import kubernetes as kube

log = logging.getLogger('driver')


# class Driver:
#     def __init__(self, v1, batch_gsa_key=None):
#         self._session = aiohttp.ClientSession(raise_for_status=True,
#                                               timeout=aiohttp.ClientTimeout(total=60))
#         self.event_queue = asyncio.Queue()
#
#         self._cookies = None
#         self._headers = None
#         self.v1 = v1
#         self.instance = 'batch-agent-9'
#         self.url = 'http://10.128.0.122:5000'
#
#         # if batch_gsa_key is None:
#         #     batch_gsa_key = os.environ.get('BATCH_GSA_KEY', '/batch-gsa-key/privateKeyData')
#         # credentials = google.oauth2.service_account.Credentials.from_service_account_file(
#         #     batch_gsa_key)
#         # self.compute_client = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
#         # result = self.compute_client.instances().get(project='hail-vdc', zone='us-central1-a', instance=self.instance).execute()
#
#     async def _get(self, path, params=None):
#         response = await self._session.get(
#             self.url + path, params=params, cookies=self._cookies, headers=self._headers)
#         return await response.json()
#
#     async def _post(self, path, json=None):
#         response = await self._session.post(
#             self.url + path, json=json, cookies=self._cookies, headers=self._headers)
#         return await response.json()
#
#     async def _patch(self, path):
#         await self._session.patch(
#             self.url + path, cookies=self._cookies, headers=self._headers)
#
#     async def _delete(self, path):
#         await self._session.delete(
#             self.url + path, cookies=self._cookies, headers=self._headers)
#
#     async def create_pod(self, spec, secrets, output_directory):
#         try:
#             body = {'spec': spec,
#                     'secrets': secrets,
#                     'output_directory': output_directory}
#             await self._post('/api/v1alpha/pods/create', json=body)
#             return None
#         except Exception as err:
#             return err
#
#     async def delete_pod(self, name):
#         log.info('calling delete pod')
#         try:
#             await self._delete(f'/api/v1alpha/pods/{name}/delete')
#             return None
#         except Exception as err:
#             return err
#
#     async def read_pod_log(self, name, container):
#         try:
#             result = await self._get(f'/api/v1alpha/pods/{name}/containers/{container}/log')
#             return result, None
#         except Exception as err:
#             return None, err
#
#     async def read_container_status(self, name, container):
#         try:
#             result = await self._get(f'/api/v1alpha/pods/{name}/containers/{container}/status')
#             return result, None
#         except Exception as err:
#             return None, err
#
#     async def list_pods(self):
#         try:
#             result = await self._get('/api/v1alpha/pods')
#             return [self.v1.api_client._ApiClient__deserialize(data, kube.client.V1Pod) for data in result], None
#         except Exception as err:
#             log.info(err)
#             return None, err


class Pod:
    def __init__(self, name, spec, secrets, output_directory, instance):
        self.name = name
        # self.cores = cores
        self.spec = spec
        self.secrets = secrets
        self.output_directory = output_directory
        self.instance = instance

    def config(self):
        return {
            'spec': self.spec,
            'secrets': self.secrets,
            'output_directory': self.output_directory
        }

    async def read_pod_log(self, container):
        if self.instance is None:
            return None

    async def read_container_status(self, container):
        if self.instance is None:
            return None

    async def delete(self):
        if self.instance is None:
            return
        # need to make request
        self.instance.unschedule(self)

    async def status(self):
        if self.instance is None:
            return {
                # pending status
            }
        else:
            return

    def __str__(self):
        return self.name


class Driver:
    def __init__(self):
        self.pods = {}
        self.event_queue = asyncio.Queue()
        self.ready_queue = asyncio.Queue()
        self.changed = asyncio.Event()
        self.instance_pool = InstancePool()

        self.app = web.Application()
        self.app.add_routes([
            # web.post('/activate_worker', self.handle_activate_worker),
            # web.post('/deactivate_worker', self.handle_deactivate_worker),
            web.post('/pod_complete', self.pod_complete),
            # web.post('/pool/size', self.handle_pool_size)
        ])

    async def create_pod(self, spec, secrets, output_directory):
        name = spec['metadata']['name']
        # cores = parse_cpu(spec['spec']['resources']['cpu'])
        pod = Pod(name, spec, secrets, output_directory, instance=None)
        self.pods[name] = pod
        await self.ready_queue.put(pod)

    async def delete_pod(self, name):
        pod = self.pods.get(name)
        if pod is None:
            raise Exception(f'pod {name} does not exist')
        await pod.delete()
        del self.pods[name]

    async def read_pod_log(self, name, container):
        pod = self.pods.get(name)
        if pod is None:
            raise Exception(f'pod {name} does not exist')
        return await pod.read_pod_log(container)

    async def read_container_status(self, name, container):
        pod = self.pods.get(name)
        if pod is None:
            raise Exception(f'pod {name} does not exist')
        return await pod.read_container_status(container)

    async def list_pods(self):
        # FIXME: this is inefficient!
        return await asyncio.gather(*[pod.status() for _, pod in self.pods.items()])

    async def pod_complete(self, request):
        data = await request.json()
        await self.event_queue.put(data)

    async def schedule(self):
        while True:
            pod = await self.ready_queue.get()
            instance = random.sample(self.instance_pool.instances)
            await instance.schedule(pod)
            await pod.create()

    async def run(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 5001)
        await site.start()

        asyncio.ensure_future(self.instance_pool.run())
        asyncio.ensure_future(self.schedule())


class InstancePool:
    def __init__(self, pool_size=2):
        self.instances = sortedcontainers.SortedSet()
        self.pool_size = pool_size

    async def create_instance(self):
        pass

    async def run(self):
        while True:
            while len(self.instances) < self.pool_size:
                instance = await self.create_instance()
                self.instances.add(instance)
            await asyncio.sleep(15)


class Instance:
    def __init__(self, name, hostname, cores):
        self.name = name
        self.hostname = hostname
        # self.cores = cores
        self.pods = sortedcontainers.SortedSet()

    def schedule(self, pod):
        self.pods.add(pod)
        pod.instance = self
        # self.cores -= pod.cores

    def unschedule(self, pod):
        assert pod in self.pods
        self.pods.remove(pod)
