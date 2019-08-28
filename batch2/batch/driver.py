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

from .batch_configuration import PROJECT, ZONE, INSTANCE_ID, BATCH_NAMESPACE, BATCH_IMAGE
from .utils import new_token
from .google_compute import GServices

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
        self.cores = 1  # FIXME
        self.spec = spec
        self.secrets = secrets
        self.output_directory = output_directory
        self.instance = instance
        self.running = False

    def config(self):
        return {
            'spec': self.spec,
            'secrets': self.secrets,
            'output_directory': self.output_directory
        }

    async def read_pod_log(self, container):
        if self.instance is None:
            return None
        return await self.instance.read_pod_log(self, container)

    async def read_container_status(self, container):
        if self.instance is None:
            return None
        return await self.instance.read_container_status(self, container)

    async def delete(self):
        if self.instance is None:
            return
        # need to make request
        await self.instance.unschedule(self)

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
    def __init__(self, batch_gsa_key=None):
        self.pods = {}
        self.event_queue = asyncio.Queue()
        self.ready_queue = asyncio.Queue()
        self.ready = sortedcontainers.SortedSet(key=lambda pod: pod.cores)
        self.changed = asyncio.Event()

        self.base_url = f'http://hail.internal/{BATCH_NAMESPACE}/batch2'

        self.instance_pool = InstancePool(self)

        if batch_gsa_key is None:
            batch_gsa_key = os.environ.get('BATCH_GSA_KEY', '/batch-gsa-key/privateKeyData')
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(batch_gsa_key)
        self.gservices = GServices(self.instance_pool.machine_name_prefix, credentials)
        self.service_account = credentials.service_account_email

        # self.app = web.Application()
        # self.app.add_routes([
        #     web.post('/activate_worker', self.activate_worker),
        #     # web.post('/deactivate_worker', self.handle_deactivate_worker),
        #     web.post('/pod_complete', self.pod_complete),
        #     # web.post('/pool/size', self.handle_pool_size)
        # ])

    # async def activate_worker(self, request):
    #     return await asyncio.shield(self._activate_worker(request))

    async def activate_worker(self, request):
        body = await request.json()
        inst_token = body['inst_token']
        ip_address = body['ip_address']

        inst = self.instance_pool.token_inst.get(inst_token)
        if not inst:
            log.warning(f'/activate_worker from unknown inst {inst_token}')
            raise web.HTTPNotFound()

        log.info(f'activating {inst}')
        inst.activate(ip_address)
        return web.Response()

    async def pod_complete(self, request):
        body = await request.json()
        inst_token = body['inst_token']
        data = body['data']

        inst = self.instance_pool.token_inst.get(inst_token)
        if not inst:
            log.warning(f'/pod_complete from unknown inst {inst_token}')
            raise web.HTTPNotFound()

        log.info(f'adding pod complete to event queue')
        await self.event_queue.put(data)
        return web.Response()

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
        try:
            result = await asyncio.gather(*[pod.status() for _, pod in self.pods.items()])
            return result, None
        except Exception as err:
            return None, err

    # async def schedule(self):
    #     while True:
    #         pod = await self.ready_queue.get()
    #         instance = random.sample(self.instance_pool.instances, 1)
    #         if instance:
    #             await instance.schedule(pod)

    async def schedule(self):
        log.info('scheduler started')

        self.changed.clear()
        should_wait = False
        while True:
            if should_wait:
                await self.changed.wait()
                self.changed.clear()

            # if not self.ready:
            #     pod = await self.ready_queue.get()
            #     if not pod:
            #         return
            #     self.ready.add(pod)
            while len(self.ready) < 1 and not self.ready_queue.empty():  # FIXME: replace with 50
                pod = self.ready_queue.get_nowait()
                self.ready.add(pod)
                log.info(f'added {pod} to ready')

            should_wait = True
            if self.instance_pool.instances_by_free_cores and self.ready:
                inst = self.instance_pool.instances_by_free_cores[-1]
                log.info(f'selected instance {inst}')
                i = self.ready.bisect_key_right(inst.free_cores)
                if i > 0:
                    pod = self.ready[i - 1]
                    assert pod.cores <= inst.free_cores
                    self.ready.remove(pod)
                    should_wait = False
                    # if not pod.state:
                    #     assert not pod.active_inst

                    log.info(f'scheduling {pod} cores {pod.cores} on {inst}')
                    await inst.schedule(pod)
                    # await self.pool.call(self.execute_task, pod, inst)

    async def run(self):
        asyncio.ensure_future(self.instance_pool.run())
        await self.schedule()

        # app_runner = None
        # site = None
        # try:
        #     app_runner = web.AppRunner(self.app)
        #     await app_runner.setup()
        #     site = web.TCPSite(app_runner, '0.0.0.0', 5001)
        #     await site.start()
        #
        #     asyncio.ensure_future(self.instance_pool.run())
        #
        #     # self.thread_pool = AsyncWorkerPool(100)
        #
        #     await self.schedule()
        # finally:
        #     if site:
        #         await site.stop()
        #     if app_runner:
        #         await app_runner.cleanup()


class InstancePool:
    def __init__(self, driver, pool_size=1, worker_type='standard', worker_cores=1, worker_disk_size_gb=10):
        self.driver = driver
        self.worker_type = worker_type
        self.worker_cores = worker_cores
        self.worker_disk_size_gb = worker_disk_size_gb

        if worker_type == 'standard':
            m = 3.75
        elif worker_type == 'highmem':
            m = 6.5
        else:
            assert worker_type == 'highcpu', worker_type
            m = 0.9
        self.worker_mem_per_core_in_gb = 0.9 * m

        self.machine_name_prefix = f'batch2-agent-{BATCH_NAMESPACE}-{INSTANCE_ID}-'

        self.instances = sortedcontainers.SortedSet()
        self.pool_size = pool_size
        self.token_inst = {}

        self.n_pending_instances = 0
        self.n_active_instances = 0

        # for active instances only
        self.instances_by_free_cores = sortedcontainers.SortedSet(key=lambda inst: inst.free_cores)

    def token_machine_name(self, inst_token):
        return f'{self.machine_name_prefix}{inst_token}'

    async def create_instance(self):
        while True:
            inst_token = new_token()
            if inst_token not in self.token_inst:
                break
        # reserve
        self.token_inst[inst_token] = None

        log.info(f'creating instance {inst_token}')

        machine_name = self.token_machine_name(inst_token)
        config = {
            'name': machine_name,
            'machineType': f'projects/{PROJECT}/zones/{ZONE}/machineTypes/n1-{self.worker_type}-{self.worker_cores}',
            'labels': {
                'role': 'batch2-agent',
                'inst_token': inst_token,
                'batch_instance': INSTANCE_ID,
                'namespace': BATCH_NAMESPACE
            },

            'disks': [{
                'boot': True,
                'autoDelete': True,
                'diskSizeGb': self.worker_disk_size_gb,
                'initializeParams': {
                    'sourceImage': 'projects/hail-vdc/global/images/batch-agent-2',
                }
            }],

            'networkInterfaces': [{
                'network': 'global/networks/default',
                'networkTier': 'PREMIUM',
                'accessConfigs': [{
                    'type': 'ONE_TO_ONE_NAT',
                    'name': 'external-nat'
                }]
            }],

            'scheduling': {
                'automaticRestart': False,
                'onHostMaintenance': "TERMINATE",
                'preemptible': True
            },

            'serviceAccounts': [{
                'email': 'batch2-agent@hail-vdc.iam.gserviceaccount.com', # self.driver.service_account,
                'scopes': [
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            }],

            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    'key': 'startup-script',
                    'value': f'''
#!/bin/bash
set -ex

export BATCH_IMAGE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_image")
export HOME=/root

docker run -v /var/run/docker.sock:/var/run/docker.sock -v /usr/bin/docker:/usr/bin/docker -p 5000:5000 -d --entrypoint "/bin/bash" $BATCH_IMAGE -c "sh /run-worker.sh"
'''
                }, {
                    'key': 'inst_token',
                    'value': inst_token
                }, {
                    'key': 'driver_base_url',
                    'value': self.driver.base_url
                }, {
                    'key': 'batch_image',
                    'value': BATCH_IMAGE
                }, {
                    'key': 'batch_instance',
                    'value': INSTANCE_ID
                }, {
                    'key': 'namespace',
                    'value': BATCH_NAMESPACE
                }]
            },
            'tags': {
                'items': [
                    "batch2-agent"
                ]
            },
        }

        await self.driver.gservices.create_instance(config)
        log.info(f'created machine {machine_name}')

        inst = Instance(self, machine_name, self.worker_cores)
        self.token_inst[inst_token] = inst
        self.instances.add(inst)

        log.info(f'created instance {inst}')

        return inst

    async def run(self):
        while True:
            while len(self.instances) < self.pool_size:
                await self.create_instance()
            await asyncio.sleep(15)


class Instance:
    def __init__(self, instance_pool, machine_name, cores):
        self.instance_pool = instance_pool
        self.machine_name = machine_name
        self.ip_address = None
        # self.name = name
        self.cores = cores
        self.pods = set()  # sortedcontainers.SortedSet()
        self.active = False
        self.deleted = False
        self.pending = True
        self.free_cores = cores

    def activate(self, ip_address):
        if self.active:
            return
        if self.deleted:
            return

        if self.pending:
            self.pending = False
            self.instance_pool.n_pending_instances -= 1
            # self.instance_pool.free_cores -= self.instance_pool.worker_capacity

        self.active = True
        self.ip_address = ip_address
        self.instance_pool.n_active_instances += 1
        self.instance_pool.instances_by_free_cores.add(self)
        # self.instance_pool.free_cores += self.instance_pool.worker_capacity
        self.instance_pool.driver.changed.set()
        log.info(f'activated instance {self.machine_name} with hostname {self.ip_address}')

    async def schedule(self, pod):
        if pod.instance is not None:
            log.info(f'pod {pod} already scheduled, ignoring')
            return

        self.pods.add(pod)
        pod.instance = self
        log.info(f'scheduling pod {pod} to instance {self.machine_name}')
        # self.cores -= pod.cores
        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            await session.post(f'http://{self.ip_address}:5000/api/v1alpha/pods/create', json=pod.config())
        # inst.update_timestamp()

    async def unschedule(self, pod):
        if pod not in self.pods:
            log.info(f'cannot unschedule unknown pod {pod}, ignoring')
            return

        self.pods.remove(pod)
        # self.cores += pod.cores
        log.info(f'unscheduling pod {pod} from instance {self.machine_name}')

        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            await session.post(f'http://{self.ip_address}:5000/api/v1alpha/pods/{pod.name}/delete', json=pod.config())

    async def read_container_status(self, pod, container):
        if pod not in self.pods:
            log.info(f'unknown pod {pod}, ignoring')
            return

        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with await session.get(f'http://{self.ip_address}:5000/api/v1alpha/pods/{pod.name}/containers/{container}/status', json=pod.config()) as resp:
                log.info(resp)
                return None

    async def read_pod_log(self, pod, container):
        if pod not in self.pods:
            log.info(f'unknown pod {pod}, ignoring')
            return

        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with await session.get(f'http://{self.ip_address}:5000/api/v1alpha/pods/{pod.name}/containers/{container}/log', json=pod.config()) as resp:
                log.info(resp)
                return None

    def __str__(self):
        return self.machine_name
