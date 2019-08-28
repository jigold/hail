import os
import asyncio
import aiohttp
import googleapiclient.discovery
import logging
import time
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
        self.active_inst = instance
        self.running = False
        self.on_ready = False

    def config(self):
        return {
            'spec': self.spec,
            'secrets': self.secrets,
            'output_directory': self.output_directory
        }

    def unschedule(self):
        if not self.active_inst:
            return

        inst = self.active_inst
        inst_pool = inst.inst_pool

        inst.tasks.remove(self)

        assert not inst.pending and inst.active
        inst_pool.instances_by_free_cores.remove(inst)
        inst.free_cores += self.cores
        inst_pool.free_cores += self.cores
        inst_pool.instances_by_free_cores.add(inst)
        inst_pool.runner.changed.set()

        self.active_inst = None

    def schedule(self, inst, runner):
        assert inst.active
        assert not self.active_inst

        # all mine
        self.unschedule()
        self.remove_from_ready(runner)

        inst.tasks.add(self)

        # inst.active
        inst.inst_pool.instances_by_free_cores.remove(inst)
        inst.free_cores -= self.cores
        inst.inst_pool.instances_by_free_cores.add(inst)
        inst.inst_pool.free_cores -= self.cores
        # can't create more scheduling opportunities, don't set changed

        self.active_inst = inst

    def remove_from_ready(self, runner):
        if not self.on_ready:
            return
        self.on_ready = False
        runner.ready_cores -= self.cores

    async def put_on_ready(self, runner):
        if self.on_ready:
            return
        self.unschedule()
        self.on_ready = True
        runner.ready_cores += self.cores
        await runner.ready_queue.put(self)
        runner.changed.set()
        log.info(f'put {self} on ready')

    async def read_pod_log(self, container):
        if self.active_inst is None:
            return None
        return await self.active_inst.read_pod_log(self, container)

    async def read_container_status(self, container):
        if self.active_inst is None:
            return None
        return await self.active_inst.read_container_status(self, container)

    async def delete(self):
        if self.active_inst is None:
            return
        # need to make request
        await self.active_inst.unschedule(self)

    async def status(self):
        if self.active_inst is None:
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
        self.complete_queue = asyncio.Queue()
        self.ready_queue = asyncio.Queue()
        self.ready = sortedcontainers.SortedSet(key=lambda pod: pod.cores)
        self.ready_cores = 0
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
        await self.complete_queue.put(data)
        return web.Response()

    async def create_pod(self, spec, secrets, output_directory):
        name = spec['metadata']['name']
        # cores = parse_cpu(spec['spec']['resources']['cpu'])
        pod = Pod(name, spec, secrets, output_directory, instance=None)
        self.pods[name] = pod
        await pod.put_on_ready(self)
        # await self.ready_queue.put(pod)
        # self.changed.set()

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

    async def schedule(self):
        log.info('scheduler started')

        self.changed.clear()
        should_wait = False
        while True:
            if should_wait:
                await self.changed.wait()
                self.changed.clear()

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

    async def handle_event(self, event):
        if not event.payload:
            log.warning(f'event has no payload')
            return

        payload = event.payload
        version = payload['version']
        if version != '1.2':
            log.warning('unknown event verison {version}')
            return

        resource_type = event.resource.type
        if resource_type != 'gce_instance':
            log.warning(f'unknown event resource type {resource_type}')
            return

        event_type = payload['event_type']
        event_subtype = payload['event_subtype']
        resource = payload['resource']
        name = resource['name']

        log.info(f'event {version} {resource_type} {event_type} {event_subtype} {name}')

        if not name.startswith(self.machine_name_prefix):
            log.warning(f'event for unknown machine {name}')
            return

        inst_token = name[len(self.machine_name_prefix):]
        inst = self.token_inst.get(inst_token)
        if not inst:
            log.warning(f'event for unknown instance {inst_token}')
            return

        if event_subtype == 'compute.instances.preempted':
            log.info(f'event handler: handle preempt {inst}')
            await inst.handle_preempt_event()
        elif event_subtype == 'compute.instances.delete':
            if event_type == 'GCE_OPERATION_DONE':
                log.info(f'event handler: remove {inst}')
                await inst.remove()
            elif event_type == 'GCE_API_CALL':
                log.info(f'event handler: handle call delete {inst}')
                await inst.handle_call_delete_event()
            else:
                log.warning(f'unknown event type {event_type}')
        else:
            log.warning(f'unknown event subtype {event_subtype}')

    async def event_loop(self):
        while True:
            try:
                async for event in await self.driver.gservices.stream_entries():
                    await self.handle_event(event)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception('event loop failed due to exception')
            await asyncio.sleep(15)

    # async def heal_loop(self):
    #     while True:
    #         try:
    #             if self.instances:
    #                 # 0 is the smallest (oldest)
    #                 inst = self.instances[0]
    #                 inst_age = time.time() - inst.last_updated
    #                 if inst_age > 60:
    #                     log.info(f'heal: oldest {inst} age {inst_age}s')
    #                     await inst.heal()
    #         except asyncio.CancelledError:  # pylint: disable=try-except-raise
    #             raise
    #         except Exception:  # pylint: disable=broad-except
    #             log.exception('instance pool heal loop: caught exception')
    #
    #         await asyncio.sleep(1)

    # async def control_loop(self):
    #     while True:
    #         try:
    #             log.info(f'n_pending_instances {self.n_pending_instances}'
    #                      f' n_active_instances {self.n_active_instances}'
    #                      f' pool_size {self.pool_size}'
    #                      f' n_instances {len(self.instances)}'
    #                      f' max_instances {self.max_instances}'
    #                      f' free_cores {self.free_cores}'
    #                      f' ready_cores {self.runner.ready_cores}')
    #
    #             if self.runner.ready_cores > 0:
    #                 instances_needed = (self.runner.ready_cores - self.free_cores + self.worker_capacity - 1) // self.worker_capacity
    #                 instances_needed = min(instances_needed,
    #                                        self.pool_size - (self.n_pending_instances + self.n_active_instances),
    #                                        self.max_instances - len(self.instances),
    #                                        # 20 queries/s; our GCE long-run quota
    #                                        300)
    #                 if instances_needed > 0:
    #                     log.info(f'creating {instances_needed} new instances')
    #                     # parallelism will be bounded by thread pool
    #                     await asyncio.gather(*[self.create_instance() for _ in range(instances_needed)])
    #         except asyncio.CancelledError:  # pylint: disable=try-except-raise
    #             raise
    #         except Exception:  # pylint: disable=broad-except
    #             log.exception('instance pool control loop: caught exception')
    #
    #         await asyncio.sleep(15)

    async def run(self):
        while True:
            while len(self.instances) < self.pool_size:
                await self.create_instance()
            await asyncio.sleep(15)


class Instance:
    def __init__(self, instance_pool, machine_name, cores):
        self.inst_pool = instance_pool
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
            self.inst_pool.n_pending_instances -= 1
            self.inst_pool.free_cores -= self.inst_pool.worker_capacity

        self.active = True
        self.ip_address = ip_address
        self.inst_pool.n_active_instances += 1
        self.inst_pool.instances_by_free_cores.add(self)
        self.inst_pool.free_cores += self.inst_pool.worker_capacity
        self.inst_pool.driver.changed.set()
        log.info(f'activated instance {self.machine_name} with hostname {self.ip_address}')

    async def deactivate(self):
        if self.pending:
            self.pending = False
            self.inst_pool.n_pending_instances -= 1
            # self.inst_pool.free_cores -= self.inst_pool.worker_capacity
            assert not self.active

            log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')
            return

        if not self.active:
            return

        pod_list = list(self.pods)
        for p in pod_list:
            p.unschedule()

        self.active = False
        self.inst_pool.instances_by_free_cores.remove(self)
        self.inst_pool.n_active_instances -= 1
        self.inst_pool.free_cores -= self.inst_pool.worker_capacity

        for p in pod_list:
            await self.inst_pool.driver.ready_queue.put()
            # await p.put_on_ready(self.inst_pool.runner)
        assert not self.pods

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    # async def schedule(self, pod):
    #     if pod.instance is not None:
    #         log.info(f'pod {pod} already scheduled, ignoring')
    #         return
    #
    #     self.pods.add(pod)
    #     pod.instance = self
    #     log.info(f'scheduling pod {pod} to instance {self.machine_name}')
    #     # self.cores -= pod.cores
    #     async with aiohttp.ClientSession(
    #             raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
    #         await session.post(f'http://{self.ip_address}:5000/api/v1alpha/pods/create', json=pod.config())
    #     # inst.update_timestamp()
    #
    # async def unschedule(self, pod):
    #     if pod not in self.pods:
    #         log.info(f'cannot unschedule unknown pod {pod}, ignoring')
    #         return
    #
    #     self.pods.remove(pod)
    #     # self.cores += pod.cores
    #     log.info(f'unscheduling pod {pod} from instance {self.machine_name}')
    #
    #     async with aiohttp.ClientSession(
    #             raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
    #         await session.post(f'http://{self.ip_address}:5000/api/v1alpha/pods/{pod.name}/delete', json=pod.config())
    #
    #     self.inst_pool.driver.changed.set()
    #
    # async def read_container_status(self, pod, container):
    #     if pod not in self.pods:
    #         log.info(f'unknown pod {pod}, ignoring')
    #         return
    #
    #     async with aiohttp.ClientSession(
    #             raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
    #         async with session.get(f'http://{self.ip_address}:5000/api/v1alpha/pods/{pod.name}/containers/{container}/status', json=pod.config()) as resp:
    #             log.info(resp)
    #             return None
    #
    # async def read_pod_log(self, pod, container):
    #     if pod not in self.pods:
    #         log.info(f'unknown pod {pod}, ignoring')
    #         return
    #
    #     async with aiohttp.ClientSession(
    #             raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
    #         async with session.get(f'http://{self.ip_address}:5000/api/v1alpha/pods/{pod.name}/containers/{container}/log', json=pod.config()) as resp:
    #             log.info(resp)
    #             return None

    def update_timestamp(self):
        if self in self.inst_pool.instances:
            self.inst_pool.instances.remove(self)
            self.last_updated = time.time()
            self.inst_pool.instances.add(self)

    async def remove(self):
        await self.deactivate()
        self.inst_pool.instances.remove(self)
        if self.token in self.inst_pool.token_inst:
            del self.inst_pool.token_inst[self.token]

    async def handle_call_delete_event(self):
        await self.deactivate()
        self.deleted = True
        self.update_timestamp()

    async def delete(self):
        if self.deleted:
            return
        await self.deactivate()
        await self.inst_pool.runner.gservices.delete_instance(self.machine_name())
        log.info(f'deleted machine {self.machine_name()}')
        self.deleted = True

    async def handle_preempt_event(self):
        await self.delete()
        self.update_timestamp()

    async def heal(self):
        try:
            spec = await self.inst_pool.runner.gservices.get_instance(self.machine_name())
        except googleapiclient.errors.HttpError as e:
            if e.resp['status'] == '404':
                await self.remove()
                return

        status = spec['status']
        log.info(f'heal: machine {self.machine_name()} status {status}')

        # preempted goes into terminated state
        if status == 'TERMINATED' and self.deleted:
            await self.remove()
            return

        if status in ('TERMINATED', 'STOPPING'):
            await self.deactivate()

        if status == 'TERMINATED' and not self.deleted:
            await self.delete()

        self.update_timestamp()


    def __str__(self):
        return self.machine_name
