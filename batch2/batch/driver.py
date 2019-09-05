import os
import json
import asyncio
import aiohttp
import logging
import google.oauth2.service_account
import sortedcontainers
import traceback
from aiohttp import web

from .batch_configuration import BATCH_NAMESPACE
from .google_compute import GServices
from .instance_pool import InstancePool
from .utils import AsyncWorkerPool, parse_cpu
from .globals import db

log = logging.getLogger('driver')


class PodWriteFailure(Exception):
    pass


class Pod:
    @staticmethod
    def from_record(record):
        spec = json.loads(record['spec'])
        status = json.loads(record['status'])
        return Pod(name=record['name'], spec=spec,
                   output_directory=record['output_directory'], cores=record['cores'],
                   instance=record['instance'], on_ready=record['on_ready'],
                   status=status)

    @staticmethod
    async def from_db(name):
        records = await db.pods.get_record(name)
        if len(records) == 1:
            return Pod.from_record(records[0])
        return None

    @staticmethod
    async def create_pod(name, spec, output_directory):
        container_cpu_requests = [container['resources']['requests']['cpu'] for container in spec['spec']['containers']]
        container_cores = [parse_cpu(cpu) for cpu in container_cpu_requests]
        if any([cores is None for cores in container_cores]):
            raise Exception(f'invalid value(s) for cpu: '
                            f'{[cpu for cpu, cores in zip(container_cpu_requests, container_cores) if cores is None]}')
        cores = max(container_cores)

        spec = json.dumps(spec)
        await db.pods.new_record(name=name, spec=spec, output_directory=output_directory,
                                 cores=cores, instance=None, on_ready=False)

        return Pod(name, spec, output_directory, cores, instance=None, on_ready=False, status=None)

    def __init__(self, name, spec, output_directory, cores, instance, on_ready, status):
        self.name = name
        self.spec = spec
        self.output_directory = output_directory
        self.cores = cores
        self.instance = instance
        self.on_ready = on_ready
        self._status = status

    async def config(self, driver):
        future_secrets = []
        secret_names = []
        for volume in self.spec['spec']['volumes']:
            if volume['secret'] is not None:
                name = volume['secret']['secret_name']
                secret_names.append(name)
                future_secrets.append(driver.k8s.read_secret(name))
        results = await asyncio.gather(*future_secrets)

        secrets = {}
        for name, (secret, err) in zip(secret_names, results):
            if err is not None:
                traceback.print_tb(err.__traceback__)
                log.info(f'could not get secret {name} due to {err}')
                raise err
            secrets[name] = secret

        return {
            'spec': self.spec,
            'secrets': secrets,
            'output_directory': self.output_directory
        }

    async def unschedule(self):
        if not self.instance:
            return

        inst = self.instance
        inst_pool = inst.inst_pool

        inst.pods.remove(self)

        assert inst.active  # not inst.pending and
        inst_pool.instances_by_free_cores.remove(inst)
        inst.free_cores += self.cores
        inst_pool.free_cores += self.cores
        inst_pool.instances_by_free_cores.add(inst)
        inst_pool.driver.changed.set()

        self.instance = None

        n_updated = await db.pods.update_record(self.name,
                                                compare_items={'instance': inst.name},
                                                instance=None)
        if n_updated != 1:
            log.info(f'changing the instance from {inst.name} -> None failed due to mismatch in db')
            raise PodWriteFailure

    async def schedule(self, inst, driver):
        assert inst.active
        assert not self.instance

        # all mine
        # await self.unschedule()  # I don't think this is ever necessary
        await self.remove_from_ready(driver)

        inst.pods.add(self)

        inst.inst_pool.instances_by_free_cores.remove(inst)
        inst.free_cores -= self.cores
        inst.inst_pool.free_cores -= self.cores
        inst.inst_pool.instances_by_free_cores.add(inst)
        # can't create more scheduling opportunities, don't set changed

        self.instance = inst

        n_updated = await db.pods.update_record(self.name,
                                                compare_items={'instance': None},
                                                instance=inst)
        if n_updated != 1:
            log.info(f'changing the instance from None -> {inst.name} failed due to mismatch in db')
            await self.unschedule()  # not sure this will work
            raise PodWriteFailure

    async def remove_from_ready(self, driver):
        if not self.on_ready:
            return

        self.on_ready = False
        driver.ready_cores -= self.cores

        # n_updated = await db.pods.update_record(self.name,
        #                                         compare_items={'on_ready': True},
        #                                         on_ready=False)
        # if n_updated != 1:
        #     log.info(f'setting on_ready={self.on_ready} failed due to mismatch in db')
        #     raise PodWriteFailure

        log.info(f'removed {self} from on ready')

    async def put_on_ready(self, driver):
        if self.on_ready:
            return

        await self.unschedule()
        self.on_ready = True
        driver.ready_cores += self.cores
        await driver.ready_queue.put(self)
        driver.changed.set()

        # n_updated = await db.pods.update_record(self.name,
        #                                         compare_items={'on_ready': False},
        #                                         on_ready=True)
        # if n_updated != 1:
        #     log.info(f'setting on_ready={self.on_ready} failed due to mismatch in db')
        #     raise PodWriteFailure

        log.info(f'put {self} on ready')

    async def create(self, inst, driver):
        await self.schedule(inst, driver)

        try:
            config = await self.config(driver)  # FIXME: handle missing secrets!

            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
                await session.post(f'http://{inst.ip_address}:5000/api/v1alpha/pods/create', json=config)

            log.info(f'created {self} on {inst}')
        except asyncio.CancelledError:  # pylint: disable=try-except-raise
            raise
        except Exception:  # pylint: disable=broad-except
            log.exception(f'failed to execute {self} on {inst}, rescheduling"')
            await self.put_on_ready(driver)

    async def delete(self):
        if self.instance:
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
                async with session.post(f'http://{self.instance.ip_address}:5000/api/v1alpha/pods/{self.name}/delete') as resp:
                    if resp.status == 200:
                        log.info(f'successfully deleted {self.name}')
                    else:
                        log.info(f'failed to delete due to {resp}')
                        return

        await self.unschedule()

        await db.pods.delete_record(self.name)

    async def read_pod_log(self, container):
        if self.instance is None:
            return None

        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get(f'http://{self.instance.ip_address}:5000/api/v1alpha/pods/{self.name}/containers/{container}/log') as resp:
                return resp.json()

    async def read_container_status(self, container):
        if self.instance is None:
            return None

        async with aiohttp.ClientSession(
                raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
            async with session.get(f'http://{self.instance.ip_address}:5000/api/v1alpha/pods/{self.name}/containers/{container}/status') as resp:
                return resp.json()

    async def status(self):
        if self._status is None:
            return {
                'metadata': self.spec['metadata'],
                'status': {
                    'containerStatuses': None,
                    'phase': 'Pending'
                }
            }
        else:
            return self._status

    def __str__(self):
        return self.name


class Driver:
    def __init__(self, k8s, batch_gsa_key=None, worker_type='standard', worker_cores=1,
                 worker_disk_size_gb=10, pool_size=1, max_instances=2):
        self.k8s = k8s
        self.pods = {}
        self.complete_queue = asyncio.Queue()
        self.ready_queue = asyncio.Queue(maxsize=1000)
        self.ready = sortedcontainers.SortedSet(key=lambda pod: pod.cores)
        self.ready_cores = 0
        self.changed = asyncio.Event()

        self.pool = None  # created in run

        self.base_url = f'http://hail.internal/{BATCH_NAMESPACE}/batch2'

        if worker_type == 'standard':
            m = 3.75
        elif worker_type == 'highmem':
            m = 6.5
        else:
            assert worker_type == 'highcpu', worker_type
            m = 0.9
        self.worker_mem_per_core_in_gb = 0.9 * m

        self.inst_pool = InstancePool(self, worker_type, worker_cores,
                                      worker_disk_size_gb, pool_size, max_instances)

        if batch_gsa_key is None:
            batch_gsa_key = os.environ.get('BATCH_GSA_KEY', '/batch-gsa-key/privateKeyData')
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(batch_gsa_key)

        self.gservices = GServices(self.inst_pool.machine_name_prefix, credentials)

    async def get_secret(self, name):
        secret = self.k8s.read_secret(name)
        return secret.data

    async def activate_worker(self, request):
        body = await request.json()
        inst_token = body['inst_token']
        ip_address = body['ip_address']

        inst = self.inst_pool.token_inst.get(inst_token)
        if not inst:
            log.warning(f'/activate_worker from unknown inst {inst_token}')
            raise web.HTTPNotFound()

        log.info(f'activating {inst}')
        inst.activate(ip_address)
        return web.Response()

    async def deactivate_worker(self, request):
        body = await request.json()
        inst_token = body['inst_token']

        inst = self.inst_pool.token_inst.get(inst_token)
        if not inst:
            log.warning(f'/deactivate_worker from unknown inst {inst_token}')
            raise web.HTTPNotFound()

        log.info(f'deactivating {inst}')
        await inst.deactivate()
        return web.Response()

    async def pod_complete(self, request):
        body = await request.json()
        inst_token = body['inst_token']
        status = body['status']

        inst = self.inst_pool.token_inst.get(inst_token)
        if not inst:
            log.warning(f'pod_complete from unknown inst {inst_token}')
            raise web.HTTPNotFound()

        pod_name = status['metadata']['name']
        pod = self.pods.get(pod_name)
        if pod is None:
            log.warning(f'pod_complete from unknown pod {pod_name}')
            return web.HTTPNotFound()

        pod._status = status

        await db.pods.update_record(pod_name, status=status)

        log.info(f'adding pod complete to event queue')
        await self.complete_queue.put(status)
        return web.Response()

    async def create_pod(self, spec, output_directory):
        name = spec['metadata']['name']

        if name in self.pods:
            raise Exception(f'pod {name} already exists')

        try:
            pod = await Pod.create_pod(name, spec, output_directory)
        except Exception as err:
            raise Exception(f'invalid pod spec given: {err}')

        self.pods[name] = pod
        await pod.put_on_ready(self)  # This isn't needed if pulling queue from db

    async def delete_pod(self, name):
        pod = self.pods.get(name)
        if pod is None:
            raise Exception(f'pod {name} does not exist')
        await self.pool.call(pod.delete)
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
        return [pod.status() for _, pod in self.pods.items()]

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

            should_wait = True
            if self.inst_pool.instances_by_free_cores and self.ready:
                inst = self.inst_pool.instances_by_free_cores[-1]
                i = self.ready.bisect_key_right(inst.free_cores)
                if i > 0:
                    pod = self.ready[i - 1]
                    assert pod.cores <= inst.free_cores
                    self.ready.remove(pod)
                    should_wait = False
                    log.info(f'scheduling {pod} cores {pod.cores} on {inst}')
                    await self.pool.call(pod.create, inst, self)

    async def run(self):
        await self.inst_pool.start()
        self.pool = AsyncWorkerPool(100)

        def _pod(record):
            pod = Pod.from_record(record)
            return pod.name, pod
        self.pods = dict([_pod(record) for record in await db.pods.get_records()])
        
        await self.schedule()
