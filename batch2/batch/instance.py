import time
import logging
import googleapiclient.errors

from hailtop import gear

gear.configure_logging()
log = logging.getLogger('instance')


class Instance:
    def __init__(self, inst_pool, inst_token):
        self.inst_pool = inst_pool
        self.pods = set()
        self.token = inst_token
        self.ip_address = None

        self.free_cores = inst_pool.worker_capacity

        # state: pending, active, deactivated (and/or deleted)
        self.pending = True
        self.active = False
        self.deleted = False

        self.inst_pool.n_pending_instances += 1
        self.inst_pool.free_cores += inst_pool.worker_capacity

        self.last_updated = time.time()

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    def machine_name(self):
        return self.inst_pool.token_machine_name(self.token)

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

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    async def deactivate(self):
        if self.pending:
            self.pending = False
            self.inst_pool.n_pending_instances -= 1
            self.inst_pool.free_cores -= self.inst_pool.worker_capacity
            assert not self.active

            log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')
            return

        if not self.active:
            return

        pod_list = list(self.pods)
        for p in pod_list:
            await p.unschedule()

        self.active = False
        self.inst_pool.instances_by_free_cores.remove(self)
        self.inst_pool.n_active_instances -= 1
        self.inst_pool.free_cores -= self.inst_pool.worker_capacity

        for p in pod_list:
            await p.put_on_ready(self.inst_pool.driver)
        assert not self.pods

        print(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

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
        await self.inst_pool.driver.gservices.delete_instance(self.machine_name())
        log.info(f'deleted machine {self.machine_name()}')
        self.deleted = True

    async def handle_preempt_event(self):
        await self.delete()
        self.update_timestamp()

    async def heal(self):
        try:
            spec = await self.inst_pool.driver.gservices.get_instance(self.machine_name())
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
        return self.machine_name()

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