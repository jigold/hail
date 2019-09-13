import time
import logging
import googleapiclient.errors
import asyncio
import aiohttp

from .globals import get_db

log = logging.getLogger('instance')
db = get_db()


class Instance:
    @staticmethod
    def from_record(inst_pool, record):
        inst = Instance(inst_pool, record['name'], record['token'],
                        ip_address=record['ip_address'], state='Active')

        inst_pool.n_active_instances += 1
        inst_pool.instances_by_free_cores.add(inst)
        inst_pool.free_cores += inst_pool.worker_capacity

        return inst

    @staticmethod
    async def create(inst_pool, name, token):
        # await db.instances.new_record(
        #     name=name,
        #     token=token,
        #     cores=inst_pool.worker_cores,
        #     memory=inst_pool.worker_memory,
        #     disk_size=inst_pool.worker_disk_size_gb,
        #     image=inst_pool.worker_image,
        #     type=inst_pool.worker_type
        # )

        await db.instances.new_record(name=name,
                                      token=token)

        inst_pool.n_pending_instances += 1
        inst_pool.free_cores += inst_pool.worker_capacity

        return Instance(inst_pool, name, token, ip_address=None, state='Pending')

    def __init__(self, inst_pool, name, token, ip_address, state):
        self.inst_pool = inst_pool
        self.name = name
        self.token = token
        self.ip_address = ip_address

        # self.lock = asyncio.Lock()

        self.pods = set()
        self.free_cores = inst_pool.worker_capacity

        # state: pending, active, deactivated (and/or deleted)
        self.state = state
        # self.pending = True
        # self.active = False
        # self.deleted = False

        self.last_updated = time.time()

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    def unschedule(self, pod):
        log.info(f'unschedule pod {pod.name} on instance {self.name}')
        self.pods.remove(pod)

        self.inst_pool.instances_by_free_cores.remove(self)
        self.free_cores += pod.cores
        self.inst_pool.free_cores += pod.cores
        self.inst_pool.instances_by_free_cores.add(self)
        self.inst_pool.driver.changed.set()

    def schedule(self, pod):
        log.info(f'schedule pod {pod.name} on instance {self.name}')
        self.pods.add(pod)
        self.inst_pool.instances_by_free_cores.remove(self)
        self.free_cores -= pod.cores
        self.inst_pool.free_cores -= pod.cores
        self.inst_pool.instances_by_free_cores.add(self)
        # can't create more scheduling opportunities, don't set changed

    async def activate(self, ip_address):
        log.info(f'activating instance {self.name}')
        if self.state == 'Active':
            return
        if self.state == 'Deleted':
            return

        if self.state == 'Pending':
            self.inst_pool.n_pending_instances -= 1
            self.inst_pool.free_cores -= self.inst_pool.worker_capacity

        self.state = 'Active'
        self.ip_address = ip_address
        self.inst_pool.n_active_instances += 1
        self.inst_pool.instances_by_free_cores.add(self)
        self.inst_pool.free_cores += self.inst_pool.worker_capacity
        self.inst_pool.driver.changed.set()

        await db.instances.update_record(self.name,
                                         ip_address=ip_address)

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    async def deactivate(self):
        log.info(f'deactivating instance {self.name}')
        if self.state == 'Pending':
            self.state = 'Deactivated'
            self.inst_pool.n_pending_instances -= 1
            self.inst_pool.free_cores -= self.inst_pool.worker_capacity
            log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')
            return

        if self.state != 'Active':
            log.info(f'instance {self.name} already deactivated')
            return

        self.state = 'Deactivated'
        self.inst_pool.instances_by_free_cores.remove(self)
        self.inst_pool.n_active_instances -= 1
        self.inst_pool.free_cores -= self.inst_pool.worker_capacity

        async def _remove_pod(pod):
            await pod.unschedule()
            await pod.put_on_ready(self.inst_pool.driver)

        pod_list = list(self.pods)
        await asyncio.gather(*[_remove_pod(p) for p in pod_list])
        assert not self.pods



        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    def update_timestamp(self):
        if self in self.inst_pool.instances:
            self.inst_pool.instances.remove(self)
            self.last_updated = time.time()
            self.inst_pool.instances.add(self)

    async def remove(self):
        log.info(f'removing instance {self.name}')
        await self.deactivate()
        self.inst_pool.instances.remove(self)
        if self.token in self.inst_pool.token_inst:
            del self.inst_pool.token_inst[self.token]
        await db.instances.delete_record(self.name)

    async def handle_call_delete_event(self):
        log.info(f'handling call delete event for {self.name}')
        await self.deactivate()
        self.state = 'Deleted'
        self.update_timestamp()

    async def delete(self):
        log.info(f'deleting instance {self.name}')
        if self.state == 'Deleted':
            return
        await self.deactivate()
        await self.inst_pool.driver.gservices.delete_instance(self.name)
        self.state = 'Deleted'
        log.info(f'deleted instance {self.name}')

    async def handle_preempt_event(self):
        log.info(f'handling preemption event for {self.name}')
        await self.delete()
        self.update_timestamp()

    async def heal(self):
        log.info(f'healing instance {self.name}')

        async def _heal_gce():
            try:
                spec = await self.inst_pool.driver.gservices.get_instance(self.name)
            except googleapiclient.errors.HttpError as e:
                if e.resp['status'] == '404':
                    await self.remove()
                    return

                status = spec['status']
                log.info(f'heal: machine {self.name} status {status}')

                # preempted goes into terminated state
                if status == 'TERMINATED' and self.state == 'Deleted':
                    await self.remove()
                    return

                if status in ('TERMINATED', 'STOPPING'):
                    await self.deactivate()

                if status == 'TERMINATED' and self.state != 'Deleted':
                    await self.delete()

        if self.ip_address:
            try:
                async with aiohttp.ClientSession(
                        raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
                    await session.get(f'http://{self.ip_address}:5000/healthcheck')
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception as err:  # pylint: disable=broad-except
                log.info(f'healthcheck failed for {self.name} due to err {err}; asking gce instead')
                await _heal_gce()
        else:
            await _heal_gce()

        self.update_timestamp()

    def __str__(self):
        return self.name
