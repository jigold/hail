import time
import logging
import googleapiclient.errors
import asyncio
import aiohttp

from hailtop import gear

from .globals import get_db

gear.configure_logging()
log = logging.getLogger('instance')
db = get_db()


class Instance:
    @staticmethod
    def from_record(inst_pool, record):
        return Instance(inst_pool, record['name'], record['token'], record['ip_address'])

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

        return Instance(inst_pool, name, token, ip_address=None)

    def __init__(self, inst_pool, name, token, ip_address):
        self.inst_pool = inst_pool
        self.name = name
        self.token = token
        self.ip_address = ip_address

        # self.lock = asyncio.Lock()

        self.pods = set()
        self.free_cores = inst_pool.worker_capacity

        # state: pending, active, deactivated (and/or deleted)
        self.pending = True
        self.active = False
        self.deleted = False

        self.inst_pool.n_pending_instances += 1
        self.inst_pool.free_cores += inst_pool.worker_capacity

        self.last_updated = time.time()

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    def unschedule(self, pod):
        assert self.active
        self.pods.remove(self)

        self.inst_pool.instances_by_free_cores.remove(self)
        self.free_cores += pod.cores
        self.inst_pool.free_cores += pod.cores
        self.inst_pool.instances_by_free_cores.add(self)
        self.inst_pool.driver.changed.set()

    def schedule(self, pod):
        self.pods.add(pod)
        self.inst_pool.instances_by_free_cores.remove(self)
        self.free_cores -= pod.cores
        self.inst_pool.free_cores -= pod.cores
        self.inst_pool.instances_by_free_cores.add(self)
        # can't create more scheduling opportunities, don't set changed

    async def activate(self, ip_address):
        log.info(f'activating instance {self.name}')
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

        await db.instances.update_record(self.name,
                                         ip_address=ip_address)

        log.info(f'{self.inst_pool.n_pending_instances} pending {self.inst_pool.n_active_instances} active workers')

    async def deactivate(self):
        log.info(f'deactivating instance {self.name}')
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
        await asyncio.gather(*[p.unschedule() for p in pod_list])

        self.active = False
        self.inst_pool.instances_by_free_cores.remove(self)
        self.inst_pool.n_active_instances -= 1
        self.inst_pool.free_cores -= self.inst_pool.worker_capacity

        await asyncio.gather(*[p.put_on_ready(self.inst_pool.driver) for p in pod_list])
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
        await self.deactivate()
        self.deleted = True
        self.update_timestamp()

    async def delete(self):
        if self.deleted:
            return
        await self.deactivate()
        await self.inst_pool.driver.gservices.delete_instance(self.name)
        self.deleted = True
        log.info(f'deleted instance {self.name}')

    async def handle_preempt_event(self):
        await self.delete()
        self.update_timestamp()

    async def heal(self):
        try:
            async with aiohttp.ClientSession(
                    raise_for_status=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
                await session.get(f'http://{self.ip_address}:5000/healthcheck')
        except asyncio.CancelledError:  # pylint: disable=try-except-raise
            raise
        except Exception as err:  # pylint: disable=broad-except
            log.info(f'healthcheck failed for {self.name}; asking gce instead')
            try:
                spec = await self.inst_pool.driver.gservices.get_instance(self.name)
            except googleapiclient.errors.HttpError as e:
                if e.resp['status'] == '404':
                    await self.remove()
                    return

                status = spec['status']
                log.info(f'heal: machine {self.name} status {status}')

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
        return self.name
