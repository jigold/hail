import abc
import asyncio

from hailtop.utils import run_if_changed, retry_long_running, AsyncWorkerPool, WaitableSharedPool
from hailtop import aiotools

from ..utils import WindowFractionCounter


class Box:
    def __init__(self, value):
        self.value = value


class ExceededSharesCounter:
    def __init__(self):
        self._global_counter = WindowFractionCounter(10)

    def push(self, success: bool):
        self._global_counter.push('exceeded_shares', success)

    def rate(self) -> float:
        return self._global_counter.fraction()

    def __repr__(self):
        return f'global {self._global_counter}'


class ResourceAllocator:
    def __init__(self, app, event, name):
        self.app = app
        self.event = event
        self.name = name
        self.async_worker_pool = AsyncWorkerPool(parallelism=100, queue_size=100)
        self.task_manager = aiotools.BackgroundTaskManager()

    async def async_init(self):
        self.task_manager.ensure_future(retry_long_running(
            self.name,
            run_if_changed, self.event, self.resource_allocation_loop_body))
        self.task_manager.ensure_future(retry_long_running(
            'bump_loop',
            self.bump_loop))

    def shutdown(self):
        try:
            self.task_manager.shutdown()
        finally:
            self.async_worker_pool.shutdown()

    @abc.abstractmethod
    async def compute_user_share(self, *args):
        raise NotImplementedError

    @abc.abstractmethod
    async def resource_generator(self, user, remaining):
        yield

    @abc.abstractmethod
    async def allocate_resource(self, waitable_pool, record):
        raise NotImplementedError

    async def bump_loop(self):
        while True:
            log.info('bump loop')
            self.event.set()
            await asyncio.sleep(60)

    async def resource_allocation_loop_body(self):
        start = time_msecs()
        n_allocated = 0
        user_share = self.compute_user_share()

        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True
        for user, share in user_share.items():
            remaining = Box(share)
            async for record in self.resource_generator(user, remaining):
                await waitable_pool.call(
                    self.allocate_resource,
                    self.app, record)

                remaining.value -= 1
                if remaining.value <= 0:
                    should_wait = False
                    break

        await waitable_pool.wait()

        return should_wait

    async def schedule_loop_body(self):
        log.info('schedule: starting')
        start = time_msecs()
        n_scheduled = 0

        user_share = self.compute_user_share()

        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True
        for user, resources in user_resources.items():

            allocated_cores_mcpu = resources['allocated_cores_mcpu']
            if allocated_cores_mcpu == 0:
                continue

            scheduled_cores_mcpu = 0
            share = user_share[user]

            log.info(f'schedule: user-share: {user}: {allocated_cores_mcpu} {share}')

            remaining = Box(share)
            async for record in self.resource_generator(user, remaining):
                batch_id = record['batch_id']
                job_id = record['job_id']
                id = (batch_id, job_id)
                attempt_id = secret_alnum_string(6)
                record['attempt_id'] = attempt_id

                if scheduled_cores_mcpu + record['cores_mcpu'] > allocated_cores_mcpu:
                    if random.random() > self.exceeded_shares_counter.rate():
                        self.exceeded_shares_counter.push(True)
                        self.scheduler_state_changed.set()
                        break
                    self.exceeded_shares_counter.push(False)

                instance = await self.inst_group.get_instance(user, record)
                if instance:
                    instance.adjust_free_cores_in_memory(-record['cores_mcpu'])
                    scheduled_cores_mcpu += record['cores_mcpu']
                    n_scheduled += 1
                    should_wait = False

                    async def schedule_with_error_handling(app, record, id, instance):
                        try:
                            await schedule_job(app, record, instance)
                        except Exception:
                            log.info(f'scheduling job {id} on {instance}', exc_info=True)
                    await waitable_pool.call(
                        schedule_with_error_handling, self.app, record, id, instance)

                remaining.value -= 1
                if remaining.value <= 0:
                    break

        await waitable_pool.wait()

        end = time_msecs()
        log.info(f'schedule: scheduled {n_scheduled} jobs in {end - start}ms')

        return should_wait
