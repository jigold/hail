import abc
import logging
import asyncio
import json
import random
import sortedcontainers

from hailtop.utils import (WaitableSharedPool, retry_long_running, run_if_changed,
    time_msecs, secret_alnum_string)
from hailtop import aiotools

from ..batch import schedule_job
from ..utils import WindowFractionCounter
from ..spec_writer import SpecWriter
from ..batch_format_version import BatchFormatVersion

log = logging.getLogger('driver')


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


class Scheduler:
    def __init__(self, app):
        self.app = app
        self.scheduler_state_changed = app['scheduler_state_changed']
        self.db = app['db']
        self.async_worker_pool = app['async_worker_pool']
        self.task_manager = aiotools.BackgroundTaskManager()

    def shutdown(self):
        try:
            self.task_manager.shutdown()
        finally:
            self.async_worker_pool.shutdown()

    async def async_init(self):
        self.task_manager.ensure_future(retry_long_running(
            'schedule_loop',
            run_if_changed, self.scheduler_state_changed, self.schedule_loop_body))
        self.task_manager.ensure_future(retry_long_running(
            'bump_loop',
            self.bump_loop))

    async def bump_loop(self):
        while True:
            log.info('bump loop')
            self.scheduler_state_changed.set()
            await asyncio.sleep(60)

    @abc.abstractmethod
    async def schedule_loop_body(self):
        raise NotImplementedError


class PoolScheduler(Scheduler):
    def __init__(self, app, pool):
        super(Scheduler).__init__(app)
        self.pool = pool
        self.exceeded_shares_counter = ExceededSharesCounter()

    async def compute_fair_share(self):
        free_cores_mcpu = sum([
            worker.free_cores_mcpu
            for worker in self.pool.healthy_instances_by_free_cores
        ])

        user_running_cores_mcpu = {}
        user_total_cores_mcpu = {}
        result = {}

        pending_users_by_running_cores = sortedcontainers.SortedSet(
            key=lambda user: user_running_cores_mcpu[user])
        allocating_users_by_total_cores = sortedcontainers.SortedSet(
            key=lambda user: user_total_cores_mcpu[user])

        records = self.db.execute_and_fetchall(
            '''
SELECT user,
  CAST(COALESCE(SUM(n_ready_jobs), 0) AS SIGNED) AS n_ready_jobs,
  CAST(COALESCE(SUM(ready_cores_mcpu), 0) AS SIGNED) AS ready_cores_mcpu,
  CAST(COALESCE(SUM(n_running_jobs), 0) AS SIGNED) AS n_running_jobs,
  CAST(COALESCE(SUM(running_cores_mcpu), 0) AS SIGNED) AS running_cores_mcpu
FROM user_pool_resources
WHERE pool = %s
GROUP BY user;
''',
            (self.pool.id,),
            timer_description='in compute_fair_share: aggregate user_pool_resources')

        async for record in records:
            user = record['user']
            user_running_cores_mcpu[user] = record['running_cores_mcpu']
            user_total_cores_mcpu[user] = record['running_cores_mcpu'] + record['ready_cores_mcpu']
            pending_users_by_running_cores.add(user)
            record['allocated_cores_mcpu'] = 0
            result[user] = record

        def allocate_cores(user, mark):
            result[user]['allocated_cores_mcpu'] = int(mark - user_running_cores_mcpu[user] + 0.5)

        mark = 0
        while free_cores_mcpu > 0 and (pending_users_by_running_cores or allocating_users_by_total_cores):
            lowest_running = None
            lowest_total = None

            if pending_users_by_running_cores:
                lowest_running_user = pending_users_by_running_cores[0]
                lowest_running = user_running_cores_mcpu[lowest_running_user]
                if lowest_running == mark:
                    pending_users_by_running_cores.remove(lowest_running_user)
                    allocating_users_by_total_cores.add(lowest_running_user)
                    continue

            if allocating_users_by_total_cores:
                lowest_total_user = allocating_users_by_total_cores[0]
                lowest_total = user_total_cores_mcpu[lowest_total_user]
                if lowest_total == mark:
                    allocating_users_by_total_cores.remove(lowest_total_user)
                    allocate_cores(lowest_total_user, mark)
                    continue

            allocation = min([c for c in [lowest_running, lowest_total] if c is not None])

            n_allocating_users = len(allocating_users_by_total_cores)
            cores_to_allocate = n_allocating_users * (allocation - mark)

            if cores_to_allocate > free_cores_mcpu:
                mark += int(free_cores_mcpu / n_allocating_users + 0.5)
                free_cores_mcpu = 0
                break

            mark = allocation
            free_cores_mcpu -= cores_to_allocate

        for user in allocating_users_by_total_cores:
            allocate_cores(user, mark)

        return result

    async def schedule_loop_body(self):
        log.info('schedule: starting')
        start = time_msecs()
        n_scheduled = 0

        user_resources = await self.compute_fair_share()

        total = sum(resources['allocated_cores_mcpu']
                    for resources in user_resources.values())
        if not total:
            log.info('schedule: no allocated cores')
            should_wait = True
            return should_wait
        user_share = {
            user: max(int(300 * resources['allocated_cores_mcpu'] / total + 0.5), 20)
            for user, resources in user_resources.items()
        }

        async def user_runnable_jobs(user, remaining):
            async for batch in self.db.select_and_fetchall(
                    '''
SELECT id, cancelled, userdata, user, format_version
FROM batches
WHERE user = %s AND `state` = 'running';
''',
                    (user,),
                    timer_description=f'in schedule: get {user} running batches'):
                async for record in self.db.select_and_fetchall(
                        '''
SELECT job_id, spec, cores_mcpu
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
WHERE batch_id = %s AND state = 'Ready' AND always_run = 1 AND pool = %s
LIMIT %s;
''',
                        (batch['id'], self.pool.id, remaining.value),
                        timer_description=f'in schedule: get {user} batch {batch["id"]} runnable jobs (1)'):
                    record['batch_id'] = batch['id']
                    record['userdata'] = batch['userdata']
                    record['user'] = batch['user']
                    record['format_version'] = batch['format_version']
                    yield record
                if not batch['cancelled']:
                    async for record in self.db.select_and_fetchall(
                            '''
SELECT job_id, spec, cores_mcpu
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
WHERE batch_id = %s AND state = 'Ready' AND always_run = 0 AND pool = %s AND cancelled = 0
LIMIT %s;
''',
                            (batch['id'], self.pool.id, remaining.value),
                            timer_description=f'in schedule: get {user} batch {batch["id"]} runnable jobs (2)'):
                        record['batch_id'] = batch['id']
                        record['userdata'] = batch['userdata']
                        record['user'] = batch['user']
                        record['format_version'] = batch['format_version']
                        yield record

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
            async for record in user_runnable_jobs(user, remaining):
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

                instance = await self.pool.get_instance(user, record)
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


class JobPrivateInstanceScheduler(Scheduler):
    def __init__(self, app):
        super(Scheduler).__init__(app)
        self.job_private_inst_manager = app['job_private_inst_manager']
        self.log_store = app['log_store']

    async def schedule_loop_body(self):
        records = self.db.select_and_fetchall(
            '''
SELECT user, n_ready_jobs
FROM (SELECT user,
    CAST(COALESCE(SUM(n_cancelled_ready_jobs), 0) AS SIGNED) AS n_ready_jobs
  FROM user_pool_resources
  WHERE pool IS NULL
  GROUP BY user) AS t
WHERE n_ready_jobs > 0;
''',
            timer_description='in schedule_loop_body for job private instances: aggregate n_ready_jobs')
        user_n_ready_jobs = {
            record['user']: record['n_ready_jobs'] async for record in records
        }

        total = sum(user_n_ready_jobs.values())
        if not total:
            should_wait = True
            return should_wait
        user_share = {
            user: max(int(300 * user_n_jobs / total + 0.5), 20)
            for user, user_n_jobs in user_n_ready_jobs.items()
        }

        async def job_private_instance_ready_jobs(user, remaining):
            async for batch in self.db.select_and_fetchall(
                    '''
SELECT id, cancelled, userdata, user, format_version
FROM batches
WHERE user = %s AND `state` = 'running';
''',
                    (user,),
                    timer_description=f'in schedule: get {user} running batches'):
                async for record in self.db.select_and_fetchall(
                        '''
SELECT job_id, spec
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
WHERE batch_id = %s AND state = 'Ready' AND always_run = 1 AND pool IS NULL
LIMIT %s;
''',
                        (batch['id'], remaining.value),
                        timer_description=f'in schedule: get {user} batch {batch["id"]} job private instance runnable jobs (1)'):
                    record['batch_id'] = batch['id']
                    record['userdata'] = batch['userdata']
                    record['user'] = batch['user']
                    record['format_version'] = batch['format_version']
                    yield record
                if not batch['cancelled']:
                    async for record in self.db.select_and_fetchall(
                            '''
SELECT job_id, spec
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
WHERE batch_id = %s AND state = 'Ready' AND always_run = 0 AND pool IS NULL AND cancelled = 0
LIMIT %s;
''',
                            (batch['id'], remaining.value),
                            timer_description=f'in schedule: get {user} batch {batch["id"]} job private instance runnable jobs (2)'):
                        record['batch_id'] = batch['id']
                        record['userdata'] = batch['userdata']
                        record['user'] = batch['user']
                        record['format_version'] = batch['format_version']
                        yield record

        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True
        for user, share in user_share.items():
            remaining = Box(share)
            async for record in job_private_instance_ready_jobs(user, remaining):
                batch_id = record['batch_id']
                job_id = record['job_id']
                id = (batch_id, job_id)

                format_version =  BatchFormatVersion(record['format_version'])
                assert format_version.has_full_spec_in_gcs()

                token, start_job_id = await SpecWriter.get_token_start_id(self.db, batch_id, job_id)
                job_spec = await self.log_store.read_spec_file(batch_id, token, start_job_id, job_id)
                job_spec = json.loads(job_spec)

                if self.job_private_inst_manager.n_instances >= self.job_private_inst_manager.max_instances:
                    should_wait = True
                    break

                async def create_instance_with_error_handling(batch_id, job_id, id, job_spec):
                    try:
                        self.job_private_inst_manager.create_instance(batch_id, job_id, job_spec)
                    except Exception:
                        log.info(f'error while creating private instance for job {id}', exc_info=True)

                await waitable_pool.call(
                    create_instance_with_error_handling,
                    batch_id, job_id, id, job_spec)

                remaining.value -= 1
                if remaining.value <= 0:
                    should_wait = False
                    break

        await waitable_pool.wait()

        return should_wait
