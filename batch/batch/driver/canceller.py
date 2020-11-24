import logging
import asyncio

from hailtop.utils import (
    WaitableSharedPool, retry_long_running, run_if_changed)
from hailtop import aiotools

from ..batch import unschedule_job, mark_job_complete

log = logging.getLogger('driver')


class Box:
    def __init__(self, value):
        self.value = value


class Canceller:
    def __init__(self, app):
        self.app = app
        self.cancel_ready_state_changed = app['cancel_ready_state_changed']
        self.cancel_running_state_changed = app['cancel_running_state_changed']
        self.db = app['db']
        self.async_worker_pool = app['async_worker_pool']
        self.task_manager = aiotools.BackgroundTaskManager()

    async def async_init(self):
        self.task_manager.ensure_future(retry_long_running(
            'cancel_cancelled_ready_jobs_loop',
            run_if_changed, self.cancel_ready_state_changed, self.cancel_cancelled_ready_jobs_loop_body))
        self.task_manager.ensure_future(retry_long_running(
            'cancel_cancelled_running_jobs_loop',
            run_if_changed, self.cancel_running_state_changed, self.cancel_cancelled_running_jobs_loop_body))
        self.task_manager.ensure_future(retry_long_running(
            'bump_loop',
            self.bump_loop))

    def shutdown(self):
        try:
            self.task_manager.shutdown()
        finally:
            self.async_worker_pool.shutdown()

    async def bump_loop(self):
        while True:
            log.info('bump loop')
            self.cancel_ready_state_changed.set()
            self.cancel_running_state_changed.set()
            await asyncio.sleep(60)

    async def cancel_cancelled_ready_jobs_loop_body(self):
        records = self.db.select_and_fetchall(
            '''
SELECT user, n_cancelled_ready_jobs
FROM (SELECT user,
    CAST(COALESCE(SUM(n_cancelled_ready_jobs), 0) AS SIGNED) AS n_cancelled_ready_jobs
  FROM user_pool_resources
  GROUP BY user) AS t
WHERE n_cancelled_ready_jobs > 0;
''',
            timer_description='in cancel_cancelled_ready_jobs: aggregate n_cancelled_ready_jobs')
        user_n_cancelled_ready_jobs = {
            record['user']: record['n_cancelled_ready_jobs'] async for record in records
        }

        total = sum(user_n_cancelled_ready_jobs.values())
        if not total:
            should_wait = True
            return should_wait
        user_share = {
            user: max(int(300 * user_n_jobs / total + 0.5), 20)
            for user, user_n_jobs in user_n_cancelled_ready_jobs.items()
        }

        async def user_cancelled_ready_jobs(user, remaining):
            async for batch in self.db.select_and_fetchall(
                    '''
SELECT id, cancelled
FROM batches
WHERE user = %s AND `state` = 'running';
''',
                    (user,),
                    timer_description=f'in cancel_cancelled_ready_jobs: get {user} running batches'):
                if batch['cancelled']:
                    async for record in self.db.select_and_fetchall(
                            '''
SELECT jobs.job_id
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
WHERE batch_id = %s AND state = 'Ready' AND always_run = 0
LIMIT %s;
''',
                            (batch['id'], remaining.value),
                            timer_description=f'in cancel_cancelled_ready_jobs: get {user} batch {batch["id"]} ready cancelled jobs (1)'):
                        record['batch_id'] = batch['id']
                        yield record
                else:
                    async for record in self.db.select_and_fetchall(
                            '''
SELECT jobs.job_id
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
WHERE batch_id = %s AND state = 'Ready' AND always_run = 0 AND cancelled = 1
LIMIT %s;
''',
                            (batch['id'], remaining.value),
                            timer_description=f'in cancel_cancelled_ready_jobs: get {user} batch {batch["id"]} ready cancelled jobs (2)'):
                        record['batch_id'] = batch['id']
                        yield record

        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True
        for user, share in user_share.items():
            remaining = Box(share)
            async for record in user_cancelled_ready_jobs(user, remaining):
                batch_id = record['batch_id']
                job_id = record['job_id']
                id = (batch_id, job_id)
                log.info(f'cancelling job {id}')

                async def cancel_with_error_handling(app, batch_id, job_id, id):
                    try:
                        resources = []
                        await mark_job_complete(
                            app, batch_id, job_id, None, None,
                            'Cancelled', None, None, None, 'cancelled', resources)
                    except Exception:
                        log.info(f'error while cancelling job {id}', exc_info=True)
                await waitable_pool.call(
                    cancel_with_error_handling,
                    self.app, batch_id, job_id, id)

                remaining.value -= 1
                if remaining.value <= 0:
                    should_wait = False
                    break

        await waitable_pool.wait()

        return should_wait

    async def cancel_cancelled_running_jobs_loop_body(self):
        records = self.db.select_and_fetchall(
            '''
SELECT user, n_cancelled_running_jobs
FROM (SELECT user,
    CAST(COALESCE(SUM(n_cancelled_running_jobs), 0) AS SIGNED) AS n_cancelled_running_jobs
  FROM user_pool_resources
  GROUP BY user) AS t
WHERE n_cancelled_running_jobs > 0;
''',
            timer_description='in cancel_cancelled_running_jobs: aggregate n_cancelled_running_jobs')
        user_n_cancelled_running_jobs = {
            record['user']: record['n_cancelled_running_jobs'] async for record in records
        }

        total = sum(user_n_cancelled_running_jobs.values())
        if not total:
            should_wait = True
            return should_wait
        user_share = {
            user: max(int(300 * user_n_jobs / total + 0.5), 20)
            for user, user_n_jobs in user_n_cancelled_running_jobs.items()
        }

        async def user_cancelled_running_jobs(user, remaining):
            async for batch in self.db.select_and_fetchall(
                    '''
SELECT id
FROM batches
WHERE user = %s AND `state` = 'running' AND cancelled = 1;
''',
                    (user,),
                    timer_description=f'in cancel_cancelled_running_jobs: get {user} cancelled batches'):
                async for record in self.db.select_and_fetchall(
                        '''
SELECT jobs.job_id, attempts.attempt_id, attempts.instance_name
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_cancelled)
STRAIGHT_JOIN attempts
  ON attempts.batch_id = jobs.batch_id AND attempts.job_id = jobs.job_id
WHERE jobs.batch_id = %s AND state = 'Running' AND always_run = 0 AND cancelled = 0
LIMIT %s;
''',
                        (batch['id'], remaining.value),
                        timer_description=f'in cancel_cancelled_running_jobs: get {user} batch {batch["id"]} running cancelled jobs'):
                    record['batch_id'] = batch['id']
                    yield record

        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True
        for user, share in user_share.items():
            remaining = Box(share)
            async for record in user_cancelled_running_jobs(user, remaining):
                batch_id = record['batch_id']
                job_id = record['job_id']
                id = (batch_id, job_id)

                async def unschedule_with_error_handling(app, record, instance_name, id):
                    try:
                        await unschedule_job(app, record)
                    except Exception:
                        log.info(f'unscheduling job {id} on instance {instance_name}', exc_info=True)
                await waitable_pool.call(
                    unschedule_with_error_handling, self.app, record, record['instance_name'], id)

                remaining.value -= 1
                if remaining.value <= 0:
                    should_wait = False
                    break

        await waitable_pool.wait()

        return should_wait
