import re
import random
import json
import logging
import asyncio
import secrets
import sortedcontainers

from typing import Optional, Dict, Any

from hailtop.utils import (Notice, run_if_changed,
                           WaitableSharedPool, time_msecs, retry_long_running,
                           secret_alnum_string, AsyncWorkerPool)

from ..batch_format_version import BatchFormatVersion
from ..batch_configuration import WORKER_MAX_IDLE_TIME_MSECS
from .create_instance import create_instance
from .instance_collection import InstanceCollection
from .instance import Instance
from .job import mark_job_started, schedule_job
from ..utils import worker_memory_per_core_bytes, Box

log = logging.getLogger('job_private_inst_coll')


MACHINE_TYPE_REGEX = re.compile('(?P<machine_family>[^-]+)-(?P<machine_type>[^-]+)-(?P<cores>\\d+)')


def machine_type_to_dict(machine_type: str) -> Optional[Dict[str, Any]]:
    match = MACHINE_TYPE_REGEX.search(machine_type)
    return match


class JobPrivateInstanceCollection(InstanceCollection):
    def __init__(self, app, name, machine_name_prefix):
        super().__init__(app, name, machine_name_prefix)

        self.app = app

        global_scheduler_state_changed: Notice = app['scheduler_state_changed']
        self.create_instances_state_changed = global_scheduler_state_changed.subscribe()
        self.scheduler_state_changed = asyncio.Event()

        self.async_worker_pool: AsyncWorkerPool = app['async_worker_pool']

    async def async_init(self):
        log.info(f'initializing {self}')

        await super().async_init()

        row = await self.db.select_and_fetchone('''
SELECT boot_disk_size_gb, max_instances, max_live_instances
FROM inst_colls
WHERE name = %s;
''',
                                                (self.name,))

        self.boot_disk_size_gb = row['boot_disk_size_gb']
        self.max_instances = row['max_instances']
        self.max_live_instances = row['max_live_instances']

        async for record in self.db.select_and_fetchall(
                'SELECT * FROM instances WHERE removed = 0 AND inst_coll = %s;',
                (self.name,)):
            instance = Instance.from_record(self.app, self, record)
            self.add_instance(instance)

        self.task_manager.ensure_future(retry_long_running(
            'create_instances_loop',
            run_if_changed, self.create_instances_state_changed, self.create_instances_loop_body))

        self.task_manager.ensure_future(retry_long_running(
            'schedule_jobs_loop',
            run_if_changed, self.scheduler_state_changed, self.schedule_jobs_loop_body))

    def config(self):
        return {
            'name': self.name,
            'worker_disk_size_gb': self.boot_disk_size_gb,
            'max_instances': self.max_instances,
            'max_live_instances': self.max_live_instances
        }

    async def configure(
            self, boot_disk_size_gb, max_instances, max_live_instances):
        await self.db.just_execute(
            '''
UPDATE inst_colls
SET boot_disk_size_gb = %s, max_instances = %s, max_live_instances = %s
WHERE name = %s;
''',
            (boot_disk_size_gb, max_instances, max_live_instances, self.name))

        self.boot_disk_size_gb = boot_disk_size_gb
        self.max_instances = max_instances
        self.max_live_instances = max_live_instances

    async def schedule_jobs_loop_body(self):
        log.info(f'starting scheduling jobs loop')
        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True

        n_scheduled = 0
        async for record in self.db.select_and_fetchall(
                '''
SELECT jobs.batch_id, jobs.job_id, jobs.attempt_id, batches.format_version,
  attempts.instance_name
FROM batches
INNER JOIN jobs ON batches.id = jobs.batch_id
LEFT JOIN attempts ON jobs.batch_id = attempts.batch_id AND jobs.job_id = jobs.batch_id
LEFT JOIN instances ON attempts.instance_name = instances.name
WHERE batches.state = 'running'
  AND jobs.state = 'Ready'
  AND (jobs.always_run OR NOT jobs.cancelled)
  AND jobs.inst_coll = %s
  AND instances.`state` = 'active'
ORDER BY instances.time_activated ASC
LIMIT 300;
''',
                (self.name,),
                timer_description=f'in schedule_jobs for {self}: get ready jobs with active instances'):
            batch_id = record['batch_id']
            job_id = record['job_id']
            instance_name = record['instance_name']
            id = (batch_id, job_id)
            log.info(f'scheduling job {id}')

            instance = self.name_instance[instance_name]
            n_scheduled += 1
            should_wait = False

            async def schedule_with_error_handling(app, record, id, instance):
                try:
                    await schedule_job(app, record, instance)
                except Exception:
                    log.info(f'scheduling job {id} on {instance} for {self}', exc_info=True)

            await waitable_pool.call(
                schedule_with_error_handling, self.app, record, id, instance)

            n_scheduled += 1

        await waitable_pool.wait()

        return should_wait

    def max_instances_to_create(self):
        n_live_instances = self.n_instances_by_state['pending'] + self.n_instances_by_state['active']

        return min(self.max_live_instances - n_live_instances,
                   self.max_instances - self.n_instances,
                   # 20 queries/s; our GCE long-run quota
                   300)

    async def compute_fair_share(self):
        n_jobs_to_allocate = self.max_instances_to_create()

        user_running_jobs = {}
        user_total_jobs = {}
        result = {}

        pending_users_by_running_jobs = sortedcontainers.SortedSet(
            key=lambda user: user_running_jobs[user])
        allocating_users_by_total_jobs = sortedcontainers.SortedSet(
            key=lambda user: user_total_jobs[user])

        records = self.db.execute_and_fetchall(
            '''
SELECT user,
  CAST(COALESCE(SUM(n_ready_jobs), 0) AS SIGNED) AS n_ready_jobs,
  CAST(COALESCE(SUM(n_running_jobs), 0) AS SIGNED) AS n_running_jobs
FROM user_inst_coll_resources
WHERE inst_coll = %s
GROUP BY user
HAVING n_ready_jobs + n_running_jobs > 0;
''',
            (self.name,),
            timer_description=f'in compute_fair_share for {self}: aggregate user_inst_coll_resources')

        async for record in records:
            user = record['user']
            user_running_jobs[user] = record['n_running_jobs']
            user_total_jobs[user] = record['n_ready_jobs'] + record['n_running_jobs']
            pending_users_by_running_jobs.add(user)
            record['n_allocated_jobs'] = 0
            result[user] = record

        def allocate_jobs(user, mark):
            result[user]['n_allocated_jobs'] = mark - user_running_jobs[user]

        mark = 0
        while n_jobs_to_allocate > 0 and (pending_users_by_running_jobs or allocating_users_by_total_jobs):
            lowest_running = None
            lowest_total = None

            if pending_users_by_running_jobs:
                lowest_running_user = pending_users_by_running_jobs[0]
                lowest_running = user_running_jobs[lowest_running_user]
                if lowest_running == mark:
                    pending_users_by_running_jobs.remove(lowest_running_user)
                    allocating_users_by_total_jobs.add(lowest_running_user)
                    continue

            if allocating_users_by_total_jobs:
                lowest_total_user = allocating_users_by_total_jobs[0]
                lowest_total = user_total_jobs[lowest_total_user]
                if lowest_total == mark:
                    allocating_users_by_total_jobs.remove(lowest_total_user)
                    allocate_jobs(lowest_total_user, mark)
                    continue

            allocation = min([c for c in [lowest_running, lowest_total] if c is not None])

            n_allocating_users = len(allocating_users_by_total_jobs)
            jobs_to_allocate = n_allocating_users * (allocation - mark)

            if jobs_to_allocate > n_jobs_to_allocate:
                mark += int(n_jobs_to_allocate / n_allocating_users + 0.5)
                n_jobs_to_allocate = 0
                break

            mark = allocation
            n_jobs_to_allocate -= jobs_to_allocate

        for user in allocating_users_by_total_jobs:
            allocate_jobs(user, mark)

        return result

    async def create_instance(self, batch_id, job_id, attempt_id, machine_spec):
        assert machine_spec is not None

        machine_name = self.generate_machine_name()
        machine_type = machine_spec['machine_type']
        preemptible = machine_spec['preemptible']
        storage_gb = machine_spec['storage_gib']

        machine_type_dict = machine_type_to_dict(machine_type)
        cores = int(machine_type_dict['cores'])
        worker_type = machine_type_dict['machine_type']

        zone = self.zone_monitor.get_zone(cores, False, storage_gb)
        if zone is None:
            return

        activation_token = secrets.token_urlsafe(32)
        instance = await Instance.create(self.app, self, machine_name, activation_token, cores * 1000,
                                         zone, machine_type, preemptible)
        self.add_instance(instance)
        log.info(f'created {instance} for {(batch_id, job_id)}')

        worker_config = await create_instance(app=self.app,
                                              zone=zone,
                                              machine_name=machine_name,
                                              machine_type=machine_type,
                                              activation_token=activation_token,
                                              max_idle_time_msecs=WORKER_MAX_IDLE_TIME_MSECS,
                                              worker_local_ssd_data_disk=False,
                                              worker_pd_ssd_data_disk_size_gb=storage_gb,
                                              boot_disk_size_gb=self.boot_disk_size_gb,
                                              preemptible=preemptible)

        memory_in_bytes = worker_memory_per_core_bytes(worker_type)
        resources = worker_config.resources(cores * 1000, memory_in_bytes)

        start_time = time_msecs()

        await mark_job_started(self.app, batch_id, job_id, attempt_id, instance, start_time, resources)

#     async def create_instances_loop_body(self):
#         log.info(f'create_instances for {self}: starting')
#         start = time_msecs()
#         n_total_created = 0
#
#         user_resources = await self.compute_fair_share()
#
#         total = sum(resources['allocated_jobs']
#                     for resources in user_resources.values())
#
#         if total == 0:
#             log.info(f'create_instances {self}: no allocated jobs')
#             should_wait = True
#             return should_wait
#
#         user_share = {
#             user: max(int(300 * resources['allocated_jobs'] / total + 0.5), 20)
#             for user, resources in user_resources.items()
#         }
#
#         waitable_pool = WaitableSharedPool(self.async_worker_pool)
#
#         should_wait = True
#         for user, resources in user_resources.items():
#             n_allocated_jobs = resources['allocated_jobs']
#
#             share = user_share[user]
#             n_created = 0
#
#             log.info(f'create_instances for {self}: user-share: {user}: {n_allocated_jobs} {share}')
#
#             if n_allocated_jobs == 0:
#                 continue
#
#             async for record in self.db.select_and_fetchall(
#                     '''
# SELECT jobs.batch_id, jobs.job_id, userdata, user, format_version,
# spec, jobs.cores_mcpu
# FROM batches
# INNER JOIN jobs ON batches.id = jobs.batch_id
# LEFT JOIN attempts ON jobs.batch_id = attempts.batch_id AND jobs.job_id = attempts.job_id
# LEFT JOIN instances ON attempts.instance_name = instances.name
# WHERE batches.user = %s
#   AND batches.state = 'running'
#   AND jobs.state = 'Ready'
#   AND (jobs.always_run OR NOT jobs.cancelled)
#   AND jobs.inst_coll = %s
#   AND NOT ((attempts.instance_name) IN (
#     SELECT `name` FROM instances
#     WHERE `state` = 'pending' OR `state` = 'active'
#   ))
# LIMIT %s;
# ''',
#                     (user, self.name, share + 1),
#                     timer_description=f'create_instances for {self}: get {user} runnable jobs'):
#                 batch_id = record['batch_id']
#                 job_id = record['job_id']
#                 id = (batch_id, job_id)
#                 attempt_id = secret_alnum_string(6)
#
#                 log.info(f'creating job private instance for job {id}')
#
#                 async def create_instance_with_error_handling(batch_id, job_id, attempt_id, record):
#                     id = (batch_id, job_id)
#                     try:
#                         batch_format_version = BatchFormatVersion(record['format_version'])
#                         spec = json.loads(record['spec'])
#                         machine_spec = batch_format_version.get_spec_machine_spec(spec)
#                         await self.create_instance(batch_id, job_id, attempt_id, machine_spec)
#                     except Exception:
#                         log.info(f'creating job private instance for job {id}', exc_info=True)
#
#                 await waitable_pool.call(
#                     create_instance_with_error_handling, batch_id, job_id, attempt_id, record)
#
#                 n_created += 1
#                 n_total_created += 1
#
#                 if n_created == share:
#                     should_wait = False
#                     break
#
#         await waitable_pool.wait()
#
#         end = time_msecs()
#         log.info(f'created {n_total_created} instances in {end - start}ms for {self}')
#
#         if n_total_created >= self.max_instances_to_create():
#             await asyncio.sleep(15)  # ensure we don't create more instances than GCE limit
#
#         return should_wait

    async def create_instances_loop_body(self):
        log.info(f'create_instances for {self}: starting')
        start = time_msecs()
        n_instances_created = 0

        user_resources = await self.compute_fair_share()

        total = sum(resources['n_allocated_jobs']
                    for resources in user_resources.values())
        if not total:
            log.info(f'create_instances {self}: no allocated jobs')
            should_wait = True
            return should_wait
        user_share = {
            user: max(int(300 * resources['n_allocated_jobs'] / total + 0.5), 20)
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
                    timer_description=f'in create_instances {self}: get {user} running batches'):
                async for record in self.db.select_and_fetchall(
                        '''
SELECT job_id, spec, cores_mcpu
FROM jobs FORCE INDEX(jobs_batch_id_state_always_run_inst_coll_cancelled)
LEFT JOIN attempts ON jobs.batch_id = attempts.batch_id AND jobs.job_id = attempts.job_id
WHERE batch_id = %s AND state = 'Ready' AND always_run = 1 AND inst_coll = %s
AND NOT ((attempts.instance_name) IN (
    SELECT `name` FROM instances
    WHERE `state` = 'pending' OR `state` = 'active'
  ))
LIMIT %s;
''',
                        (batch['id'], self.name, remaining.value),
                        timer_description=f'in create_instances {self}: get {user} batch {batch["id"]} runnable jobs (1)'):
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
LEFT JOIN attempts ON jobs.batch_id = attempts.batch_id AND jobs.job_id = attempts.job_id
WHERE batch_id = %s AND state = 'Ready' AND always_run = 0 AND inst_coll = %s AND cancelled = 0
AND NOT ((attempts.instance_name) IN (
    SELECT `name` FROM instances
    WHERE `state` = 'pending' OR `state` = 'active'
  ))
LIMIT %s;
''',
                            (batch['id'], self.name, remaining.value),
                            timer_description=f'in create_instances {self}: get {user} batch {batch["id"]} runnable jobs (2)'):
                        record['batch_id'] = batch['id']
                        record['userdata'] = batch['userdata']
                        record['user'] = batch['user']
                        record['format_version'] = batch['format_version']
                        yield record

        waitable_pool = WaitableSharedPool(self.async_worker_pool)

        should_wait = True
        for user, resources in user_resources.items():
            n_allocated_instances = resources['n_allocated_jobs']
            if n_allocated_instances == 0:
                continue

            n_user_instances_created = 0

            share = user_share[user]

            log.info(f'create_instances {self}: user-share: {user}: {share}')

            remaining = Box(share)
            async for record in user_runnable_jobs(user, remaining):
                batch_id = record['batch_id']
                job_id = record['job_id']
                id = (batch_id, job_id)
                attempt_id = secret_alnum_string(6)
                record['attempt_id'] = attempt_id

                if n_user_instances_created >= n_allocated_instances:
                    # FIXME: Add exceeded shares once this is all working
                    break

                n_instances_created += 1
                n_user_instances_created += 1
                should_wait = False

                log.info(f'creating job private instance for job {id}')

                async def create_instance_with_error_handling(batch_id, job_id, attempt_id, record):
                    try:
                        batch_format_version = BatchFormatVersion(record['format_version'])
                        spec = json.loads(record['spec'])
                        machine_spec = batch_format_version.get_spec_machine_spec(spec)
                        await self.create_instance(batch_id, job_id, attempt_id, machine_spec)
                    except Exception:
                        log.info(f'creating job private instance for job {id}', exc_info=True)

                await waitable_pool.call(
                    create_instance_with_error_handling, batch_id, job_id, attempt_id, record)

                remaining.value -= 1
                if remaining.value <= 0:
                    break

        await waitable_pool.wait()

        end = time_msecs()
        log.info(f'create_instances: created instances for {n_instances_created} jobs in {end - start}ms for {self}')

        await asyncio.sleep(15)  # ensure we don't create more instances than GCE limit

        return should_wait

    def __str__(self):
        return f'jpic {self.name}'
