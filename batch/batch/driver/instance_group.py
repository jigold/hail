import abc
import asyncio
import logging
import collections
import sortedcontainers

from hailtop import aiotools
from hailtop.utils import retry_long_running, time_msecs

from .scheduler import Scheduler
from .instance import Instance
from ..batch_configuration import ENABLE_STANDING_WORKER, STANDING_WORKER_MAX_IDLE_TIME_MSECS

log = logging.getLogger('instance_group')


class InstanceGroup:
    def __init__(self, app, name):
        self.app = app
        self.name = name
        self.scheduler = None
        self.task_manager = aiotools.BackgroundTaskManager()

    def shutdown(self):
        try:
            self.scheduler.shutdown()
        finally:
            self.task_manager.shutdown()

    async def async_init(self):
        pass

    async def run(self):
        await self.scheduler.async_init()

        self.task_manager.ensure_future(retry_long_running(
            'control_loop',
            self.control_loop))

    @abc.abstractmethod
    async def get_instance(self, user, cores_mcpu):
        raise NotImplementedError

    @abc.abstractmethod
    def adjust_for_remove_instance(self, instance):
        raise NotImplementedError

    @abc.abstractmethod
    def adjust_for_add_instance(self, instance):
        raise NotImplementedError

    @abc.abstractmethod
    async def control_loop(self):
        raise NotImplementedError

    def __str__(self):
        return f'{self.name}'


class Pool(InstanceGroup):
    @staticmethod
    def from_record(app, record):
        return Pool(app, record['name'], record['type'], record['cores'], record['disk_size_gb'],
                    record['local_ssd_data_disk'], record['pd_ssd_data_disk_size_gb'],
                    record['pool_size'], record['max_instances'], record['standing_worker'],
                    record['standing_worker_cores'])

    def __init__(self, app, name, typ, cores, disk_size_gb, local_ssd_data_disk,
                 pd_ssd_data_disk_size_gb, pool_size, max_instances, standing_worker,
                 standing_cores):
        super(InstanceGroup).__init__(app, name)
        self.app = app
        self.db = app['db']
        self.zone_monitor = app['zone_monitor']
        self.instance_monitor = app['inst_monitor']
        self.log_store = app['log_store']
        self.compute_client = app['compute_client']

        self.name = name
        self.type = typ
        self.cores = cores
        self.disk_size_gb = disk_size_gb
        self.local_ssd_data_disk = local_ssd_data_disk
        self.pd_ssd_data_disk_size_gb = pd_ssd_data_disk_size_gb
        self.pool_size = pool_size
        self.max_instances = max_instances
        self.standing_worker = standing_worker
        self.standing_cores = standing_cores

        # n * 16 cores / 15s = excess_scheduling_rate/s = 10/s => n ~= 10
        self.excess_scheduling_rate = 10 * (16 // self.cores)

        self.healthy_instances_by_free_cores = sortedcontainers.SortedSet(
            key=lambda instance: instance.free_cores_mcpu)

        self.n_instances = 0

        self.n_instances_by_state = {
            'pending': 0,
            'active': 0,
            'inactive': 0,
            'deleted': 0
        }

        # pending and active
        self.live_free_cores_mcpu = 0
        self.live_total_cores_mcpu = 0

    def config(self):
        return {
            'name': self.name,
            'type': self.type,
            'cores': self.cores,
            'disk_size_gb': self.disk_size_gb,
            'local_ssd_data_disk': self.local_ssd_data_disk,
            'pd_ssd_data_disk_size_gb': self.pd_ssd_data_disk_size_gb,
            'pool_size': self.pool_size,
            'max_instances': self.max_instances,
            'standing_worker': self.standing_worker,
            'standing_cores': self.standing_cores
        }

    async def get_instance(self, user, record):
        cores_mcpu = record['cores_mcpu']
        i = self.healthy_instances_by_free_cores.bisect_key_left(cores_mcpu)
        while i < len(self.healthy_instances_by_free_cores):
            instance = self.healthy_instances_by_free_cores[i]
            assert cores_mcpu <= instance.free_cores_mcpu
            if user != 'ci' or (user == 'ci' and instance.zone.startswith('us-central1')):
                return instance
            i += 1
        histogram = collections.defaultdict(int)
        for instance in self.healthy_instances_by_free_cores:
            histogram[instance.free_cores_mcpu] += 1
        log.info(f'schedule: no viable instances for {cores_mcpu}: {histogram}')
        return None

    def adjust_for_remove_instance(self, instance):
        self.n_instances_by_state[instance.state] -= 1
        self.n_instances -= 1

        if instance.state in ('pending', 'active'):
            self.live_free_cores_mcpu -= max(0, instance.free_cores_mcpu)
            self.live_total_cores_mcpu -= instance.cores_mcpu
        if instance in self.healthy_instances_by_free_cores:
            self.healthy_instances_by_free_cores.remove(instance)

    def adjust_for_add_instance(self, instance):
        self.n_instances_by_state[instance.state] += 1
        self.n_instances += 1

        if instance.state in ('pending', 'active'):
            self.live_free_cores_mcpu += max(0, instance.free_cores_mcpu)
            self.live_total_cores_mcpu += instance.cores_mcpu
        if (instance.state == 'active'
                and instance.failed_request_count <= 1):
            self.healthy_instances_by_free_cores.add(instance)

    async def create_instance(self, cores, max_idle_time_msecs=None):
        raise NotImplementedError

    async def control_loop(self):
        log.info(f'starting control loop for pool {self}')
        while True:
            try:
                ready_cores = await self.db.select_and_fetchone(
                    '''
SELECT CAST(COALESCE(SUM(ready_cores_mcpu), 0) AS SIGNED) AS ready_cores_mcpu
FROM user_pool_resources
WHERE pool = %s
LOCK IN SHARED MODE;
''',
                    (self.name,))
                ready_cores_mcpu = ready_cores['ready_cores_mcpu']

                free_cores_mcpu = sum([
                    worker.free_cores_mcpu
                    for worker in self.healthy_instances_by_free_cores
                ])
                free_cores = free_cores_mcpu / 1000

                log.info(f'pool {self} n_instances {self.n_instances} {self.n_instances_by_state}'
                         f' free_cores {free_cores} live_free_cores {self.live_free_cores_mcpu / 1000}'
                         f' ready_cores {ready_cores_mcpu / 1000}')

                if ready_cores_mcpu > 0 and free_cores < 500:
                    n_live_instances = self.n_instances_by_state['pending'] + self.n_instances_by_state['active']
                    instances_needed = (
                            (ready_cores_mcpu - self.live_free_cores_mcpu + (self.cores * 1000) - 1)
                            // (self.cores * 1000))
                    instances_needed = min(instances_needed,
                                           self.pool_size - n_live_instances,
                                           self.max_instances - self.n_instances,
                                           # 20 queries/s; our GCE long-run quota
                                           300,
                                           self.excess_scheduling_rate)
                    if instances_needed > 0:
                        log.info(f'creating {instances_needed} new instances')
                        # parallelism will be bounded by thread pool
                        await asyncio.gather(*[self.create_instance(self.cores) for _ in range(instances_needed)])

                n_live_instances = self.n_instances_by_state['pending'] + self.n_instances_by_state['active']

                if (ENABLE_STANDING_WORKER and
                        self.standing_worker and
                        n_live_instances == 0 and
                        self.max_instances > 0):
                    await self.create_instance(cores=self.standing_cores,
                                               max_idle_time_msecs=STANDING_WORKER_MAX_IDLE_TIME_MSECS)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:
                log.exception('in control loop')
            await asyncio.sleep(15)


class JobPrivateInstanceGroup(InstanceGroup):
    def __init__(self, app, name):
        super(InstanceGroup).__init__(app, name)

        self.db = self.app['db']
        self.inst_monitor = self.app['inst_monitor']
        self.n_instances = 0

    async def get_instance(self, user, record):
        record = await self.db.select_and_fetchone(f'''
SELECT instance FROM instance_private_jobs
WHERE batch_id = %s AND job_id = %s AND NOW < time_created + 3 mins
ORDER BY time_created DESC
LIMIT 1
LOCK IN SHARE MODE;
''',
                                                   (record['batch_id'], record['job_id']))

        if record:
            instance = self.inst_monitor.name_instance[record['instance']]
            return instance
        return None

    def adjust_for_remove_instance(self, instance):
        self.n_instances -= 1

    def adjust_for_add_instance(self, instance):
        self.n_instances += 1

    async def create_instance(self, batch_id, job_id):
        instance = Instance.create_instance(...)

        await self.db.insert(f'''
INSERT INTO instance_private_jobs (instance, batch_id, job_id)
  VALUES (%s, %s, %s);
''',
                             (instance.name, batch_id, job_id))
