import asyncio
import logging
import collections

from hailtop import aiotools

from ..batch_configuration import ENABLE_STANDING_WORKER, STANDING_WORKER_MAX_IDLE_TIME_MSECS
from .pool import PoolConfig, Pool

log = logging.getLogger('pool_manager')


class PoolManager:
    def __init__(self, app):
        self.app = app
        self.db = app['db']
        self.compute_client = app['compute_client']

        self.id_pool = {}

        self.worker_disk_size_gb = None
        self.worker_local_ssd_data_disk = None
        self.worker_pd_ssd_data_disk_size_gb = None

        self.standing_pool = None

        self.max_instances = None
        self.pool_size = None

        self.pool_fairshare = collections.defaultdict(lambda: {'pool_size': 0,
                                                               'long_run_quota': 0,
                                                               'excess_scheduling_rate': 0})

        self.task_manager = aiotools.BackgroundTaskManager()

    @property
    def n_pending_instances(self):
        return sum([pool.n_instances_by_state['pending'] for _, pool in self.id_pool.items()])

    @property
    def n_active_instances(self):
        return sum([pool.n_instances_by_state['active'] for _, pool in self.id_pool.items()])

    @property
    def n_pools(self):
        return len(self.id_pool)

    async def async_init(self):
        log.info('initializing pool manager')

        row = await self.db.select_and_fetchone('''
SELECT standing_worker_family, standing_worker_type, standing_worker_cores,
  standing_worker_preemptible, worker_disk_size_gb,
  worker_local_ssd_data_disk, worker_pd_ssd_data_disk_size_gb,
  max_instances, pool_size
FROM globals;
''')

        standing_pool_config = PoolConfig(row['standing_worker_family'],
                                          row['standing_worker_type'],
                                          row['standing_worker_cores'],
                                          row['standing_worker_preemptible'],
                                          row['standing_worker_private'])
        self.standing_pool = standing_pool_config.find_optimal_pool(self.id_pool.values(), standing=True)
        assert self.standing_pool, f'No suitable pool found for {standing_pool_config}'

        self.worker_disk_size_gb = row['worker_disk_size_gb']
        self.worker_local_ssd_data_disk = row['worker_local_ssd_data_disk']
        self.worker_pd_ssd_data_disk_size_gb = row['worker_pd_ssd_data_disk_size_gb']

        self.max_instances = row['max_instances']
        self.pool_size = row['pool_size']

        async for record in self.db.select_and_fetchall(
                'SELECT * FROM pools;'):
            pool = Pool.from_record(self.app, record)
            self.add_pool(pool)
            await pool.async_init()

    async def run(self):
        await asyncio.gather(*[pool.run() for pool in self.id_pool.values()])
        self.task_manager.ensure_future(self.control_loop())

    async def create_pool(self):
        raise NotImplementedError

    def add_pool(self, pool):
        self.id_pool[pool.id] = pool

    def remove_pool(self, pool):
        pool.shutdown()
        del self.id_pool[pool.id]

    def shutdown(self):
        try:
            for _, pool in self.id_pool.items():
                try:
                    pool.shutdown()
                except Exception:
                    pass
        finally:
            self.task_manager.shutdown()

    async def compute_fairshare(self):
        # instances_needed = min(instances_needed,
        #                        pool_size - n_live_instances,
        #                        self.pool_manager.max_instances - self.n_instances,
        #                        # 20 queries/s; our GCE long-run quota
        #                        300,
        #                        # n * 16 cores / 15s = excess_scheduling_rate/s = 10/s => n ~= 10
        #                        10)
        pass

    async def control_loop(self):
        log.info(f'starting control loop for pool manager')
        while True:
            try:
                n_live_instances = self.n_pending_instances + self.n_active_instances
                if ENABLE_STANDING_WORKER and n_live_instances == 0 and self.max_instances > 0:
                    await self.standing_pool.create_instance(max_idle_time_msecs=STANDING_WORKER_MAX_IDLE_TIME_MSECS)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:
                log.exception('in control loop')
            await asyncio.sleep(15)
