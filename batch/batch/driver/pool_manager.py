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
        #
        # self.worker_disk_size_gb = None
        # self.worker_local_ssd_data_disk = None
        # self.worker_pd_ssd_data_disk_size_gb = None
        #
        # self.standing_pool = None
        #
        # self.max_instances = None
        # self.pool_size = None
        #
        # self.pool_fairshare = collections.defaultdict(lambda: {'pool_size': 0,
        #                                                        'long_run_quota': 0,
        #                                                        'excess_scheduling_rate': 0})

        self.task_manager = aiotools.BackgroundTaskManager()

    @property
    def n_pending_instances(self):
        return sum([pool.n_instances_by_state['pending']
                    for pool in self.id_pool.values()])

    @property
    def n_active_instances(self):
        return sum([pool.n_instances_by_state['active']
                    for pool in self.id_pool.values()])

    @property
    def n_instances_by_state(self):
        state_counter = collections.Counter({
            'pending': 0,
            'active': 0,
            'inactive': 0,
            'deleted': 0
        })

        for pool in self.id_pool.values():
            state_counter += collections.Counter(pool.n_instances_by_state)

        return state_counter

    @property
    def n_pools(self):
        return len(self.id_pool)

    async def async_init(self):
        log.info('initializing pool manager')

#         row = await self.db.select_and_fetchone('''
# SELECT worker_disk_size_gb, worker_local_ssd_data_disk, worker_pd_ssd_data_disk_size_gb
# FROM globals;
# ''')
#
#         self.worker_disk_size_gb = row['worker_disk_size_gb']
#         self.worker_local_ssd_data_disk = row['worker_local_ssd_data_disk']
#         self.worker_pd_ssd_data_disk_size_gb = row['worker_pd_ssd_data_disk_size_gb']

        async for record in self.db.select_and_fetchall(
                'SELECT * FROM pools;'):
            pool = Pool.from_record(self.app, record)
            self.add_pool(pool)
            await pool.async_init()

    async def run(self):
        await asyncio.gather(*[pool.run() for pool in self.id_pool.values()])

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
