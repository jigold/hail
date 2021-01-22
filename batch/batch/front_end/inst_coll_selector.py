from gear import Database

from ..globals import MAX_PERSISTENT_SSD_SIZE_BYTES
from ..utils import (adjust_cores_for_memory_request, adjust_cores_for_packability,
                     adjust_cores_for_storage_request, get_cpu_cost_per_core_hour,
                     adjust_cores_for_worker_type, round_storage_bytes_to_gib)


class PoolConfig:
    @staticmethod
    def from_record(record):
        return PoolConfig(record['name'], record['worker_type'], record['worker_cores'],
                          record['boot_disk_size_gb'], record['worker_local_ssd_data_disk'],
                          record['worker_pd_ssd_data_disk_size_gb'])

    def __init__(self, name, worker_type, worker_cores, boot_disk_size_gb,
                 worker_local_ssd_data_disk, worker_pd_ssd_data_disk_size_gb):
        self.name = name
        self.worker_type = worker_type
        self.worker_cores = worker_cores
        self.worker_local_ssd_data_disk = worker_local_ssd_data_disk
        self.worker_pd_ssd_data_disk_size_gb = worker_pd_ssd_data_disk_size_gb
        self.boot_disk_size_gb = boot_disk_size_gb

    def resources_to_cores_mcpu(self, cores_mcpu, memory_bytes, storage_bytes):
        cores_mcpu = adjust_cores_for_memory_request(cores_mcpu, memory_bytes, self.worker_type)
        cores_mcpu = adjust_cores_for_storage_request(cores_mcpu, storage_bytes, self.worker_cores,
                                                      self.worker_local_ssd_data_disk, self.worker_pd_ssd_data_disk_size_gb)
        cores_mcpu = adjust_cores_for_packability(cores_mcpu)

        if cores_mcpu < self.worker_cores * 1000:
            return cores_mcpu
        return None

    def cpu_memory_cost_per_core_hour(self, cores_mcpu):
        # We are assuming the disk configurations are equal between workers
        cpu_cost_per_core_hour = get_cpu_cost_per_core_hour(self.worker_type, preemptible=True)
        return cpu_cost_per_core_hour * (cores_mcpu / 1000 / self.worker_cores)


class PoolSelector:
    def __init__(self, app):
        self.app = app
        self.db: Database = app['db']
        self.pool_configs = {}

    async def async_init(self):
        records = self.db.execute_and_fetchall('''
SELECT pools.name, worker_type, worker_cores, boot_disk_size_gb,
  worker_local_ssd_data_disk, worker_pd_ssd_data_disk_size_gb
FROM pools
LEFT JOIN inst_colls ON pools.name = inst_colls.name;
''')
        async for record in records:
            self.pool_configs[record['name']] = PoolConfig.from_record(record)

    def select_pool(self, cores_mcpu, memory_bytes, storage_bytes):
        optimal_pool_name = None
        optimal_cores_mcpu = None
        min_cost = None
        for pool in self.pool_configs.values():
            cores_mcpu = pool.resources_to_cores_mcpu(cores_mcpu, memory_bytes, storage_bytes)
            if cores_mcpu is not None:
                cost = pool.cpu_memory_cost_per_core_hour(cores_mcpu)
                if min_cost is None or cost < min_cost:
                    min_cost = cost
                    optimal_cores_mcpu = cores_mcpu
                    optimal_pool_name = pool.name

        if optimal_pool_name is not None:
            return (optimal_pool_name, optimal_cores_mcpu)

        return None


class JobPrivateInstanceSelector:
    @staticmethod
    def get_machine_type(cores_mcpu, memory_bytes, storage_bytes, preemptible):
        if storage_bytes > MAX_PERSISTENT_SSD_SIZE_BYTES:
            return None

        storage_gib = round_storage_bytes_to_gib(storage_bytes)

        optimal_machine_type = None
        optimal_cores_mcpu = None
        min_cost = None
        for worker_type in ('standard', 'highmem', 'highcpu'):
            cores_mcpu = adjust_cores_for_memory_request(cores_mcpu, memory_bytes, worker_type)
            cores_mcpu = adjust_cores_for_packability(cores_mcpu)
            cores_mcpu = adjust_cores_for_worker_type(cores_mcpu, worker_type)
            cores = cores_mcpu // 1000
            if cores_mcpu is not None:
                cpu_cost_per_core_hour = get_cpu_cost_per_core_hour(worker_type, preemptible)
                cost = cpu_cost_per_core_hour * cores
                if min_cost is None or cost < min_cost:
                    min_cost = cost
                    optimal_machine_type = f'n1-{worker_type}-{cores}'
                    optimal_cores_mcpu = cores

        if optimal_machine_type:
            return ('job-private', optimal_machine_type, optimal_cores_mcpu, storage_gib)

        return None
