from typing import Dict, List, Sequence
import abc

from .products import QuantifiedResource, Product
from .cloud.resource_utils import cores_mcpu_to_memory_bytes


def is_power_two(n):
    return (n & (n - 1) == 0) and n != 0


class InstanceConfig(abc.ABC):
    cloud: str
    cores: int
    job_private: bool
    products: Sequence[Product]

    @staticmethod
    @abc.abstractmethod
    def create(latest_product_versions: Dict[str, str],
               machine_type: str,
               preemptible: bool,
               local_ssd_data_disk: bool,
               data_disk_size_gb: int,
               boot_disk_size_gb: int,
               job_private: bool,
               location: str) -> 'InstanceConfig':
        raise NotImplementedError

    @abc.abstractmethod
    def worker_type(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def to_dict(self) -> dict:
        raise NotImplementedError

    def resources(self,
                  cpu_in_mcpu: int,
                  memory_in_bytes: int,
                  extra_storage_in_gib: int,
                  ) -> List[QuantifiedResource]:
        assert memory_in_bytes % (1024 * 1024) == 0, memory_in_bytes
        assert isinstance(extra_storage_in_gib, int), extra_storage_in_gib
        assert is_power_two(self.cores) and self.cores <= 256, self.cores

        worker_fraction_in_1024ths = 1024 * cpu_in_mcpu // (self.cores * 1000)

        quantified_resources = []
        for product in self.products:
            quantified_resource = product.to_quantified_resource(cpu_in_mcpu=cpu_in_mcpu,
                                                                 memory_in_bytes=memory_in_bytes,
                                                                 worker_fraction_in_1024ths=worker_fraction_in_1024ths,
                                                                 external_storage_in_gib=extra_storage_in_gib)
            quantified_resources.append(quantified_resource)
        return quantified_resources

    def is_valid_configuration(self, valid_resources):
        is_valid = True
        dummy_resources = self.resources(0, 0, 0)
        for resource in dummy_resources:
            is_valid &= resource['name'] in valid_resources
        return is_valid

    @staticmethod
    def _cost_per_hour_from_resources(resource_rates: Dict[str, float],
                                      resources: List[QuantifiedResource]
                                      ) -> float:
        cost_per_msec = 0.0
        for r in resources:
            name = r['name']
            quantity = r['quantity']
            rate_unit_msec = resource_rates[name]
            cost_per_msec += quantity * rate_unit_msec
        return cost_per_msec * 1000 * 60 * 60

    def cost_per_hour(self,
                      resource_rates: Dict[str, float],
                      cpu_in_mcpu: int,
                      memory_in_bytes: int,
                      storage_in_gb: int,
                      ) -> float:
        resources = self.resources(cpu_in_mcpu, memory_in_bytes, storage_in_gb)
        return InstanceConfig._cost_per_hour_from_resources(resource_rates, resources)

    def cost_per_hour_from_cores(self,
                                 resource_rates: Dict[str, float],
                                 utilized_cores_mcpu: int,
                                 ) -> float:
        assert 0 <= utilized_cores_mcpu <= self.cores * 1000
        memory_in_bytes = cores_mcpu_to_memory_bytes(self.cloud, utilized_cores_mcpu, self.worker_type())
        storage_in_gb = 0   # we don't need to account for external storage
        return self.cost_per_hour(resource_rates, utilized_cores_mcpu, memory_in_bytes, storage_in_gb)

    def actual_cost_per_hour(self, resource_rates: Dict[str, float]) -> float:
        cpu_in_mcpu = self.cores * 1000
        memory_in_bytes = cores_mcpu_to_memory_bytes(self.cloud, cpu_in_mcpu, self.worker_type())
        storage_in_gb = 0   # we don't need to account for external storage
        resources = self.resources(cpu_in_mcpu, memory_in_bytes, storage_in_gb)
        resources = [r for r in resources if 'service-fee' not in r['name']]
        return InstanceConfig._cost_per_hour_from_resources(resource_rates, resources)
