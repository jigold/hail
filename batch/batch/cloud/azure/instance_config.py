from typing import List, Dict

from hailtop.utils import flatten

from ...instance_config import InstanceConfig
from .resource_utils import azure_machine_type_to_worker_type_and_cores
from .products import (AzureProduct, AzureVMProduct, AzureDiskProduct, AzureExternalDiskProduct,
                       AzureServiceFeeProduct, AzureIPFeeProduct, azure_product_from_dict)


AZURE_INSTANCE_CONFIG_VERSION = 2


class AzureSlimInstanceConfig(InstanceConfig):
    @staticmethod
    def create(latest_product_versions: Dict[str, str],
               machine_type: str,
               preemptible: bool,
               local_ssd_data_disk: bool,
               data_disk_size_gb: int,
               boot_disk_size_gb: int,
               job_private: bool,
               location: str) -> 'AzureSlimInstanceConfig':
        if local_ssd_data_disk:
            data_disk_product = None
        else:
            data_disk_product = AzureDiskProduct.new_product(latest_product_versions, 'P', data_disk_size_gb, location)

        products = flatten([
            AzureVMProduct.new_product(latest_product_versions, machine_type, preemptible, location),
            AzureDiskProduct.new_product(latest_product_versions, 'P', boot_disk_size_gb, location),
            data_disk_product,
            AzureExternalDiskProduct.new_product(latest_product_versions, 'P', location),
            AzureIPFeeProduct.new_product(latest_product_versions, 1024),
            AzureServiceFeeProduct.new_product(latest_product_versions),
        ])

        return AzureSlimInstanceConfig(
            machine_type=machine_type,
            preemptible=preemptible,
            local_ssd_data_disk=local_ssd_data_disk,
            data_disk_size_gb=data_disk_size_gb,
            boot_disk_size_gb=boot_disk_size_gb,
            job_private=job_private,
            products=products,
        )

    def __init__(self,
                 machine_type: str,
                 preemptible: bool,
                 local_ssd_data_disk: bool,
                 data_disk_size_gb: int,
                 boot_disk_size_gb: int,
                 job_private: bool,
                 products: List[AzureProduct]
                 ):
        self.cloud = 'azure'
        self._machine_type = machine_type
        self.preemptible = preemptible
        self.local_ssd_data_disk = local_ssd_data_disk
        self.data_disk_size_gb = data_disk_size_gb
        self.job_private = job_private
        self.boot_disk_size_gb = boot_disk_size_gb
        self.products = products

        worker_type, cores = azure_machine_type_to_worker_type_and_cores(self._machine_type)

        self._worker_type = worker_type
        self.cores = cores

    def worker_type(self) -> str:
        return self._worker_type

    @staticmethod
    def from_dict(data: dict) -> 'AzureSlimInstanceConfig':
        products = data.get('products')
        if products is None:
            assert data['version'] == 1, data['version']
            products = []
        products = [azure_product_from_dict(data) for data in products]

        return AzureSlimInstanceConfig(
            data['machine_type'],
            data['preemptible'],
            data['local_ssd_data_disk'],
            data['data_disk_size_gb'],
            data['boot_disk_size_gb'],
            data['job_private'],
            products,
        )

    def to_dict(self) -> dict:
        return {
            'version': AZURE_INSTANCE_CONFIG_VERSION,
            'cloud': 'azure',
            'machine_type': self._machine_type,
            'preemptible': self.preemptible,
            'local_ssd_data_disk': self.local_ssd_data_disk,
            'data_disk_size_gb': self.data_disk_size_gb,
            'boot_disk_size_gb': self.boot_disk_size_gb,
            'job_private': self.job_private,
            'products': [product.to_dict() for product in self.products]
        }
