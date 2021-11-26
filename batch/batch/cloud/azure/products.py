import abc
import re
from typing import Dict, Any

from ...products import (QuantifiedResource, Product, DiskProductMixin, VMProductMixin, IPFeeProductMixin,
                         ServiceFeeProductMixin, ExternalDiskProductMixin)
from .resource_utils import azure_disk_from_storage_in_gib, valid_azure_disk_names


class AzureProduct(Product, abc.ABC):
    pass


class AzureDiskProduct(DiskProductMixin, AzureProduct):
    FORMAT_VERSION = 1
    TYPE = 'azure_disk'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureDiskProduct':
        assert data['type'] == AzureDiskProduct.TYPE
        return AzureDiskProduct(data['name'], data['storage_in_gib'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], disk_type: str, storage_in_gib: int, location: str):
        # Azure bills for specific disk sizes so we must round the storage_in_gib to the nearest power of two
        disk = azure_disk_from_storage_in_gib(disk_type, storage_in_gib)
        assert disk, f'disk_type={disk_type} storage_in_gib={storage_in_gib}'
        prefix = f'az/disk/{disk.name}/{location}'
        name = AzureDiskProduct.latest_product_name(latest_versions, prefix)
        return AzureDiskProduct(name, storage_in_gib)

    def __init__(self, name: str, storage_in_gib: int):
        self.name = name
        self.storage_in_gib = storage_in_gib

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'storage_in_gib': self.storage_in_gib,
            'version': self.FORMAT_VERSION
        }


class AzureExternalDiskProduct(ExternalDiskProductMixin, AzureProduct):
    FORMAT_VERSION = 1
    TYPE = 'azure_external_disk'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureExternalDiskProduct':
        assert data['type'] == AzureExternalDiskProduct.TYPE
        return AzureExternalDiskProduct(data['disk_type'], data['location'], data['latest_disk_versions'])

    @staticmethod
    def new_product(latest_product_versions: Dict[str, str], disk_type: str, location: str):
        def is_disk_product(product_name):
            match = re.fullmatch(rf'az/disk/(?P<name>[^/]+)/{location}', product_name)
            if match is None:
                return False
            return match.groupdict()['name'] in valid_azure_disk_names

        latest_disk_versions = {product_name: version
                                for product_name, version in latest_product_versions.items()
                                if is_disk_product(product_name)}

        return AzureExternalDiskProduct(disk_type, location, latest_disk_versions)

    def __init__(self, disk_type: str, location: str, latest_disk_versions: Dict[str, str]):
        self.disk_type = disk_type
        self.location = location
        self.latest_disk_versions = latest_disk_versions

    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del cpu_in_mcpu, memory_in_bytes, worker_fraction_in_1024ths

        # Azure bills for specific disk sizes so we must round the storage_in_gib to the nearest power of two
        disk = azure_disk_from_storage_in_gib(self.disk_type, external_storage_in_gib)
        assert disk, f'disk_type={self.disk_type} storage_in_gib={external_storage_in_gib}'
        prefix = f'az/disk/{disk.name}/{self.location}'
        version = self.latest_disk_versions[prefix]
        return {'name': f'{prefix}/{version}', 'quantity': disk.size_in_gib * 1024}  # storage is in units of MiB

    def to_dict(self):
        return {
            'type': self.TYPE,
            'disk_type': self.disk_type,
            'location': self.location,
            'latest_disk_versions': self.latest_disk_versions,
            'version': self.FORMAT_VERSION
        }


class AzureVMProduct(VMProductMixin, AzureProduct):
    FORMAT_VERSION = 1
    TYPE = 'azure_vm'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureVMProduct':
        assert data['type'] == AzureVMProduct.TYPE
        return AzureVMProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], machine_type: str, preemptible: bool, location: str):
        preemptible_str = 'spot' if preemptible else 'regular'
        prefix = f'az/vm/{machine_type}/{preemptible_str}/{location}'
        name = AzureVMProduct.latest_product_name(latest_versions, prefix)
        return AzureVMProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class AzureServiceFeeProduct(ServiceFeeProductMixin, AzureProduct):
    FORMAT_VERSION = 1
    TYPE = 'azure_service_fee'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureServiceFeeProduct':
        assert data['type'] == AzureServiceFeeProduct.TYPE
        return AzureServiceFeeProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str]):
        prefix = 'az/service-fee'
        name = AzureServiceFeeProduct.latest_product_name(latest_versions, prefix)
        return AzureServiceFeeProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class AzureIPFeeProduct(IPFeeProductMixin, AzureProduct):
    FORMAT_VERSION = 1
    TYPE = 'azure_ip_fee'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureIPFeeProduct':
        assert data['type'] == AzureIPFeeProduct.TYPE
        return AzureIPFeeProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], base: int):
        prefix = f'az/ip-fee/{base}'
        name = AzureIPFeeProduct.latest_product_name(latest_versions, prefix)
        return AzureIPFeeProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


def azure_product_from_dict(data: dict) -> AzureProduct:
    typ = data['type']
    if typ == AzureDiskProduct.TYPE:
        return AzureDiskProduct.from_dict(data)
    if typ == AzureExternalDiskProduct.TYPE:
        return AzureExternalDiskProduct.from_dict(data)
    if typ == AzureVMProduct.TYPE:
        return AzureVMProduct.from_dict(data)
    if typ == AzureServiceFeeProduct.TYPE:
        return AzureServiceFeeProduct.from_dict(data)
    assert typ == AzureIPFeeProduct.TYPE
    return AzureIPFeeProduct.from_dict(data)
