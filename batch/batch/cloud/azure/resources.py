import abc
import re
from typing import Dict, Any

from ...driver.resource_manager import ResourceVersions, resource_version_to_name
from ...resources import (QuantifiedResource, Resource, DiskResourceMixin, VMResourceMixin, IPFeeResourceMixin,
                          ServiceFeeResourceMixin, ExternalDiskResourceMixin)
from .resource_utils import azure_disk_from_storage_in_gib, valid_azure_disk_names


class AzureResource(Resource, abc.ABC):
    pass


class AzureDiskResource(DiskResourceMixin, AzureResource):
    FORMAT_VERSION = 1
    TYPE = 'azure_disk'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureDiskResource':
        assert data['type'] == AzureDiskResource.TYPE
        return AzureDiskResource(data['name'], data['storage_in_gib'])

    @staticmethod
    def new_resource(resource_versions: ResourceVersions, disk_type: str, storage_in_gib: int, location: str):
        # Azure bills for specific disk sizes so we must round the storage_in_gib to the nearest power of two
        disk = azure_disk_from_storage_in_gib(disk_type, storage_in_gib)
        assert disk, f'disk_type={disk_type} storage_in_gib={storage_in_gib}'
        name = resource_versions.latest_resource_name(f'az/disk/{disk.name}/{location}')
        return AzureDiskResource(name, storage_in_gib)

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


class AzureExternalDiskResource(ExternalDiskResourceMixin, AzureResource):
    FORMAT_VERSION = 1
    TYPE = 'azure_external_disk'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureExternalDiskResource':
        assert data['type'] == AzureExternalDiskResource.TYPE
        return AzureExternalDiskResource(data['disk_type'], data['location'], data['latest_disk_versions'])

    @staticmethod
    def new_resource(resource_versions: ResourceVersions, disk_type: str, location: str):
        def is_disk_product(product_name):
            match = re.fullmatch(rf'az/disk/(?P<name>[^/]+)/{location}', product_name)
            if match is None:
                return False
            return match.groupdict()['name'] in valid_azure_disk_names

        latest_disk_versions = {product_name: version
                                for product_name, version in resource_versions.to_dict().items()
                                if is_disk_product(product_name)}

        return AzureExternalDiskResource(disk_type, location, latest_disk_versions)

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
        name = resource_version_to_name(prefix, version)
        return {'name': name, 'quantity': disk.size_in_gib * 1024}  # storage is in units of MiB

    def to_dict(self):
        return {
            'type': self.TYPE,
            'disk_type': self.disk_type,
            'location': self.location,
            'latest_disk_versions': self.latest_disk_versions,
            'version': self.FORMAT_VERSION
        }


class AzureVMResource(VMResourceMixin, AzureResource):
    FORMAT_VERSION = 1
    TYPE = 'azure_vm'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureVMResource':
        assert data['type'] == AzureVMResource.TYPE
        return AzureVMResource(data['name'])

    @staticmethod
    def new_resource(resource_versions: ResourceVersions, machine_type: str, preemptible: bool, location: str):
        preemptible_str = 'spot' if preemptible else 'regular'
        name = resource_versions.latest_resource_name(f'az/vm/{machine_type}/{preemptible_str}/{location}')
        return AzureVMResource(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class AzureServiceFeeResource(ServiceFeeResourceMixin, AzureResource):
    FORMAT_VERSION = 1
    TYPE = 'azure_service_fee'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureServiceFeeResource':
        assert data['type'] == AzureServiceFeeResource.TYPE
        return AzureServiceFeeResource(data['name'])

    @staticmethod
    def new_resource(resource_versions: ResourceVersions):
        name = resource_versions.latest_resource_name('az/service-fee')
        return AzureServiceFeeResource(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class AzureIPFeeResource(IPFeeResourceMixin, AzureResource):
    FORMAT_VERSION = 1
    TYPE = 'azure_ip_fee'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'AzureIPFeeResource':
        assert data['type'] == AzureIPFeeResource.TYPE
        return AzureIPFeeResource(data['name'])

    @staticmethod
    def new_resource(resource_versions: ResourceVersions, base: int):
        name = resource_versions.latest_resource_name(f'az/ip-fee/{base}')
        return AzureIPFeeResource(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


def azure_resource_from_dict(data: dict) -> AzureResource:
    typ = data['type']
    if typ == AzureDiskResource.TYPE:
        return AzureDiskResource.from_dict(data)
    if typ == AzureExternalDiskResource.TYPE:
        return AzureExternalDiskResource.from_dict(data)
    if typ == AzureVMResource.TYPE:
        return AzureVMResource.from_dict(data)
    if typ == AzureServiceFeeResource.TYPE:
        return AzureServiceFeeResource.from_dict(data)
    assert typ == AzureIPFeeResource.TYPE
    return AzureIPFeeResource.from_dict(data)
