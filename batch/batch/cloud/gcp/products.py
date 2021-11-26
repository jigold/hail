import abc
from typing import Dict, Any

from ...products import (Product, DiskProductMixin, ComputeProductMixin, MemoryProductMixin, IPFeeProductMixin, ServiceFeeProductMixin,
                         ExternalDiskProductMixin, QuantifiedResource)


class GCPProduct(Product, abc.ABC):
    pass


class GCPDiskProduct(DiskProductMixin, GCPProduct):
    FORMAT_VERSION = 1
    TYPE = 'gcp_disk'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'GCPDiskProduct':
        assert data['type'] == GCPDiskProduct.TYPE
        return GCPDiskProduct(data['name'], data['storage_in_gib'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], disk_type: str, storage_in_gib: int):
        prefix = f'disk/{disk_type}'
        name = GCPDiskProduct.latest_product_name(latest_versions, prefix)
        return GCPDiskProduct(name, storage_in_gib)

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


class GCPExternalDiskProduct(ExternalDiskProductMixin, GCPProduct):
    FORMAT_VERSION = 1
    TYPE = 'gcp_external_disk'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'GCPExternalDiskProduct':
        assert data['type'] == GCPExternalDiskProduct.TYPE
        return GCPExternalDiskProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], disk_type: str):
        prefix = f'disk/{disk_type}'
        name = GCPExternalDiskProduct.latest_product_name(latest_versions, prefix)
        return GCPExternalDiskProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del cpu_in_mcpu, memory_in_bytes, worker_fraction_in_1024ths
        return {'name': self.name, 'quantity': external_storage_in_gib * 1024}  # storage is in units of MiB

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'version': self.FORMAT_VERSION
        }


class GCPComputeProduct(ComputeProductMixin, GCPProduct):
    FORMAT_VERSION = 1
    TYPE = 'gcp_compute'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'GCPComputeProduct':
        assert data['type'] == GCPComputeProduct.TYPE
        return GCPComputeProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], instance_family: str, preemptible: bool):
        preemptible_str = 'preemptible' if preemptible else 'nonpreemptible'
        prefix = f'compute/{instance_family}-{preemptible_str}'
        name = GCPComputeProduct.latest_product_name(latest_versions, prefix)
        return GCPComputeProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class GCPMemoryProduct(MemoryProductMixin, GCPProduct):
    FORMAT_VERSION = 1
    TYPE = 'gcp_memory'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'GCPMemoryProduct':
        assert data['type'] == GCPMemoryProduct.TYPE
        return GCPMemoryProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], instance_family: str, preemptible: bool):
        preemptible_str = 'preemptible' if preemptible else 'nonpreemptible'
        prefix = f'memory/{instance_family}-{preemptible_str}'
        name = GCPMemoryProduct.latest_product_name(latest_versions, prefix)
        return GCPMemoryProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class GCPServiceFeeProduct(ServiceFeeProductMixin, GCPProduct):
    FORMAT_VERSION = 1
    TYPE = 'gcp_service_fee'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'GCPServiceFeeProduct':
        assert data['type'] == GCPServiceFeeProduct.TYPE
        return GCPServiceFeeProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str]):
        prefix = 'service-fee'
        name = GCPServiceFeeProduct.latest_product_name(latest_versions, prefix)
        return GCPServiceFeeProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


class GCPIPFeeProduct(IPFeeProductMixin, GCPProduct):
    FORMAT_VERSION = 1
    TYPE = 'gcp_ip_fee'

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'GCPIPFeeProduct':
        assert data['type'] == GCPIPFeeProduct.TYPE
        return GCPIPFeeProduct(data['name'])

    @staticmethod
    def new_product(latest_versions: Dict[str, str], base: int):
        prefix = f'ip-fee/{base}'
        name = GCPIPFeeProduct.latest_product_name(latest_versions, prefix)
        return GCPIPFeeProduct(name)

    def __init__(self, name: str):
        self.name = name

    def to_dict(self):
        return {
            'type': self.TYPE,
            'name': self.name,
            'format_version': self.FORMAT_VERSION
        }


def gcp_product_from_dict(data: dict) -> GCPProduct:
    typ = data['type']
    if typ == GCPDiskProduct.TYPE:
        return GCPDiskProduct.from_dict(data)
    if typ == GCPExternalDiskProduct.TYPE:
        return GCPExternalDiskProduct.from_dict(data)
    if typ == GCPComputeProduct.TYPE:
        return GCPComputeProduct.from_dict(data)
    if typ == GCPMemoryProduct.TYPE:
        return GCPMemoryProduct.from_dict(data)
    if typ == GCPServiceFeeProduct.TYPE:
        return GCPServiceFeeProduct.from_dict(data)
    assert typ == GCPIPFeeProduct.TYPE
    return GCPIPFeeProduct.from_dict(data)
