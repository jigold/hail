import abc
from typing import Any, Dict
from typing_extensions import TypedDict


class QuantifiedResource(TypedDict):
    name: str
    quantity: int


class Resource(abc.ABC):
    name: str

    @staticmethod
    def latest_resource_name(latest_product_versions: Dict[str, str], prefix: str):
        version = latest_product_versions[prefix]
        return f'{prefix}/{version}'

    @property
    def prefix(self):
        return self.name.rsplit('/', maxsplit=1)[0]

    @property
    def product_version(self):
        return self.name.rsplit('/', maxsplit=1)[1]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> 'Resource':
        raise NotImplementedError

    @abc.abstractmethod
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:
        raise NotImplementedError

    @abc.abstractmethod
    def to_dict(self):
        raise NotImplementedError


class DiskResourceMixin(Resource, abc.ABC):
    storage_in_gib: int

    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del cpu_in_mcpu, memory_in_bytes, external_storage_in_gib
        # the factors of 1024 cancel between GiB -> MiB and fraction_1024 -> fraction
        return {'name': self.name, 'quantity': self.storage_in_gib * worker_fraction_in_1024ths}


class ExternalDiskResourceMixin(Resource, abc.ABC):
    @abc.abstractmethod
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        raise NotImplementedError


class ComputeResourceMixin(Resource, abc.ABC):
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del memory_in_bytes, worker_fraction_in_1024ths, external_storage_in_gib
        return {'name': self.name, 'quantity': cpu_in_mcpu}


class VMResourceMixin(Resource, abc.ABC):
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del cpu_in_mcpu, memory_in_bytes, external_storage_in_gib
        return {'name': self.name, 'quantity': worker_fraction_in_1024ths}


class MemoryResourceMixin(Resource, abc.ABC):
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del cpu_in_mcpu, worker_fraction_in_1024ths, external_storage_in_gib
        return {'name': self.name, 'quantity': memory_in_bytes // 1024 // 1024}


class IPFeeResourceMixin(Resource, abc.ABC):
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del cpu_in_mcpu, memory_in_bytes, external_storage_in_gib
        return {'name': self.name, 'quantity': worker_fraction_in_1024ths}


class ServiceFeeResourceMixin(Resource, abc.ABC):
    def to_quantified_resource(self,
                               cpu_in_mcpu: int,
                               memory_in_bytes: int,
                               worker_fraction_in_1024ths: int,
                               external_storage_in_gib: int) -> QuantifiedResource:  # pylint: disable=unused-argument
        del memory_in_bytes, worker_fraction_in_1024ths, external_storage_in_gib
        return {'name': self.name, 'quantity': cpu_in_mcpu}
