import abc
from typing import Dict, Optional
import logging

from gear import Database


log = logging.getLogger('resource_manager')


def resource_version_to_name(prefix: str, version: str) -> str:
    return f'{prefix}/{version}'


class ResourceVersions:
    def __init__(self, data: Optional[Dict[str, str]] = None):
        if data is None:
            data = {}
        self._resource_versions = data

    def latest_version(self, prefix: str) -> str:
        return self._resource_versions[prefix]

    def latest_resource_name(self, prefix: str) -> str:
        version = self.latest_version(prefix)
        return resource_version_to_name(prefix, version)

    def update(self, data: Dict[str, str]):
        self._resource_versions = data

    def to_dict(self) -> Dict[str, str]:
        return self._resource_versions


class CloudResourceManager(abc.ABC):
    db: Database
    resource_versions: ResourceVersions

    async def refresh_resource_versions(self):
        latest_versions = await refresh_latest_resource_versions(self.db)
        self.resource_versions.update(latest_versions)


async def refresh_latest_resource_versions(db: Database) -> Dict[str, str]:
    records = db.execute_and_fetchall('SELECT prefix, version FROM latest_resource_versions')
    log.info('refreshed resource versions')
    return {record['prefix']: record['version'] async for record in records}
