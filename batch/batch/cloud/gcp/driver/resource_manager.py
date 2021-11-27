from typing import Dict

import logging

from gear import Database

from ....driver.resource_manager import CloudResourceManager, refresh_latest_resource_versions

log = logging.getLogger('resource_manager')


class GCPResourceManager(CloudResourceManager):
    @staticmethod
    async def create(db: Database):
        pm = GCPResourceManager(db)
        await pm.refresh_latest_resource_versions()
        return pm

    def __init__(self, db: Database):
        self.db = db
        self._latest_resource_versions: Dict[str, str] = {}

    def latest_resource_versions(self) -> Dict[str, str]:
        return self._latest_resource_versions

    def latest_resource_version(self, prefix: str) -> str:
        return self._latest_resource_versions[prefix]

    async def refresh_latest_resource_versions(self):
        self._latest_resource_versions = await refresh_latest_resource_versions(self.db)
