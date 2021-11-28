import logging

from gear import Database

from ....driver.resource_manager import CloudResourceManager, ResourceVersions

log = logging.getLogger('resource_manager')


class GCPResourceManager(CloudResourceManager):
    @staticmethod
    async def create(db: Database):
        pm = GCPResourceManager(db)
        await pm.refresh_resource_versions()
        return pm

    def __init__(self, db: Database):
        self.db = db
        self.resource_versions = ResourceVersions()
