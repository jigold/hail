import logging

from gear import Database

from ....driver.resource_manager import CloudResourceManager, ResourceVersions

log = logging.getLogger('resource_manager')


class AzureResourceManager(CloudResourceManager):
    @staticmethod
    async def create(db: Database):
        pm = AzureResourceManager(db)
        await pm.refresh_resource_versions()
        return pm

    def __init__(self, db: Database):
        self.db = db
        self.resource_versions = ResourceVersions()
