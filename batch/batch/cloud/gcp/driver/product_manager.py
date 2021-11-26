import logging

from gear import Database

from ....driver.product_manager import CloudProductManager

log = logging.getLogger('product_manager')


class GCPProductManager(CloudProductManager):
    @staticmethod
    async def create(db: Database):
        pm = GCPProductManager(db)
        await pm.refresh_latest_product_versions()
        return pm

    def __init__(self, db: Database):
        self.db = db
        self._latest_product_versions = {}
