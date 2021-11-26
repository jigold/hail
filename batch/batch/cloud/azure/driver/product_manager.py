from typing import Dict
import logging

from gear import Database

from ....driver.product_manager import CloudProductManager

log = logging.getLogger('product_manager')


class AzureProductManager(CloudProductManager):
    @staticmethod
    async def create(db: Database):
        pm = AzureProductManager(db)
        await pm.refresh_latest_product_versions()
        return pm

    def __init__(self, db: Database):
        self.db = db
        self._latest_product_versions: Dict[str, str] = {}
