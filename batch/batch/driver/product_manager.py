import abc
from typing import Dict
import logging

from gear import Database


log = logging.getLogger('product_manager')


class CloudProductManager(abc.ABC):
    db: Database
    _latest_product_versions: Dict[str, str]

    def latest_product_version(self, product: str) -> str:
        return self._latest_product_versions[product]

    def latest_product_versions(self) -> Dict[str, str]:
        return self._latest_product_versions

    async def refresh_latest_product_versions(self):
        records = self.db.execute_and_fetchall('SELECT product_name, version FROM latest_product_versions')
        self._latest_product_versions = {record['product_name']: record['version'] async for record in records}
        log.info('refreshed product versions')
