import abc
from typing import Dict
import logging

from gear import Database


log = logging.getLogger('resource_manager')


class CloudResourceManager(abc.ABC):
    @abc.abstractmethod
    def latest_resource_versions(self) -> Dict[str, str]:
        raise NotImplementedError


async def refresh_latest_resource_versions(db: Database) -> Dict[str, str]:
    records = db.execute_and_fetchall('SELECT prefix, version FROM latest_resource_versions')
    log.info('refreshed resource versions')
    return {record['prefix']: record['version'] async for record in records}
