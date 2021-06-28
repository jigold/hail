
from hailtop.utils import LoggingTimer
from hailtop.hail_logging import configure_logging

configure_logging()

import logging

log = logging.getLogger("foo")

import sys

sys.path.insert(0, '/Users/jigold/projects/hail/hail/python/hailtop/')

from hailtop.aiogoogle.client.compute_client import ComputeClient


class Disk:
    def __init__(self, compute_client):
        self.compute_client = compute_client

    async def attach(self):
        async with LoggingTimer(f'attaching disk'):
            await self.compute_client.attach_disk('/zones/us-central1-a/instances/foo/attachDisk')

    async def create(self):
        async with LoggingTimer(f'attaching disk'):
            labels = {}

            config = {
                'name': 'foo',
                'sizeGb': f'0',
                'type': f'zones/us-central1-a/diskTypes/pd-ssd',
                'labels': labels,
            }

            await self.compute_client.create_disk(f'/zones/us-central1-a/disks', json=config)

async def main():
    client = ComputeClient('hail-vdc', raise_for_status=False)
    disk = Disk(client)
    try:
        await disk.create()
    finally:
        await client.close()

import asyncio

asyncio.run(main())