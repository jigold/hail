import os
import asyncio
from gear import Database


async def main():
    if os.environ['HAIL_SCOPE'] != 'dev':
        return

    worker_cores = 16
    max_instances = 8
    max_live_instances = 8

    db = Database()
    await db.async_init()

    await db.execute_update(
        '''
UPDATE pools
SET worker_cores = %s
WHERE name = %s;
''', (worker_cores, "standard"))

    await db.execute_update(
        '''
UPDATE pools
SET enable_standing_worker = 0;
''')

    await db.execute_update(
        '''
UPDATE inst_colls
SET max_instances = %s, max_live_instances = %s
WHERE name = %s;
''', (max_instances, max_live_instances, "standard"))

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
