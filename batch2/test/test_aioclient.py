import os
import asyncio
import aiohttp
import unittest
from hailtop.batch_client.aioclient import BatchClient


class Test(unittest.TestCase):
    def setUp(self):
        session = aiohttp.ClientSession(
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=60))
        self.client = BatchClient(session, url=os.environ.get('BATCH_URL'))

    def tearDown(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.client.close())

    def test_job(self):
        async def f():
            b = self.client.create_batch()
            j = b.create_job('alpine', ['echo', 'test'])
            await b.submit()
            status = await j.wait()
            self.assertTrue('attributes' not in status, (status, await j.log()))
            self.assertEqual(status['state'], 'Success', (status, await j.log()))
            self.assertEqual(status['exit_code']['main'], 0, (status, await j.log()))

            self.assertEqual((await j.log())['main'], 'test\n')

            self.assertTrue(await j.is_complete())

        loop = asyncio.get_event_loop()
        loop.run_until_complete(f())
