import os
import asyncio
import pytest
import aiohttp
import unittest
from hailtop.batch_client.aioclient import BatchClient


#
# def async_to_blocking(coro):
#     return asyncio.get_event_loop().run_until_complete(coro)


class Test(unittest.TestCase):
    @pytest.mark.asyncio
    async def setUp(self):
        self.client = await BatchClient()

    @pytest.mark.asyncio
    async def tearDown(self):
        await self.client.close()
        # loop = asyncio.get_event_loop()
        # loop.run_until_complete(self.client.close())

    @pytest.mark.asyncio
    async def test_job(self):
        b = self.client.create_batch()
        j = b.create_job('alpine', ['echo', 'test'])
        await b.submit()
        status = await j.wait()
        self.assertTrue('attributes' not in status, (status, await j.log()))
        self.assertEqual(status['state'], 'Success', (status, await j.log()))
        self.assertEqual(status['exit_code']['main'], 0, (status, await j.log()))

        self.assertEqual((await j.log())['main'], 'test\n')

        self.assertTrue(await j.is_complete())

        # async_to_blocking(f())
