import os
import asyncio
import pytest
import aiohttp
import unittest
from hailtop.batch_client.aioclient import BatchClient


#
# def async_to_blocking(coro):
#     return asyncio.get_event_loop().run_until_complete(coro)

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="module")
async def batch_client():
    bc = await BatchClient()
    yield bc
    await bc.close()


async def test_job(batch_client):
    j = batch_client.create_job('alpine', ['echo', 'test'])
    await batch_client.submit()
    status = await j.wait()
    assert 'attributes' not in status, (status, await j.log())
    assert status['state'] == 'Success', (status, await j.log())
    assert status['exit_code']['main'] == 0, (status, await j.log())

    assert await j.log()['main'] == 'test\n'

    assert await j.is_complete()


# class Test(unittest.TestCase):
#     async def setUp(self):
#         self.client = await BatchClient()
#
#     async def tearDown(self):
#         await self.client.close()
#         # loop = asyncio.get_event_loop()
#         # loop.run_until_complete(self.client.close())
#
#     async def test_job(self):
#         b = self.client.create_batch()
#         j = b.create_job('alpine', ['echo', 'test'])
#         await b.submit()
#         status = await j.wait()
#         self.assertTrue('attributes' not in status, (status, await j.log()))
#         self.assertEqual(status['state'], 'Success', (status, await j.log()))
#         self.assertEqual(status['exit_code']['main'], 0, (status, await j.log()))
#
#         self.assertEqual((await j.log())['main'], 'test\n')
#
#         self.assertTrue(await j.is_complete())
#
#         # async_to_blocking(f())
