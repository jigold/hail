import pytest
from hailtop.batch_client.aioclient import BatchClient

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def client():
    bc = await BatchClient()
    yield bc
    await bc.close()


async def test_job(client):
    j = client.create_job('alpine', ['echo', 'test'])
    await client.submit()
    status = await j.wait()
    assert 'attributes' not in status, (status, await j.log())
    assert status['state'] == 'Success', (status, await j.log())
    assert status['exit_code']['main'] == 0, (status, await j.log())
    assert await j.log()['main'] == 'test\n'
    assert await j.is_complete()
