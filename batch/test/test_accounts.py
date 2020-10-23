import aiohttp
import base64
import os
import pytest
import secrets
from hailtop.batch_client.aioclient import BatchClient

pytestmark = pytest.mark.asyncio


@pytest.fixture
async def make_client():
    _bcs = []
    async def factory(project=None):
        bc = await BatchClient(project, token_file=os.environ['HAIL_TEST_TOKEN_FILE'])
        _bcs.append(bc)
        return bc

    yield factory
    for bc in _bcs:
        await bc.close()


@pytest.fixture
async def dev_client():
    bc = await BatchClient(None, token_file=os.environ['HAIL_TEST_DEV_TOKEN_FILE'])
    yield bc
    await bc.close()


@pytest.fixture(scope='module')
def get_billing_project_name():
    prefix = f'__testproject_{secrets.token_urlsafe(15)}'
    projects = []
    def get_name():
        name = f'{prefix}_{len(projects)}'
        projects.append(name)
        return name
    return get_name


@pytest.fixture
async def new_billing_project(dev_client, get_billing_project_name):
    project = get_billing_project_name()
    yield await dev_client.create_billing_project(project)

    try:
        r = await dev_client.get_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert r['status'] != 'deleted', r
        if r['status'] == 'open':
            await dev_client.close_billing_project(project)
        if r['status'] != 'deleted':
            await dev_client.delete_billing_project(project)


async def test_bad_token():
    token = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('ascii')
    bc = await BatchClient('test', _token=token)
    try:
        b = bc.create_batch()
        j = b.create_job('ubuntu:18.04', ['false'])
        await b.submit()
        assert False, j
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    finally:
        await bc.close()


async def test_get_billing_project(make_client):
    c = await make_client()
    r = await c.get_billing_project('test')
    assert r['billing_project'] == 'test', r
    assert set(r['users']) == {'test', 'test-dev'}, r
    assert r['status'] == 'open', r


async def test_list_billing_projects(make_client):
    c = await make_client()
    r = await c.list_billing_projects()
    test_bps = [p for p in r if p['billing_project'] == 'test']
    assert len(test_bps) == 1, r
    bp = test_bps[0]
    assert bp['billing_project'] == 'test', bp
    assert set(bp['users']) == {'test', 'test-dev'}, bp
    assert bp['status'] == 'open', bp


async def test_unauthorized_billing_project_modification(make_client, new_billing_project):
    project = new_billing_project
    client = await make_client()
    try:
        await client.create_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    else:
        assert False, 'expected error'

    try:
        await client.add_user('test', project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    else:
        assert False, 'expected error'

    try:
        await client.remove_user('test', project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    else:
        assert False, 'expected error'

    try:
        await client.close_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    else:
        assert False, 'expected error'

    try:
        await client.reopen_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    else:
        assert False, 'expected error'


async def test_create_billing_project(dev_client, new_billing_project):
    project = new_billing_project
    r = await dev_client.list_billing_projects()
    assert project in {bp['billing_project'] for bp in r}

    try:
        await dev_client.create_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'


async def test_close_reopen_billing_project(dev_client, new_billing_project):
    project = new_billing_project
    await dev_client.close_billing_project(project)
    r = await dev_client.list_billing_projects()
    assert [bp for bp in r if bp['billing_project'] == project and bp['status'] == 'open'] == [], r

    try:
        await dev_client.close_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e

    await dev_client.reopen_billing_project(project)
    r = await dev_client.list_billing_projects()
    assert [bp['billing_project'] for bp in r if bp['billing_project'] == project and bp['status'] == 'open'] == [project], r

    try:
        await dev_client.reopen_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'


async def test_close_billing_project_with_open_batch_errors(dev_client, make_client, new_billing_project):
    project = new_billing_project
    await dev_client.add_user("test", project)
    client = await make_client(project)
    b = await client.create_batch()._create()

    try:
        await dev_client.close_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'
    await client._patch(f'/api/v1alpha/batches/{b.id}/close')


async def test_close_nonexistent_billing_project(dev_client):
    try:
        await dev_client.close_billing_project("nonexistent_project")
    except aiohttp.ClientResponseError as e:
        assert e.status == 404, e
    else:
        assert False, 'expected error'


async def test_add_user_with_nonexistent_billing_project(dev_client):
    try:
        await dev_client.add_user("test", "nonexistent_project")
    except aiohttp.ClientResponseError as e:
        assert e.status == 404, e
    else:
        assert False, 'expected error'


async def test_remove_user_with_nonexistent_billing_project(dev_client):
    try:
        await dev_client.remove_user("test", "nonexistent_project")
    except aiohttp.ClientResponseError as e:
        assert e.status == 404, e
    else:
        assert False, 'expected error'


async def test_delete_billing_project_only_when_closed(dev_client, new_billing_project):
    project = new_billing_project
    try:
        await dev_client.delete_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'

    await dev_client.close_billing_project(project)
    await dev_client.delete_billing_project(project)

    try:
        await dev_client.get_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'

    try:
        await dev_client.reopen_billing_project(project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'


async def test_add_and_delete_user(dev_client, new_billing_project):
    project = new_billing_project
    r = await dev_client.add_user('test', project)
    assert r['user'] == 'test'
    assert r['billing_project'] == project

    bp = await dev_client.get_billing_project(project)
    assert r['user'] in bp['users'], bp

    try:
        await dev_client.add_user('test', project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 403, e
    else:
        assert False, 'expected error'

    r = await dev_client.remove_user('test', project)
    assert r['user']  == 'test'
    assert r['billing_project']  == project

    bp = await dev_client.get_billing_project(project)
    assert r['user'] not in bp['users']

    try:
        await dev_client.remove_user('test', project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 404, e
    else:
        assert False, 'expected error'


async def test_edit_billing_limit_dev(dev_client, new_billing_project):
    project = new_billing_project
    r = await dev_client.add_user('test', project)
    assert r['user'] == 'test'
    assert r['billing_project'] == project

    limit = 5
    r = await dev_client.edit_billing_limit(limit, project)
    assert r['limit'] == limit

    limit = None
    r = await dev_client.edit_billing_limit(limit, project)
    assert r['limit'] is None

    try:
        limit = 'foo'
        r = await dev_client.edit_billing_limit(limit, project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 400, e
    else:
        r = await dev_client.get_billing_project(project)
        assert r['limit'] is None, r
        assert False, 'expected error'

    try:
        limit = -1
        r = await dev_client.edit_billing_limit(limit, project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 400, e
    else:
        r = await dev_client.get_billing_project(project)
        assert r['limit'] is None, r
        assert False, 'expected error'


async def test_edit_billing_limit_nondev(make_client, dev_client, new_billing_project):
    project = new_billing_project
    r = await dev_client.add_user('test', project)
    assert r['user'] == 'test'
    assert r['billing_project'] == project

    client = await make_client(project)

    try:
        limit = 5
        await client.edit_billing_limit(limit, project)
    except aiohttp.ClientResponseError as e:
        assert e.status == 401, e
    else:
        r = await client.get_billing_project(project)
        assert r['limit'] is None, r
        assert False, 'expected error'


async def test_billing_project_accrued_costs(dev_client, new_billing_project):
    project = new_billing_project
    r = await dev_client.add_user('test', project)
    assert r['user'] == 'test'
    assert r['billing_project'] == project

    def approx_equal(x, y, tolerance=1e-10):
        return abs(x - y) <= tolerance

    b1 = await dev_client.create_batch()
    j1_1 = b1.create_job('ubuntu:18.04', command=['echo', 'head'])
    j1_2 = b1.create_job('ubuntu:18.04', command=['echo', 'head'])
    b1 = b1.submit()

    b2 = await dev_client.create_batch()
    j2_1 = b2.create_job('ubuntu:18.04', command=['echo', 'head'])
    j2_2 = b2.create_job('ubuntu:18.04', command=['echo', 'head'])
    b2 = b2.submit()

    b1 = b1.wait()
    b2 = b2.wait()

    b1_expected_cost = j1_1.status()['cost'] + j1_2.status()['cost']
    assert approx_equal(b1_expected_cost, b1['cost']), (b1_expected_cost, b1['cost'])

    b2_expected_cost = j2_1.status()['cost'] + j2_2.status()['cost']
    assert approx_equal(b2_expected_cost, b2['cost']), (b2_expected_cost, b2['cost'])

    cost_by_batch = b1['cost'] + b2['cost']
    cost_by_billing_project = (await dev_client.get_billing_project(project))['accrued_cost']

    assert approx_equal(cost_by_batch, cost_by_billing_project), (cost_by_batch, cost_by_billing_project)