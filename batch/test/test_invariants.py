import logging
import asyncio
import pytest
import aiohttp

from hailtop.config import get_deploy_config
from hailtop.auth import service_auth_headers
from hailtop.tls import in_cluster_ssl_client_session
import hailtop.utils as utils

pytestmark = pytest.mark.asyncio

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


async def test_invariants():
    deploy_config = get_deploy_config()
    url = deploy_config.url('batch-driver', '/check_invariants')
    headers = service_auth_headers(deploy_config, 'batch-driver')
    async with in_cluster_ssl_client_session(
            raise_for_status=True,
            timeout=aiohttp.ClientTimeout(total=60)) as session:

        resp = await utils.request_retry_transient_errors(
            session, 'GET', url, headers=headers)
        data = await resp.json()

        assert data['check_incremental_error'] is None, data
        assert data['check_resource_aggregation_error'] is None, data
