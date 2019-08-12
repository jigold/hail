import os
import asyncio
import aiohttp
import googleapiclient.discovery
import logging
import google.oauth2.service_account
import requests

log = logging.getLogger('driver')


class Driver:
    def __init__(self, batch_gsa_key=None):
        self._session = aiohttp.ClientSession(raise_for_status=True,
                                              timeout=aiohttp.ClientTimeout(total=60))

        self._cookies = None
        self._headers = None
        self.instance = 'batch-agent-8'
        self.url = 'batch-agent-8:5000'

        if batch_gsa_key is None:
            batch_gsa_key = os.environ.get('BATCH_GSA_KEY', '/batch-gsa-key/privateKeyData')
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(
            batch_gsa_key)
        self.compute_client = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
        log.info(self.compute_client.instances().get(project='hail-vdc', zone='us-central1-a', instance=self.instance).execute())
        log.info(requests.get(self.url + '/healthcheck'))

    async def _get(self, path, params=None):
        response = await self._session.get(
            self.url + path, params=params, cookies=self._cookies, headers=self._headers)
        return await response.json()

    async def _post(self, path, json=None):
        response = await self._session.post(
            self.url + path, json=json, cookies=self._cookies, headers=self._headers)
        return await response.json()

    async def _patch(self, path):
        await self._session.patch(
            self.url + path, cookies=self._cookies, headers=self._headers)

    async def _delete(self, path):
        await self._session.delete(
            self.url + path, cookies=self._cookies, headers=self._headers)

    async def create_pod(self, spec):
        await self._post('/api/v1alpha/pods/create', json=spec)

        # submit request to that instance
        # update db

    async def delete_pod(self, name):
        pass

    async def read_pod_log(self, name, container):
        pass

    async def read_pod_status(self, name):
        pass

    async def list_pods(self):
        pass
