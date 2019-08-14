import os
import asyncio
import aiohttp
import googleapiclient.discovery
import logging
import google.oauth2.service_account
import requests
import kubernetes as kube

log = logging.getLogger('driver')


class Driver:
    def __init__(self, v1, batch_gsa_key=None):
        self._session = aiohttp.ClientSession(raise_for_status=True,
                                              timeout=aiohttp.ClientTimeout(total=60))
        self.event_queue = asyncio.Queue()

        self._cookies = None
        self._headers = None
        self.v1 = v1
        self.instance = 'batch-agent-8'
        self.url = 'http://10.128.0.95:5000'

        # if batch_gsa_key is None:
        #     batch_gsa_key = os.environ.get('BATCH_GSA_KEY', '/batch-gsa-key/privateKeyData')
        # credentials = google.oauth2.service_account.Credentials.from_service_account_file(
        #     batch_gsa_key)
        # self.compute_client = googleapiclient.discovery.build('compute', 'v1', credentials=credentials)
        # result = self.compute_client.instances().get(project='hail-vdc', zone='us-central1-a', instance=self.instance).execute()

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

    async def create_pod(self, spec, secrets, output_directory):
        try:
            body = {'spec': spec,
                    'secrets': secrets,
                    'output_directory': output_directory}
            await self._post('/api/v1alpha/pods/create', json=body)
            return None
        except Exception as err:
            return err

    async def delete_pod(self, name):
        log.info('calling delete pod')
        try:
            await self._delete(f'/api/v1alpha/pods/{name}/delete')
            return None
        except Exception as err:
            return err

    async def read_pod_log(self, name, container):
        try:
            result = await self._get(f'/api/v1alpha/pods/{name}/containers/{container}/log')
            return result, None
        except Exception as err:
            return None, err

    async def read_pod_status(self, name):
        try:
            result = await self._get(f'/api/v1alpha/pods/{name}')
            return result, None
        except Exception as err:
            return None, err

    async def list_pods(self):
        try:
            result = await self._get('/api/v1alpha/pods')
            return [self.v1.api_client._ApiClient__deserialize(data, kube.client.V1Pod) for data in result], None
        except Exception as err:
            log.info(err)
            return None, err
