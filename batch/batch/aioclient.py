import os
import math
import time
import random

import hailjwt as hj

from .requests_helper import filter_params


class Job:
    def __init__(self, client, batch, id, attributes=None, parent_ids=None, _status=None):
        if parent_ids is None:
            parent_ids = []
        if attributes is None:
            attributes = {}

        self.client = client
        self.batch = batch
        self.id = id
        self.attributes = attributes
        self.parent_ids = parent_ids
        self._status = _status

    async def is_complete(self):
        if self._status:
            state = self._status['state']
            if state in ('Complete', 'Cancelled'):
                return True
        await self.status()
        state = self._status['state']
        return state in ('Complete', 'Cancelled')

    async def status(self):
        self._status = await self.client._get('/batches/{}/jobs/{}'.format(self.batch.id, self.id))
        return self._status

    async def wait(self):
        i = 0
        while True:
            if await self.is_complete():
                return self._status
            j = random.randrange(math.floor(1.1 ** i))
            time.sleep(0.100 * j)
            # max 4.45s
            if i < 64:
                i = i + 1

    async def log(self):
        return await self.client._get('/batches/{}/jobs/{}/log'.format(self.batch.id, self.id))


class Batch:
    def __init__(self, client, id):
        self.client = client
        self.id = id

    async def create_job(self, image, command=None, args=None, env=None, ports=None,
                         resources=None, tolerations=None, volumes=None, security_context=None,
                         service_account_name=None, attributes=None, callback=None, parents=None,
                         input_files=None, output_files=None, always_run=False):
        if parents is None:
            parents = []
        parent_ids = [p.id for p in parents]

        if env:
            env = [{'name': k, 'value': v} for (k, v) in env.items()]
        else:
            env = []
        env.extend([{
            'name': 'POD_IP',
            'valueFrom': {
                'fieldRef': {'fieldPath': 'status.podIP'}
            }
        }, {
            'name': 'POD_NAME',
            'valueFrom': {
                'fieldRef': {'fieldPath': 'metadata.name'}
            }
        }])

        container = {
            'image': image,
            'name': 'main'
        }
        if command:
            container['command'] = command
        if args:
            container['args'] = args
        if env:
            container['env'] = env
        if ports:
            container['ports'] = [{
                'containerPort': p,
                'protocol': 'TCP'
            } for p in ports]
        if resources:
            container['resources'] = resources
        if volumes:
            container['volumeMounts'] = [v['volume_mount'] for v in volumes]
        spec = {
            'containers': [container],
            'restartPolicy': 'Never'
        }
        if volumes:
            spec['volumes'] = [v['volume'] for v in volumes]
        if tolerations:
            spec['tolerations'] = tolerations
        if security_context:
            spec['securityContext'] = security_context
        if service_account_name:
            spec['serviceAccountName'] = service_account_name

        doc = {
            'spec': spec,
            'parent_ids': parent_ids,
            'always_run': always_run,
            'batch_id': self.id
        }
        if attributes:
            doc['attributes'] = attributes
        if callback:
            doc['callback'] = callback
        if input_files:
            doc['input_files'] = input_files
        if output_files:
            doc['output_files'] = output_files

        j = await self.client._post('/jobs/create', json=doc)

        return Job(self.client,
                   self,
                   j['job_id'],
                   attributes=j.get('attributes'),
                   parent_ids=j.get('parent_ids', []))

    async def close(self):
        await self.client._patch('/batches/{}/close'.format(self.id))

    async def cancel(self):
        await self.client._patch('/batches/{}/cancel'.format(self.id))

    async def status(self):
        return await self.client._get('/batches/{}'.format(self.id))

    async def wait(self):
        i = 0
        while True:
            status = await self.status()
            if status['complete']:
                return status
            j = random.randrange(math.floor(1.1 ** i))
            time.sleep(0.100 * j)
            # max 4.45s
            if i < 64:
                i = i + 1

    async def delete(self):
        await self.client._delete('/batches/{}/delete'.format(self.id))


class BatchClient:
    def __init__(self, session, url=None, token_file=None, token=None, headers=None):
        if not url:
            url = 'http://batch.default'
        self.url = url
        self._session = session
        if token is None:
            token_file = (token_file or
                          os.environ.get('HAIL_TOKEN_FILE') or
                          os.path.expanduser('~/.hail/token'))
            if not os.path.exists(token_file):
                raise ValueError(
                    f'Cannot create a client without a token. No file was '
                    f'found at {token_file}')
            with open(token_file) as f:
                token = f.read()
        userdata = hj.JWTClient.unsafe_decode(token)
        assert "bucket_name" in userdata, userdata
        self.bucket = userdata["bucket_name"]
        self.cookies = {'user': token}
        self.headers = headers

    async def _get(self, path, params=None):
        response = await self._session.get(
            self.url + path, params=params, cookies=self.cookies, headers=self.headers)
        return await response.json()

    async def _post(self, path, json=None):
        response = await self._session.post(
            self.url + path, json=json, cookies=self.cookies, headers=self.headers)
        return await response.json()

    async def _patch(self, path):
        await self._session.patch(
            self.url + path, cookies=self.cookies, headers=self.headers)

    async def _delete(self, path):
        await self._session.delete(
            self.url + path, cookies=self.cookies, headers=self.headers)

    async def _refresh_k8s_state(self):
        await self._post('/refresh_k8s_state')

    async def list_batches(self, complete=None, success=None, attributes=None):
        params = filter_params(complete, success, attributes)
        batches = await self._get('/batches', params=params)
        return [Batch(self, b['id']) for b in batches]

    async def get_job(self, batch_id, job_id):
        j = await self._get('/batches/{}/jobs/{}'.format(batch_id, job_id))
        return Job(self,
                   Batch(self, j['batch_id']),
                   j['job_id'],
                   attributes=j.get('attributes'),
                   parent_ids=j.get('parent_ids', []),
                   _status=j)

    async def get_batch(self, id):
        # make sure batch exists
        await self._get(f'/batches/{id}')
        return Batch(self, id)

    async def create_batch(self, attributes=None, callback=None, ttl=None):
        doc = {}
        if attributes:
            doc['attributes'] = attributes
        if callback:
            doc['callback'] = callback
        if ttl:
            doc['ttl'] = ttl
        b = await self._post('/batches/create', json=doc)
        return Batch(self, b['id'])

    async def close(self):
        await self._session.close()
        self._session = None
