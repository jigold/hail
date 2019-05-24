import os
import math
import time
import random

import hailjwt as hj

from .requests_helper import filter_params


class Job:
    @staticmethod
    def exit_code(job_status):
        if 'exit_code' not in job_status or job_status['exit_code'] is None:
            return None

        exit_codes = job_status['exit_code']
        exit_codes = [exit_codes[task] for task in ['input', 'main', 'output'] if task in exit_codes]

        i = 0
        while i < len(exit_codes):
            ec = exit_codes[i]
            if ec is None:
                return None
            if ec > 0:
                return ec
            i += 1
        return 0

    def __init__(self, batch, job_id, attributes=None, parent_ids=None, _status=None):
        if parent_ids is None:
            parent_ids = []
        if attributes is None:
            attributes = {}

        self.batch = batch
        self.job_id = job_id
        self.attributes = attributes
        self.parent_ids = parent_ids
        self._status = _status

    @property
    def batch_id(self):
        return self.batch.id

    @property
    def id(self):
        return self.batch_id, self.job_id

    async def is_complete(self):
        if not self.batch._submitted:
            raise ValueError('Cannot ask if a job is complete if the job is in an unsubmitted batch')

        if self._status:
            state = self._status['state']
            if state in ('Complete', 'Cancelled'):
                return True
        await self.status()
        state = self._status['state']
        return state in ('Complete', 'Cancelled')

    async def status(self):
        if not self.batch._submitted:
            raise ValueError('Cannot get the status of a job in an unsubmitted batch')
        self._status = await self.batch._client._get('/batches/{}/jobs/{}'.format(*self.id))
        return self._status

    async def wait(self):
        if not self.batch._submitted:
            raise ValueError('Cannot wait on a job in an unsubmitted batch')
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
        if not self.batch._submitted:
            raise ValueError('Cannot get the log of a job in an unsubmitted batch')
        return await self.batch._client._get('/batches/{}/jobs/{}/log'.format(*self.id))


class Batch:
    @staticmethod
    def create_batch(client, attributes, callback):
        doc = {'jobs': []}
        if attributes:
            doc['attributes'] = attributes
        if callback:
            doc['callback'] = callback

        b = Batch(client, attributes=attributes)
        b._doc = doc
        b._submitted = False
        return b

    def __init__(self, client, id=None, attributes=None):
        self._client = client
        self.id = id
        self.attributes = attributes
        self._doc = None
        self._submitted = True
        self._job_idx = 0

    def create_job(self, image, command=None, args=None, env=None, ports=None,
                   resources=None, tolerations=None, volumes=None, security_context=None,
                   service_account_name=None, attributes=None, callback=None, parents=None,
                   input_files=None, output_files=None, always_run=False, pvc_size=None):
        if self._submitted:
            raise ValueError(f'Cannot create a new job in an already submitted batch.')

        self._job_idx += 1

        if parents is None:
            parents = []
        parent_ids = [p.job_id for p in parents]

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
            'batch_id': self.id,
            'job_id': self._job_idx
        }
        if attributes:
            doc['attributes'] = attributes
        if callback:
            doc['callback'] = callback
        if input_files:
            doc['input_files'] = input_files
        if output_files:
            doc['output_files'] = output_files
        if pvc_size:
            doc['pvc_size'] = pvc_size

        self._doc['jobs'].append(doc)

        return Job(self,
                   self._job_idx,
                   attributes=attributes,
                   parent_ids=parent_ids)

    async def cancel(self):
        if not self._submitted:
            raise ValueError('An unsubmitted batch cannot be cancelled.')
        await self._client._patch('/batches/{}/cancel'.format(self.id))

    async def status(self):
        if not self._submitted:
            raise ValueError('An unsubmitted batch has no status.')
        return await self._client._get('/batches/{}'.format(self.id))

    async def wait(self):
        if not self._submitted:
            raise ValueError('An unsubmitted batch cannot be waited on.')
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
        if not self._submitted:
            raise ValueError('An unsubmitted batch cannot be deleted.')
        await self._client._delete('/batches/{}'.format(self.id))

    async def submit(self):
        if self._submitted:
            raise ValueError('A submitted batch cannot be resubmitted.')
        b = await self._client._post('/batches/create', json=self._doc)
        self._submitted = True
        self.id = b['id']
        self.attributes = b.get('attributes')


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
        assert "bucket_name" in userdata
        self.bucket = userdata["bucket_name"]
        self._cookies = {'user': token}
        self._headers = headers

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

    async def _refresh_k8s_state(self):
        await self._post('/refresh_k8s_state')

    async def list_batches(self, complete=None, success=None, attributes=None):
        params = filter_params(complete, success, attributes)
        batches = await self._get('/batches', params=params)
        return [Batch(self,
                      b['id'],
                      attributes=b.get('attributes'))
                for b in batches]

    async def get_job(self, batch_id, job_id):
        b = await self.get_batch(batch_id)
        j = await self._get('/batches/{}/jobs/{}'.format(batch_id, job_id))
        return Job(b,
                   j['job_id'],
                   attributes=j.get('attributes'),
                   parent_ids=j.get('parent_ids', []),
                   _status=j)

    async def get_batch(self, batch_id):
        b = await self._get(f'/batches/{batch_id}')
        return Batch(self,
                     b['id'],
                     attributes=b.get('attributes'))

    def create_batch(self, attributes=None, callback=None):
        return Batch.create_batch(self, attributes, callback)

    async def close(self):
        await self._session.close()
        self._session = None
