import os

import hailjwt as hj

from . import api
from .poll_until import poll_until


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

    def is_complete(self):
        if self._status:
            state = self._status['state']
            if state in ('Complete', 'Cancelled'):
                return True
        return False

    def cached_status(self):
        assert self._status is not None
        return self._status

    def status(self):
        self._status = self.client._get_job(self.batch.id, self.id)
        return self._status

    def wait(self):
        def update_and_is_complete():
            self.status()
            return self.is_complete()
        poll_until(update_and_is_complete)
        return self._status

    def log(self):
        return self.client._get_job_log(self.batch.id, self.id)


class Batch:
    def __init__(self, client, id):
        self.client = client
        self.id = id

    def create_job(self, image, command=None, args=None, env=None, ports=None,
                   resources=None, tolerations=None, volumes=None, security_context=None,
                   service_account_name=None, attributes=None, callback=None, parents=None,
                   input_files=None, output_files=None, always_run=False):

        if parents is None:
            parents = []
        parent_ids = [p.id for p in parents]

        return self.client._create_job(
            self, image, command, args, env, ports, resources, tolerations, volumes, security_context,
            service_account_name, attributes, self.id, callback, parent_ids, input_files,
            output_files, always_run)

    def close(self):
        self.client._close_batch(self.id)

    def status(self):
        return self.client._get_batch(self.id)

    def wait(self):
        def update_and_is_complete():
            status = self.status()
            if status['complete']:
                return status
            return False
        return poll_until(update_and_is_complete)

    def cancel(self):
        self.client._cancel_batch(self.id)

    def delete(self):
        self.client._delete_batch(self.id)


class BatchClient:
    def __init__(self, url=None, timeout=None, token_file=None, token=None, headers=None):
        if token_file is not None and token is not None:
            raise ValueError('set only one of token_file and token')
        if not url:
            url = 'http://batch.default'
        self.url = url
        if token is None:
            token_file = (token_file or
                          os.environ.get('HAIL_TOKEN_FILE') or
                          os.path.expanduser('~/.hail/token'))
            if not os.path.exists(token_file):
                raise ValueError(
                    f'cannot create a client without a token. no file was '
                    f'found at {token_file}')
            with open(token_file) as f:
                token = f.read()
        userdata = hj.JWTClient.unsafe_decode(token)
        assert "bucket_name" in userdata
        self.bucket = userdata["bucket_name"]
        self.api = api.API(timeout=timeout,
                           cookies={'user': token},
                           headers=headers)

    def _create_job(self,  # pylint: disable=R0912
                    batch,
                    image,
                    command,
                    args,
                    env,
                    ports,
                    resources,
                    tolerations,
                    volumes,
                    security_context,
                    service_account_name,
                    attributes,
                    batch_id,
                    callback,
                    parent_ids,
                    input_files,
                    output_files,
                    always_run):
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

        j = self.api.create_job(self.url, spec, attributes, batch_id, callback,
                                parent_ids, input_files, output_files, always_run)
        return Job(self,
                   batch,
                   j['job_id'],
                   attributes=j.get('attributes'),
                   parent_ids=j.get('parent_ids', []))

    def _get_job(self, batch_id, job_id):
        return self.api.get_job(self.url, batch_id, job_id)

    def _get_job_log(self, batch_id, job_id):
        return self.api.get_job_log(self.url, batch_id, job_id)

    def _get_batch(self, batch_id):
        return self.api.get_batch(self.url, batch_id)

    def _cancel_batch(self, batch_id):
        self.api.cancel_batch(self.url, batch_id)

    def _delete_batch(self, batch_id):
        self.api.delete_batch(self.url, batch_id)

    def _close_batch(self, batch_id):
        return self.api.close_batch(self.url, batch_id)

    def list_batches(self, complete=None, success=None, attributes=None):
        batches = self.api.list_batches(self.url, complete=complete, success=success, attributes=attributes)
        return [Batch(self, b['id']) for b in batches]

    def get_job(self, batch_id, job_id):
        # make sure job exists
        j = self._get_job(batch_id, job_id)
        return Job(self,
                   Batch(self, j['batch_id']),
                   j['job_id'],
                   attributes=j.get('attributes'),
                   parent_ids=j.get('parent_ids', []),
                   _status=j)

    def get_batch(self, id):
        # make sure batch exists
        self._get_batch(id)
        return Batch(self, id)

    def create_batch(self, attributes=None, callback=None, ttl=None):
        batch = self.api.create_batch(self.url, attributes, callback, ttl)
        return Batch(self, batch['id'])
