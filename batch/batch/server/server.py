import sys
import os
import time
import random
import sched
import uuid
import json
from collections import Counter
import logging
import threading
import kubernetes as kube
import cerberus
import requests
import uvloop
import asyncio
import aiohttp_jinja2
import jinja2
import pymysql
from aiohttp import web
from asyncinit import asyncinit

from .globals import _log_path, _read_file, pod_name_job, job_id_job, batch_id_batch
from .globals import next_id, get_recent_events, add_event

from .. import schemas
from .database import Database

uvloop.install()

s = sched.scheduler()


def schedule(ttl, fun, args=(), kwargs=None):
    if kwargs is None:
        kwargs = {}
    s.enter(ttl, 1, fun, args, kwargs)


if not os.path.exists('logs'):
    os.mkdir('logs')
else:
    if not os.path.isdir('logs'):
        raise OSError('logs exists but is not a directory')


def make_logger():
    fmt = logging.Formatter(
        # NB: no space after levename because WARNING is so long
        '%(levelname)s\t| %(asctime)s \t| %(filename)s \t| %(funcName)s:%(lineno)d | '
        '%(message)s')

    file_handler = logging.FileHandler('batch.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(fmt)

    log = logging.getLogger('batch')
    log.setLevel(logging.INFO)

    logging.basicConfig(handlers=[file_handler, stream_handler], level=logging.INFO)

    return log


log = make_logger()

KUBERNETES_TIMEOUT_IN_SECONDS = float(os.environ.get('KUBERNETES_TIMEOUT_IN_SECONDS', 5.0))
REFRESH_INTERVAL_IN_SECONDS = int(os.environ.get('REFRESH_INTERVAL_IN_SECONDS', 5 * 60))
POD_NAMESPACE = os.environ.get('POD_NAMESPACE', 'batch-pods')
POD_VOLUME_SIZE = os.environ.get('POD_VOLUME_SIZE', '10Mi')

log.info(f'KUBERNETES_TIMEOUT_IN_SECONDS {KUBERNETES_TIMEOUT_IN_SECONDS}')
log.info(f'REFRESH_INTERVAL_IN_SECONDS {REFRESH_INTERVAL_IN_SECONDS}')
log.info(f'POD_NAMESPACE {POD_NAMESPACE}')
log.info(f'POD_VOLUME_SIZE {POD_VOLUME_SIZE}')

STORAGE_CLASS_NAME = 'batch'

if 'BATCH_USE_KUBE_CONFIG' in os.environ:
    kube.config.load_kube_config()
else:
    kube.config.load_incluster_config()
v1 = kube.client.CoreV1Api()

instance_id = uuid.uuid4().hex
log.info(f'instance_id = {instance_id}')

app = web.Application()
routes = web.RouteTableDef()
aiohttp_jinja2.setup(app, loader=jinja2.PackageLoader('batch', 'templates'))

loop = asyncio.get_event_loop()
db = loop.run_until_complete(Database(os.environ.get('CLOUD_SQL_CONFIG_PATH',
                                                     '/batch-secrets/batch-production-cloud-sql-config.json')))


def abort(code, reason=None):
    if code == 400:
        raise web.HTTPBadRequest(reason=reason)
    if code == 404:
        raise web.HTTPNotFound(reason=reason)
    raise web.HTTPException(reason=reason)


def jsonify(data):
    return web.json_response(data)


class JobTask:  # pylint: disable=R0903
    @classmethod
    def from_dict(cls, d):
        jt = object.__new__(cls)
        jt.name = d['name']
        assert d['pod_template'] is not None
        jt.pod_template = v1.api_client._ApiClient__deserialize(d['pod_template'], kube.client.V1Pod)
        return jt

    @staticmethod
    def copy_task(job_id, task_name, files, copy_service_account_name):
        if files is not None:
            assert copy_service_account_name is not None
            authenticate = 'gcloud -q auth activate-service-account --key-file=/gcp-sa-key/key.json'
            copies = ' & '.join([f'gsutil cp {src} {dst}' for (src, dst) in files])
            wait = 'wait'
            sh_expression = f'{authenticate} && ({copies} ; {wait})'
            container = kube.client.V1Container(
                image='google/cloud-sdk:237.0.0-alpine',
                name=task_name,
                command=['/bin/sh', '-c', sh_expression],
                volume_mounts=[
                    kube.client.V1VolumeMount(
                        mount_path='/gcp-sa-key',
                        name='gcp-sa-key')])
            spec = kube.client.V1PodSpec(
                containers=[container],
                restart_policy='Never',
                volumes=[
                    kube.client.V1Volume(
                        secret=kube.client.V1SecretVolumeSource(
                            secret_name=f'gcp-sa-key-{copy_service_account_name}'),
                        name='gcp-sa-key')])
            return JobTask(job_id, task_name, spec)
        return None

    def __init__(self, job_id, name, pod_spec):
        assert pod_spec is not None

        metadata = kube.client.V1ObjectMeta(generate_name='job-{}-{}-'.format(job_id, name),
                                            labels={
                                                'app': 'batch-job',
                                                'hail.is/batch-instance': instance_id,
                                                'uuid': uuid.uuid4().hex
                                            })

        self.pod_template = kube.client.V1Pod(metadata=metadata,
                                              spec=pod_spec)
        self.name = name

    def to_dict(self):
        return {'name': self.name,
                'pod_template': self.pod_template.to_dict()}


@asyncinit
class Job:
    async def _next_task(self):
        self._task_idx += 1
        await db.jobs.update_record(self.id, task_idx=self._task_idx)
        if self._task_idx < len(self._tasks):
            self._current_task = self._tasks[self._task_idx]

    def _has_next_task(self):
        return self._task_idx < len(self._tasks)

    def _create_pvc(self):
        pvc = v1.create_namespaced_persistent_volume_claim(
            POD_NAMESPACE,
            kube.client.V1PersistentVolumeClaim(
                metadata=kube.client.V1ObjectMeta(
                    generate_name=f'job-{self.id}-',
                    labels={'app': 'batch-job',
                            'hail.is/batch-instance': instance_id}),
                spec=kube.client.V1PersistentVolumeClaimSpec(
                    access_modes=['ReadWriteOnce'],
                    volume_mode='Filesystem',
                    resources=kube.client.V1ResourceRequirements(
                        requests={'storage': POD_VOLUME_SIZE}),
                    storage_class_name=STORAGE_CLASS_NAME)),
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        log.info(f'created pvc name: {pvc.metadata.name} for job {self.id}')
        return pvc

    async def _create_pod(self):
        assert self._pod_name is None
        assert self._current_task is not None

        if len(self._tasks) > 1:
            if self._pvc is None:
                self._pvc = self._create_pvc()
                await db.jobs.update_record(self.id,
                                            pvc=json.dumps(v1.api_client.sanitize_for_serialization(self._pvc.to_dict())))
            current_pod_spec = self._current_task.pod_template.spec
            if current_pod_spec.volumes is None:
                current_pod_spec.volumes = []
            current_pod_spec.volumes.append(
                kube.client.V1Volume(
                    persistent_volume_claim=kube.client.V1PersistentVolumeClaimVolumeSource(
                        claim_name=self._pvc.metadata.name),
                    name=self._pvc.metadata.name))
            for container in current_pod_spec.containers:
                if container.volume_mounts is None:
                    container.volume_mounts = []
                container.volume_mounts.append(
                    kube.client.V1VolumeMount(
                        mount_path='/io',
                        name=self._pvc.metadata.name))

        pod = v1.create_namespaced_pod(
            POD_NAMESPACE,
            self._current_task.pod_template,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        self._pod_name = pod.metadata.name

        await db.jobs.update_record(self.id,
                                    pod_name=self._pod_name)
        pod_name_job[self._pod_name] = self

        add_event({'message': f'created pod for job {self.id}, task {self._current_task.name}',
                   'command': f'{pod.spec.containers[0].command}'})

        log.info('created pod name: {} for job {}, task {}'.format(self._pod_name,
                                                                   self.id,
                                                                   self._current_task.name))

    async def _delete_pvc(self):
        if self._pvc is not None:
            try:
                v1.delete_namespaced_persistent_volume_claim(
                    self._pvc.metadata.name,
                    POD_NAMESPACE,
                    _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            except kube.client.rest.ApiException as err:
                if err.status == 404:
                    log.info(f'persistent volume claim {self._pvc.metadata.name} in '
                             f'{self._pvc.metadata.namespace} is already deleted')
                    return
                raise
            self._pvc = None
            await db.jobs.update_record(self.id, pvc=None)

    async def _delete_k8s_resources(self):
        await self._delete_pvc()
        if self._pod_name is not None:
            try:
                v1.delete_namespaced_pod(
                    self._pod_name,
                    POD_NAMESPACE,
                    _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            except kube.client.rest.ApiException as err:
                if err.status == 404:
                    pass
                raise
            del pod_name_job[self._pod_name]
            self._pod_name = None
            await db.jobs.update_record(self.id, pod_name=None)

    def _read_logs(self):
        logs = {jt.name: _read_file(_log_path(self.id, jt.name))
                for idx, jt in enumerate(self._tasks) if idx < self._task_idx and jt is not None}
        if self._state == 'Created':
            if self._pod_name:
                try:
                    log = v1.read_namespaced_pod_log(
                        self._pod_name,
                        POD_NAMESPACE,
                        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
                    logs[self._current_task.name] = log
                except kube.client.rest.ApiException:
                    pass
            return logs
        if self._state == 'Complete':
            return logs
        assert self._state == 'Cancelled'
        return None

    @classmethod
    async def from_sql(cls, id):
        record = await db.jobs.get_record(id)
        record = record.result()
        if record is not None:
            j = object.__new__(cls)

            j.id = id
            j.batch_id = record['batch_id']
            j.attributes = json.loads(record['attributes'])
            j.callback = record['callback']
            # j.child_ids = json.loads(record['child_ids']) # FIXME
            # j.parent_ids = json.loads(record['parent_ids'])

            # x = await db.jobs.get_records(j.parent_ids)
            # print(x.result())
            # j.incomplete_parent_ids = set([parent.id for parent in x.result() if parent['state'] == 'Created'])
            # j.incomplete_parent_ids = set([])
            j.scratch_folder = record['scratch_folder']
            j.always_run = record['always_run']

            if record['pvc']:
                j._pvc = v1.api_client._ApiClient__deserialize(json.loads(record['pvc']),
                                                               kube.client.V1PersistentVolumeClaim)
            else:
                j._pvc = None

            j._pod_name = record['pod_name']
            j.exit_code = record['exit_code']
            j._state = record['state']
            j._task_idx = record['task_idx']

            x = json.loads(record['tasks'])
            j._tasks = [JobTask.from_dict(item) if item is not None else None for item in x]
            j._current_task = j._tasks[j._task_idx] if j._task_idx < len(j._tasks) else None

            return j

    async def __init__(self, pod_spec, batch_id, attributes, callback, parent_ids,
                       scratch_folder, input_files, output_files, copy_service_account_name,
                       always_run):
        self.batch_id = batch_id
        self.attributes = attributes
        self.callback = callback
        # self.child_ids = set([])
        # self.parent_ids = parent_ids
        self.incomplete_parent_ids = set(self.parent_ids)
        self.scratch_folder = scratch_folder
        self.always_run = always_run

        self._pvc = None
        self._pod_name = None
        self.exit_code = None
        self._state = 'Created'
        self._task_idx = -1
        self._current_task = None

        self.id = await db.jobs.new_record(state=self._state,
                                           exit_code=self.exit_code,
                                           batch_id=self.batch_id,
                                           scratch_folder=self.scratch_folder,
                                           pod_name=self._pod_name,
                                           pvc=self._pvc,
                                           callback=self.callback,
                                           attributes=json.dumps(self.attributes),
                                           task_idx=self._task_idx,
                                           always_run=self.always_run) #,
                                           # parent_ids=json.dumps(self.parent_ids))

        self._tasks = [JobTask.copy_task(self.id, 'input', input_files, copy_service_account_name),
                       JobTask(self.id, 'main', pod_spec),
                       JobTask.copy_task(self.id, 'output', output_files, copy_service_account_name)]
        await db.jobs.update_record(self.id,
                                    tasks=json.dumps([jt.to_dict() if jt is not None else None for jt in self._tasks]))

        await self._next_task()
        assert self._current_task is not None

        job_id_job[self.id] = self

        for parent in parent_ids:
            db.job_id_parents.new_record(id=self.id,
                                         parent=parent.id)
            # job_id_job[parent].child_ids.add(self.id)
            # db.jobs.update_record(parent.id, ...)  # FIXME: parent-to-children

        if batch_id:
            batch = batch_id_batch[batch_id]
            batch.jobs.add(self)

        log.info('created job {}'.format(self.id))
        add_event({'message': f'created job {self.id}'})

        if not self.parent_ids:
            await self._create_pod()
        else:
            await self.refresh_parents_and_maybe_create()

    # pylint incorrect error: https://github.com/PyCQA/pylint/issues/2047
    async def refresh_parents_and_maybe_create(self):  # pylint: disable=invalid-name
        for parent in self.parent_ids:
            parent_job = job_id_job[parent]
            await self.parent_new_state(parent_job._state, parent, parent_job.exit_code)

    async def set_state(self, new_state):
        if self._state != new_state:
            log.info('job {} changed state: {} -> {}'.format(
                self.id,
                self._state,
                new_state))
            self._state = new_state
            await self.notify_children(new_state)
            await db.jobs.update_record(self.id, state=new_state)

    async def notify_children(self, new_state):
        child_ids = await db.jobs_parents.get_children(self.id)
        for child_id in child_ids:
            child = job_id_job.get(child_id)
            if child:
                await child.parent_new_state(new_state, self.id, self.exit_code)
            else:
                log.info(f'missing child: {child_id}')

    async def parent_new_state(self, new_state, parent_id, maybe_exit_code):
        async def update():
            # incomplete_parent_ids =  # FIXME
            self.incomplete_parent_ids.discard(parent_id)  # FIXME
            if not self.incomplete_parent_ids:
                assert self._state in ('Cancelled', 'Created'), f'bad state: {self._state}'
                if self._state != 'Cancelled':
                    log.info(f'all parents complete for {self.id},'
                             f' creating pod')
                    await self._create_pod()
                else:
                    log.info(f'all parents complete for {self.id},'
                             f' but it is already cancelled')

        if new_state == 'Complete' and maybe_exit_code == 0:
            log.info(f'parent {parent_id} successfully complete for {self.id}')
            await update()
        elif new_state == 'Cancelled' or (new_state == 'Complete' and maybe_exit_code != 0):
            log.info(f'parents deleted, cancelled, or failed: {new_state} {maybe_exit_code} {parent_id}')
            if not self.always_run:
                await self.cancel()
            else:
                log.info(f'job {self.id} is set to always run despite '
                         f' parents deleted, cancelled, or failed.')
                await update()

    async def cancel(self):
        if self.is_complete():
            return
        await self._delete_k8s_resources()
        await self.set_state('Cancelled')

    async def delete(self):
        # remove from structures
        del job_id_job[self.id]
        await db.jobs.update_record(self.id, pod_name=None)  # FIXME: should this get removed from database

        if self.batch_id:
            batch = batch_id_batch[self.batch_id]
            batch.remove(self)

        await self._delete_k8s_resources()
        await self.set_state('Cancelled')

    def is_complete(self):
        return self._state == 'Complete' or self._state == 'Cancelled'

    async def mark_unscheduled(self):
        if self._pod_name:
            del pod_name_job[self._pod_name]
            self._pod_name = None
            await db.jobs.update_record(self.id, pod_name=None)
        await self._create_pod()

    async def mark_complete(self, pod):
        task_name = self._current_task.name

        self.exit_code = pod.status.container_statuses[0].state.terminated.exit_code
        await db.jobs.update_record(self.id, exit_code=self.exit_code)

        pod_log = v1.read_namespaced_pod_log(
            pod.metadata.name,
            POD_NAMESPACE,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

        add_event({'message': f'job {self.id}, {task_name} task exited', 'log': pod_log[:64000]})

        fname = _log_path(self.id, task_name)
        with open(fname, 'w') as f:
            f.write(pod_log)
        log.info(f'wrote log for job {self.id}, {task_name} task to {fname}')

        if self._pod_name:
            del pod_name_job[self._pod_name]
            self._pod_name = None
            await db.jobs.update_record(self.id, pod_name=None)

        await self._next_task()
        if self.exit_code == 0:
            if self._has_next_task():
                await self._create_pod()
                return
            await self._delete_pvc()
        else:
            await self._delete_pvc()

        await self.set_state('Complete')
        # await db.jobs.update_record(self.id, time_ended="NOW()") # FIXME

        log.info('job {} complete, exit_code {}'.format(self.id, self.exit_code))

        if self.callback:
            def handler(id, callback, json):
                try:
                    requests.post(callback, json=json, timeout=120)
                except requests.exceptions.RequestException as exc:
                    log.warning(
                        f'callback for job {id} failed due to an error, I will not retry. '
                        f'Error: {exc}')

            threading.Thread(target=handler, args=(self.id, self.callback, self.to_dict())).start()

        if self.batch_id:
            batch_id_batch[self.batch_id].mark_job_complete(self)

    def to_dict(self):
        result = {
            'id': self.id,
            'state': self._state
        }
        if self._state == 'Complete':
            result['exit_code'] = self.exit_code

        logs = self._read_logs()
        if logs is not None:
            result['log'] = logs

        if self.attributes:
            result['attributes'] = self.attributes
        if self.parent_ids:
            result['parent_ids'] = self.parent_ids
        if self.scratch_folder:
            result['scratch_folder'] = self.scratch_folder
        return result


@routes.post('/jobs/create')
async def create_job(request):  # pylint: disable=R0912
    parameters = await request.json()

    schema = {
        # will be validated when creating pod
        'spec': schemas.pod_spec,
        'copy_service_account_name': {'type': 'string'},
        'batch_id': {'type': 'integer'},
        'parent_ids': {'type': 'list', 'schema': {'type': 'integer'}},
        'scratch_folder': {'type': 'string'},
        'input_files': {
            'type': 'list',
            'schema': {'type': 'list', 'items': 2 * ({'type': 'string'},)}},
        'output_files': {
            'type': 'list',
            'schema': {'type': 'list', 'items': 2 * ({'type': 'string'},)}},
        'always_run': {'type': 'boolean'},
        'attributes': {
            'type': 'dict',
            'keyschema': {'type': 'string'},
            'valueschema': {'type': 'string'}
        },
        'callback': {'type': 'string'}
    }
    validator = cerberus.Validator(schema)
    if not validator.validate(parameters):
        abort(400, 'invalid request: {}'.format(validator.errors))

    pod_spec = v1.api_client._ApiClient__deserialize(
        parameters['spec'], kube.client.V1PodSpec)

    batch_id = parameters.get('batch_id')
    if batch_id:
        batch = batch_id_batch.get(batch_id)
        if batch is None:
            abort(404, f'invalid request: batch_id {batch_id} not found')
        if not batch.is_open:
            abort(400, f'invalid request: batch_id {batch_id} is closed')

    parent_ids = parameters.get('parent_ids', [])
    for parent_id in parent_ids:
        parent_job = job_id_job.get(parent_id, None)
        if parent_job is None:
            abort(400, f'invalid parent_id: no job with id {parent_id}')
        if parent_job.batch_id != batch_id or parent_job.batch_id is None or batch_id is None:
            abort(400,
                  f'invalid parent batch: {parent_id} is in batch '
                  f'{parent_job.batch_id} but child is in {batch_id}')

    scratch_folder = parameters.get('scratch_folder')
    input_files = parameters.get('input_files')
    output_files = parameters.get('output_files')
    copy_service_account_name = parameters.get('copy_service_account_name')
    always_run = parameters.get('always_run', False)

    if len(pod_spec.containers) != 1:
        abort(400, f'only one container allowed in pod_spec {pod_spec}')

    if pod_spec.containers[0].name != 'main':
        abort(400, f'container name must be "main" was {pod_spec.containers[0].name}')

    if not both_or_neither(input_files is None and output_files is None,
                           copy_service_account_name is None):
        abort(400,
              f'invalid request: if either input_files or ouput_files is set, '
              f'then the service account must be specified; otherwise the '
              f'service account must not be specified. input_files: {input_files}, '
              f'output_files: {output_files}, copy_service_account_name: '
              f'{copy_service_account_name}')

    job = await Job(
        pod_spec,
        batch_id,
        parameters.get('attributes'),
        parameters.get('callback'),
        parent_ids,
        scratch_folder,
        input_files,
        output_files,
        copy_service_account_name,
        always_run)
    return jsonify(job.to_dict())


def both_or_neither(x, y):  # pylint: disable=C0103
    assert isinstance(x, bool)
    assert isinstance(y, bool)
    return x == y


@routes.get('/jobs')
async def get_job_list(request):  # pylint: disable=W0613
    # job = await Job.from_sql(job_id)
    return jsonify([job.to_dict() for _, job in job_id_job.items()])


@routes.get('/jobs/{job_id}')
async def get_job(request):
    job_id = int(request.match_info['job_id'])

    # job_exists = await db.jobs.has_record(job_id)
    # if not job_exists:
    #     abort(404)

    # if job_id in job_id_job:
    #     json
    # else:
    #     await Job.from_sql(job_id)
    # print("here")
    job = await Job.from_sql(job_id)
    # job = job_id_job.get(job_id)
    if not job:
        abort(404)
    return jsonify(job.to_dict())


@routes.get('/jobs/{job_id}/log')
async def get_job_log(request):  # pylint: disable=R1710
    job_id = int(request.match_info['job_id'])

    # job_exists = await db.jobs.has_record(job_id)
    # if not job_exists:
    #     abort(404)

    job = await Job.from_sql(job_id)
    # job = job_id_job.get(job_id)
    if job:
        job_log = job._read_logs()
        if job_log:
            return jsonify(job_log)
    else:
        logs = {}
        for task_name in ['input', 'main', 'output']:
            fname = _log_path(job_id, task_name)
            if os.path.exists(fname):
                logs[task_name] = _read_file(fname)
        if logs:
            return jsonify(logs)
    abort(404)


@routes.delete('/jobs/{job_id}/delete')
async def delete_job(request):
    job_id = int(request.match_info['job_id'])

    # job_exists = await db.jobs.has_record(job_id)
    # if not job_exists:
    #     abort(404)

    job = await Job.from_sql(job_id)
    # job = job_id_job.get(job_id)
    if not job:
        abort(404)
    await job.delete()
    return jsonify({})


@routes.post('/jobs/{job_id}/cancel')
async def cancel_job(request):
    job_id = int(request.match_info['job_id'])

    # job_exists = await db.jobs.has_record(job_id)
    # if not job_exists:
    #     abort(404)

    job = await Job.from_sql(job_id)
    # job = job_id_job.get(job_id)
    if not job:
        abort(404)
    await job.cancel()
    return jsonify({})


class Batch:
    MAX_TTL = 30 * 60

    def __init__(self, attributes, callback, ttl):
        self.attributes = attributes
        self.callback = callback
        self.id = next_id()
        batch_id_batch[self.id] = self
        self.jobs = set([])
        self.is_open = True
        if ttl is None or ttl > Batch.MAX_TTL:
            ttl = Batch.MAX_TTL
        self.ttl = ttl
        schedule(self.ttl, self.close)

    def delete(self):
        del batch_id_batch[self.id]
        for j in self.jobs:
            assert j.batch_id == self.id
            j.batch_id = None

    def remove(self, job):
        self.jobs.remove(job)

    def mark_job_complete(self, job):
        assert job in self.jobs
        if self.callback:
            def handler(id, job_id, callback, json):
                try:
                    requests.post(callback, json=json, timeout=120)
                except requests.exceptions.RequestException as exc:
                    log.warning(
                        f'callback for batch {id}, job {job_id} failed due to an error, I will not retry. '
                        f'Error: {exc}')

            threading.Thread(
                target=handler,
                args=(self.id, job.id, self.callback, job.to_dict())
            ).start()

    def close(self):
        if self.is_open:
            log.info(f'closing batch {self.id}, ttl was {self.ttl}')
            self.is_open = False
        else:
            log.info(f're-closing batch {self.id}, ttl was {self.ttl}')

    def to_dict(self):
        state_count = Counter([j._state for j in self.jobs])
        return {
            'id': self.id,
            'jobs': {
                'Created': state_count.get('Created', 0),
                'Complete': state_count.get('Complete', 0),
                'Cancelled': state_count.get('Cancelled', 0)
            },
            'exit_codes': {j.id: j.exit_code for j in self.jobs},
            'attributes': self.attributes,
            'is_open': self.is_open
        }


@routes.post('/batches/create')
async def create_batch(request):
    parameters = await request.json()

    schema = {
        'attributes': {
            'type': 'dict',
            'keyschema': {'type': 'string'},
            'valueschema': {'type': 'string'}
        },
        'callback': {'type': 'string'},
        'ttl': {'type': 'number'}
    }
    validator = cerberus.Validator(schema)
    if not validator.validate(parameters):
        abort(400, 'invalid request: {}'.format(validator.errors))

    batch = Batch(parameters.get('attributes'), parameters.get('callback'), parameters.get('ttl'))
    return jsonify(batch.to_dict())


@routes.get('/batches/{batch_id}')
async def get_batch(request):
    batch_id = int(request.match_info['batch_id'])
    batch = batch_id_batch.get(batch_id)
    if not batch:
        abort(404)
    return jsonify(batch.to_dict())


@routes.delete('/batches/{batch_id}/delete')
async def delete_batch(request):
    batch_id = int(request.match_info['batch_id'])
    batch = batch_id_batch.get(batch_id)
    if not batch:
        abort(404)
    batch.delete()
    return jsonify({})


@routes.post('/batches/{batch_id}/close')
async def close_batch(request):
    batch_id = int(request.match_info['batch_id'])
    batch = batch_id_batch.get(batch_id)
    if not batch:
        abort(404)
    batch.close()
    return jsonify({})


async def update_job_with_pod(job, pod):
    if pod:
        if pod.status.container_statuses:
            assert len(pod.status.container_statuses) == 1
            container_status = pod.status.container_statuses[0]
            assert container_status.name in ['input', 'main', 'output']

            if container_status.state and container_status.state.terminated:
                await job.mark_complete(pod)
    else:
        await job.mark_unscheduled()


@routes.post('/pod_changed')
async def pod_changed(request):
    parameters = await request.json()

    pod_name = parameters['pod_name']

    job = pod_name_job.get(pod_name)

    if job and not job.is_complete():
        try:
            pod = v1.read_namespaced_pod(
                pod_name,
                POD_NAMESPACE,
                _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        except kube.client.rest.ApiException as exc:
            if exc.status == 404:
                pod = None
            else:
                raise

        await update_job_with_pod(job, pod)

    return web.Response(status=204)


@routes.post('/refresh_k8s_state')
async def refresh_k8s_state(request):  # pylint: disable=W0613
    log.info('started k8s state refresh')

    pods = v1.list_namespaced_pod(
        POD_NAMESPACE,
        label_selector=f'app=batch-job,hail.is/batch-instance={instance_id}',
        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

    seen_pods = set()
    for pod in pods.items:
        pod_name = pod.metadata.name
        seen_pods.add(pod_name)

        job = pod_name_job.get(pod_name)
        if job and not job.is_complete():
            await update_job_with_pod(job, pod)

    for pod_name, job in pod_name_job.items():
        if pod_name not in seen_pods:
            await update_job_with_pod(job, None)

    log.info('k8s state refresh complete')

    return web.Response(status=204)


@routes.get('/recent')
@aiohttp_jinja2.template('recent.html')
async def recent(request):  # pylint: disable=W0613
    recent_events = get_recent_events()
    return {'recent': list(reversed(recent_events))}


def run_forever(target, *args, **kwargs):
    expected_retry_interval_ms = 15 * 1000

    while True:
        start = time.time()
        run_once(target, *args, **kwargs)
        end = time.time()

        run_time_ms = int((end - start) * 1000 + 0.5)

        sleep_duration_ms = random.randrange(expected_retry_interval_ms * 2) - run_time_ms
        if sleep_duration_ms > 0:
            log.debug(f'run_forever: {target.__name__}: sleep {sleep_duration_ms}ms')
            time.sleep(sleep_duration_ms / 1000.0)


def run_once(target, *args, **kwargs):
    try:
        log.info(f'run_forever: {target.__name__}')
        target(*args, **kwargs)
        log.info(f'run_forever: {target.__name__} returned')
    except Exception:  # pylint: disable=W0703
        log.error(f'run_forever: {target.__name__} caught_exception: ', exc_info=sys.exc_info())


def aiohttp_event_loop(port):
    app.add_routes(routes)
    web.run_app(app, host='0.0.0.0', port=port)


def kube_event_loop(port):
    # May not be thread-safe; opens http connection, so use local version
    v1_ = kube.client.CoreV1Api()

    watch = kube.watch.Watch()
    stream = watch.stream(
        v1_.list_namespaced_pod,
        POD_NAMESPACE,
        label_selector=f'app=batch-job,hail.is/batch-instance={instance_id}')
    for event in stream:
        pod = event['object']
        name = pod.metadata.name
        requests.post(f'http://127.0.0.1:{port}/pod_changed', json={'pod_name': name}, timeout=120)


def polling_event_loop(port):
    time.sleep(1)
    while True:
        try:
            response = requests.post(f'http://127.0.0.1:{port}/refresh_k8s_state', timeout=120)
            response.raise_for_status()
        except requests.HTTPError as exc:
            log.error(f'Could not poll due to exception: {exc}, text: {exc.response.text}')
        except Exception as exc:  # pylint: disable=W0703
            log.error(f'Could not poll due to exception: {exc}')
        time.sleep(REFRESH_INTERVAL_IN_SECONDS)


def scheduling_loop():
    while True:
        try:
            s.run(blocking=False)
        except Exception as exc:  # pylint: disable=W0703
            log.error(f'Could not run scheduled jobs due to: {exc}')


def serve(db_config_path, port=5000):
    kube_thread = threading.Thread(target=run_forever, args=(kube_event_loop, port))
    kube_thread.start()

    polling_thread = threading.Thread(target=run_forever, args=(polling_event_loop, port))
    polling_thread.start()

    scheduling_thread = threading.Thread(target=run_forever, args=(scheduling_loop,))
    scheduling_thread.start()

    # debug/reloader must run in main thread
    # see: https://stackoverflow.com/questions/31264826/start-a-flask-application-in-separate-thread
    # flask_thread = threading.Thread(target=flask_event_loop)
    # flask_thread.start()
    run_forever(aiohttp_event_loop, port)

    kube_thread.join()
