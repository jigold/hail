import sys
import os
import time
import random
import sched
import uuid
from collections import Counter
import logging
import threading
from flask import Flask, request, jsonify, abort, render_template
import kubernetes as kube
import cerberus
import requests

from .globals import max_id, pod_name_job_task, job_id_job, _log_path, _read_file, batch_id_batch
from .globals import next_id, get_recent_events, add_event

from .. import schemas

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

log.info(f'KUBERNETES_TIMEOUT_IN_SECONDS {KUBERNETES_TIMEOUT_IN_SECONDS}')
log.info(f'REFRESH_INTERVAL_IN_SECONDS {REFRESH_INTERVAL_IN_SECONDS}')

if 'BATCH_USE_KUBE_CONFIG' in os.environ:
    kube.config.load_kube_config()
else:
    kube.config.load_incluster_config()
v1 = kube.client.CoreV1Api()

instance_id = uuid.uuid4().hex
log.info(f'instance_id = {instance_id}')


class JobTask:
    @staticmethod
    def copy_task(job_id, task_name, files):
        container = kube.client.V1Container(image='alpine',
                                            name=task_name,
                                            command=['echo', 'hello'])

        spec = kube.client.V1PodSpec(containers=[container],
                                     restart_policy='Never')

        if files:  # pylint: disable=R1705
            return JobTask(job_id, task_name, spec)
        else:
            return None

    def __init__(self, job, name, pod_spec):
        assert pod_spec is not None

        self.job = job
        self.job_id = self.job.id
        self.name = name

        metadata = kube.client.V1ObjectMeta(generate_name='job-{}-{}-'.format(self.job_id, name),
                                            labels={
                                                'app': 'batch-job',
                                                'hail.is/batch-instance': instance_id,
                                                'uuid': uuid.uuid4().hex
                                            })

        self.pod_template = kube.client.V1Pod(metadata=metadata,
                                              spec=pod_spec)

        self._pod_name = None
        self._state = 'Created'
        self.exit_code = None
        add_event({'message': f'created job {self.job_id}, {name} task',
                   'command': f'{pod_spec.containers[0].command}'})

    def set_state(self, state):
        self._state = state

    def is_complete(self):
        return self._state == 'Complete' or self._state == 'Cancelled'

    def create_pod(self):
        assert not self._pod_name

        pod = v1.create_namespaced_pod(
            POD_NAMESPACE,
            self.pod_template,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
        self._pod_name = pod.metadata.name
        pod_name_job_task[self._pod_name] = self

        log.info('created pod name: {} for job {}, {} task'.format(self._pod_name, self.job_id, self.name))
        return self._pod_name

    def delete_pod(self):
        self.set_state('Cancelled')
        if self._pod_name:
            try:
                v1.delete_namespaced_pod(
                    self._pod_name,
                    POD_NAMESPACE,
                    kube.client.V1DeleteOptions(),
                    _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
            except kube.client.rest.ApiException as err:
                if err.status == 404:
                    pass
                else:
                    raise
            del pod_name_job_task[self._pod_name]
            self._pod_name = None

    def read_log(self):
        if self._state == 'Created':
            if self._pod_name:
                try:
                    return v1.read_namespaced_pod_log(
                        self._pod_name,
                        POD_NAMESPACE,
                        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)
                except kube.client.rest.ApiException:
                    pass
            return None
        if self._state == 'Complete':
            return _read_file(_log_path(self.job_id, self.name))
        assert self._state == 'Cancelled'
        return None

    def mark_unscheduled(self):
        if self._pod_name:
            del pod_name_job_task[self._pod_name]
            self._pod_name = None
        self.create_pod()

    def mark_complete(self, pod):
        self.exit_code = pod.status.container_statuses[0].state.terminated.exit_code

        pod_log = v1.read_namespaced_pod_log(
            pod.metadata.name,
            POD_NAMESPACE,
            _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

        add_event({'message': f'job {self.job_id}, {self.name} task exited', 'log': pod_log[:64000]})

        fname = _log_path(self.job_id, self.name)
        with open(fname, 'w') as f:
            f.write(pod_log)
        log.info(f'wrote log for job {self.job_id}, {self.name} task to {fname}')

        if self._pod_name:
            del pod_name_job_task[self._pod_name]
            self._pod_name = None

        self.set_state('Complete')
        self.job.mark_complete()


class Job:
    def _next_task(self):
        self._task_idx += 1
        if self._task_idx < len(self._tasks):  # pylint: disable=R1705
            self._current_task = self._tasks[self._task_idx]
            return self._current_task
        else:
            return None

    def _read_logs(self):
        logs = {}
        for task in self._tasks:
            log = task.read_log()
            if log is not None:
                logs[task.name] = log
        if logs:  # pylint: disable=R1705
            return logs
        else:
            return None

    def __init__(self, pod_spec, batch_id, attributes, callback, parent_ids,
                 scratch_folder, input_files, output_files):
        self.id = next_id()
        self.batch_id = batch_id
        self.attributes = attributes
        self.callback = callback
        self.child_ids = set([])
        self.parent_ids = parent_ids
        self.incomplete_parent_ids = set(self.parent_ids)
        self.scratch_folder = scratch_folder
        self.exit_code = None
        self._state = 'Created'

        self._tasks = [JobTask.copy_task(self, 'input', input_files),
                       JobTask(self, 'main', pod_spec),
                       JobTask.copy_task(self, 'output', output_files)]

        self._tasks = [t for t in self._tasks if t is not None]
        self._task_idx = -1
        self._current_task = self._next_task()
        assert self._current_task is not None

        job_id_job[self.id] = self

        for parent in self.parent_ids:
            job_id_job[parent].child_ids.add(self.id)

        if batch_id:
            batch = batch_id_batch[batch_id]
            batch.jobs.add(self)

        log.info('created job {}'.format(self.id))
        add_event({'message': f'created job {self.id}'})

        if not self.parent_ids:
            self._current_task.create_pod()
        else:
            self.refresh_parents_and_maybe_create()

    def refresh_parents_and_maybe_create(self):
        for parent in self.parent_ids:
            parent_job = job_id_job[parent]
            self.parent_new_state(parent_job._state, parent, parent_job.exit_code)

    def set_state(self, new_state):
        if self._state != new_state:
            log.info('job {} changed state: {} -> {}'.format(
                self.id,
                self._state,
                new_state))
            self._state = new_state

            for task in self._tasks[self._task_idx:]:
                task.set_state(new_state)

            self.notify_children(new_state)

    def notify_children(self, new_state):
        for child_id in self.child_ids:
            child = job_id_job.get(child_id)
            if child:
                child.parent_new_state(new_state, self.id, self.exit_code)
            else:
                log.info(f'missing child: {child_id}')

    def parent_new_state(self, new_state, parent_id, maybe_exit_code):
        if new_state == 'Complete' and maybe_exit_code == 0:
            log.info(f'parent {parent_id} successfully complete for {self.id}')
            self.incomplete_parent_ids.discard(parent_id)
            if not self.incomplete_parent_ids:
                assert self._state in ('Cancelled', 'Created'), f'bad state: {self._state}'
                if self._state != 'Cancelled':
                    log.info(f'all parents successfully complete for {self.id},'
                             f' creating pod')
                    self._current_task.create_pod()
                else:
                    log.info(f'all parents successfully complete for {self.id},'
                             f' but it is already cancelled')
        elif new_state == 'Cancelled' or (new_state == 'Complete' and maybe_exit_code != 0):
            log.info(f'parents deleted, cancelled, or failed: {new_state} {maybe_exit_code} {parent_id}')
            self.incomplete_parent_ids.discard(parent_id)
            self.cancel()

    def cancel(self):
        if self.is_complete():
            return
        self._current_task.delete_pod()
        self.set_state('Cancelled')

    def delete(self):
        # remove from structures
        del job_id_job[self.id]
        if self.batch_id:
            batch = batch_id_batch[self.batch_id]
            batch.remove(self)

        self._current_task.delete_pod()
        self.set_state('Cancelled')

    def is_complete(self):
        return self._state == 'Complete' or self._state == 'Cancelled'

    def mark_complete(self):
        self.exit_code = self._current_task.exit_code
        if self.exit_code == 0 and self._next_task() is not None:
            self._current_task.create_pod()
            return

        self.set_state('Complete')

        log.info('job {} complete, exit_code {}'.format(self.id, self.exit_code))

        if self.callback:
            def handler(id, callback, json):
                try:
                    requests.post(callback, json=json, timeout=120)
                except requests.exceptions.RequestException as exc:
                    log.warning(
                        f'callback for job {id} failed due to an error, I will not retry. '
                        f'Error: {exc}')

            threading.Thread(target=handler, args=(self.id, self.callback, self.to_json())).start()

        if self.batch_id:
            batch_id_batch[self.batch_id].mark_job_complete(self)

    def to_json(self):
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


app = Flask('batch')

log.info(f'app.root_path = {app.root_path}')


@app.route('/jobs/create', methods=['POST'])
def create_job():  # pylint: disable=R0912
    parameters = request.json

    schema = {
        # will be validated when creating pod
        'spec': schemas.pod_spec,
        'batch_id': {'type': 'integer'},
        'parent_ids': {'type': 'list', 'schema': {'type': 'integer'}},
        'scratch_folder': {'type': 'string'},
        'input_files': {'type': 'list', 'schema': {'type': 'string'}},
        'output_files': {'type': 'list', 'schema': {'type': 'string'}},
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

    if len(pod_spec.containers) != 1:
        abort(400, f'only one container allowed in pod_spec {pod_spec}')

    if pod_spec.containers[0].name != 'default':
        abort(400, f'container name must be "default" was {pod_spec.containers[0].name}')

    job = Job(
        pod_spec,
        batch_id,
        parameters.get('attributes'),
        parameters.get('callback'),
        parent_ids,
        scratch_folder,
        input_files,
        output_files)
    return jsonify(job.to_json())


@app.route('/jobs', methods=['GET'])
def get_job_list():
    return jsonify([job.to_json() for _, job in job_id_job.items()])


@app.route('/jobs/<int:job_id>', methods=['GET'])
def get_job(job_id):
    job = job_id_job.get(job_id)
    if not job:
        abort(404)
    return jsonify(job.to_json())


@app.route('/jobs/<int:job_id>/log', methods=['GET'])
def get_job_log(job_id):  # pylint: disable=R1710
    if job_id > max_id():
        abort(404)

    job = job_id_job.get(job_id)
    if job:
        job_log = job._read_logs()
        if job_log:
            return jsonify(job_log)
    else:
        logs = {}
        for job_task_name in ['input', 'main', 'output']:
            fname = _log_path(job_id, job_task_name)
            if os.path.exists(fname):
                logs[job_task_name] = _read_file(fname)
        if logs:
            return jsonify(logs)
    abort(404)


@app.route('/jobs/<int:job_id>/delete', methods=['DELETE'])
def delete_job(job_id):
    job = job_id_job.get(job_id)
    if not job:
        abort(404)
    job.delete()
    return jsonify({})


@app.route('/jobs/<int:job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    job = job_id_job.get(job_id)
    if not job:
        abort(404)
    job.cancel()
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
                args=(self.id, job.id, self.callback, job.to_json())
            ).start()

    def close(self):
        if self.is_open:
            log.info(f'closing batch {self.id}, ttl was {self.ttl}')
            self.is_open = False
        else:
            log.info(f're-closing batch {self.id}, ttl was {self.ttl}')

    def to_json(self):
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


@app.route('/batches/create', methods=['POST'])
def create_batch():
    parameters = request.json

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
    return jsonify(batch.to_json())


@app.route('/batches/<int:batch_id>', methods=['GET'])
def get_batch(batch_id):
    batch = batch_id_batch.get(batch_id)
    if not batch:
        abort(404)
    return jsonify(batch.to_json())


@app.route('/batches/<int:batch_id>/delete', methods=['DELETE'])
def delete_batch(batch_id):
    batch = batch_id_batch.get(batch_id)
    if not batch:
        abort(404)
    batch.delete()
    return jsonify({})


@app.route('/batches/<int:batch_id>/close', methods=['POST'])
def close_batch(batch_id):
    batch = batch_id_batch.get(batch_id)
    if not batch:
        abort(404)
    batch.close()
    return jsonify({})


def update_job_task_with_pod(job_task, pod):
    if pod:
        if pod.status.container_statuses:
            assert len(pod.status.container_statuses) == 1
            container_status = pod.status.container_statuses[0]
            assert container_status.name in ['input', 'default', 'output']

            if container_status.state and container_status.state.terminated:
                job_task.mark_complete(pod)
    else:
        job_task.mark_unscheduled()


@app.route('/pod_changed', methods=['POST'])
def pod_changed():
    parameters = request.json

    pod_name = parameters['pod_name']

    job_task = pod_name_job_task.get(pod_name)
    if job_task and not job_task.is_complete():
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

        update_job_task_with_pod(job_task, pod)

    return '', 204


@app.route('/refresh_k8s_state', methods=['POST'])
def refresh_k8s_state():
    log.info('started k8s state refresh')

    pods = v1.list_namespaced_pod(
        POD_NAMESPACE,
        label_selector=f'app=batch-job,hail.is/batch-instance={instance_id}',
        _request_timeout=KUBERNETES_TIMEOUT_IN_SECONDS)

    seen_pods = set()
    for pod in pods.items:
        pod_name = pod.metadata.name
        seen_pods.add(pod_name)

        job = pod_name_job_task.get(pod_name)
        if job and not job.is_complete():
            update_job_task_with_pod(job, pod)

    for pod_name, job in pod_name_job_task.items():
        if pod_name not in seen_pods:
            update_job_task_with_pod(job, None)

    log.info('k8s state refresh complete')

    return '', 204


@app.route('/recent', methods=['GET'])
def recent():
    recent_events = get_recent_events()
    return render_template('recent.html', recent=list(reversed(recent_events)))


def run_forever(target, *args, **kwargs):
    # target should be a function
    target_name = target.__name__

    expected_retry_interval_ms = 15 * 1000
    while True:
        start = time.time()
        try:
            log.info(f'run_forever: run target {target_name}')
            target(*args, **kwargs)
            log.info(f'run_forever: target {target_name} returned')
        except Exception:  # pylint: disable=W0703
            log.error(f'run_forever: target {target_name} threw exception', exc_info=sys.exc_info())
        end = time.time()

        run_time_ms = int((end - start) * 1000 + 0.5)
        sleep_duration_ms = random.randrange(expected_retry_interval_ms * 2) - run_time_ms
        if sleep_duration_ms > 0:
            log.debug(f'run_forever: {target_name}: sleep {sleep_duration_ms}ms')
            time.sleep(sleep_duration_ms / 1000.0)


def flask_event_loop(port):
    app.run(threaded=False, host='0.0.0.0', port=port)


def kube_event_loop(port):
    watch = kube.watch.Watch()
    stream = watch.stream(
        v1.list_namespaced_pod,
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


def serve(port=5000):
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
    run_forever(flask_event_loop, port)

    kube_thread.join()
