from typing import Dict, Tuple, List
import os
import json
import sys
import logging
import asyncio
import random
import signal
import aiohttp
import aiohttp.client_exceptions
from aiohttp import web
import concurrent.futures

from collections import defaultdict

from gear.clients import get_compute_client, get_cloud_async_fs

from hailtop.utils import (
    time_msecs,
    request_retry_transient_errors,
    retry_all_errors,
    dump_all_stacktraces,
    periodically_call,
)
from hailtop.aiotools.router_fs import RouterAsyncFS
from hailtop.aiotools import LocalAsyncFS
from hailtop import aiotools, httpx

# import uvloop

from hailtop.config import DeployConfig

from ..semaphore import FIFOWeightedSemaphore
from ..file_store import FileStore
from ..globals import HTTP_CLIENT_MAX_SIZE
from ..batch_format_version import BatchFormatVersion
from ..utils import Box

from .config import (
    NAMESPACE,
    CORES,
    UNRESERVED_WORKER_DATA_DISK_SIZE_GB,
    BATCH_WORKER_IMAGE_ID,
    instance_config,
    CLOUD_WORKER_API,
    MAX_IDLE_TIME_MSECS,
    NAME,
    BATCH_LOGS_STORAGE_URI,
    INSTANCE_ID,
)
from .network import get_network_allocator
from .images import ImageManager, get_image_manager
from .job import Job, JVMJob, DockerJob
from .jvm import JVM
from .utils import user_error

log = logging.getLogger('worker')

deploy_config = DeployConfig('gce', NAMESPACE, {})


class Worker:
    def __init__(self, client_session: httpx.ClientSession, image_manager: ImageManager):
        self.active = False
        self.cores_mcpu = CORES * 1000
        self.last_updated = time_msecs()
        self.cpu_sem = FIFOWeightedSemaphore(self.cores_mcpu)
        self.data_disk_space_remaining = Box(UNRESERVED_WORKER_DATA_DISK_SIZE_GB)
        self.pool = concurrent.futures.ThreadPoolExecutor()
        self.jobs: Dict[Tuple[int, int], Job] = {}
        self.stop_event = asyncio.Event()
        self.task_manager = aiotools.BackgroundTaskManager()
        os.mkdir('/hail-jars/')
        self.jar_download_locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self.client_session = client_session

        self.image_manager = image_manager
        self.image_manager.image_data[BATCH_WORKER_IMAGE_ID] += 1

        # filled in during activation
        self.fs = None
        self.file_store = None
        self.headers = None
        self.compute_client = None

        self._jvm_initializer_task = asyncio.ensure_future(self._initialize_jvms())
        self._jvms: List[JVM] = []

    async def _initialize_jvms(self):
        if instance_config.worker_type() in ('standard', 'D'):
            self._jvms = await asyncio.gather(*[JVM.create(i, self.pool) for i in range(CORES)])
        log.info(f'JVMs initialized {self._jvms}')

    async def borrow_jvm(self) -> JVM:
        if instance_config.worker_type() not in ('standard', 'D'):
            raise ValueError(f'JVM jobs not allowed on {instance_config.worker_type()}')
        await self._jvm_initializer_task
        assert self._jvms
        return self._jvms.pop()

    def return_jvm(self, jvm: JVM):
        if instance_config.worker_type() not in ('standard', 'D'):
            raise ValueError(f'JVM jobs not allowed on {instance_config.worker_type()}')
        jvm.reset()
        self._jvms.append(jvm)

    async def shutdown(self):
        log.info('Worker.shutdown')
        try:
            self.task_manager.shutdown()
            log.info('shutdown task manager')
        finally:
            try:
                if self.fs:
                    await self.fs.close()
                    log.info('closed worker file system')
            finally:
                try:
                    if self.compute_client:
                        await self.compute_client.close()
                        log.info('closed compute client')
                finally:
                    try:
                        if self.file_store:
                            await self.file_store.close()
                            log.info('closed file store')
                    finally:
                        await self.client_session.close()
                        log.info('closed client session')

    async def run_job(self, job):
        try:
            await job.run()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if not user_error(e):
                log.exception(f'while running {job}, ignoring')

    async def create_job_1(self, request):
        body = await request.json()

        batch_id = body['batch_id']
        job_id = body['job_id']

        format_version = BatchFormatVersion(body['format_version'])

        token = body['token']
        start_job_id = body['start_job_id']
        addtl_spec = body['job_spec']

        job_spec = await self.file_store.read_spec_file(batch_id, token, start_job_id, job_id)
        job_spec = json.loads(job_spec)

        job_spec['attempt_id'] = addtl_spec['attempt_id']
        job_spec['secrets'] = addtl_spec['secrets']

        addtl_env = addtl_spec.get('env')
        if addtl_env:
            env = job_spec.get('env')
            if not env:
                env = []
                job_spec['env'] = env
            env.extend(addtl_env)

        assert job_spec['job_id'] == job_id
        id = (batch_id, job_id)

        # already running
        if id in self.jobs:
            return web.HTTPForbidden()

        # check worker hasn't started shutting down
        if not self.active:
            return web.HTTPServiceUnavailable()

        credentials = CLOUD_WORKER_API.user_credentials(body['gsa_key'])

        type = job_spec['process']['type']
        if type == 'docker':
            job = DockerJob(
                batch_id,
                body['user'],
                credentials,
                job_spec,
                format_version,
                self.task_manager,
                self.pool,
                self.client_session,
                self,
            )
        else:
            assert type == 'jvm'
            job = JVMJob(
                batch_id, body['user'], credentials, job_spec, format_version, self.task_manager, self.pool, self
            )

        log.info(f'created {job}, adding to jobs')

        self.jobs[job.id] = job

        self.task_manager.ensure_future(self.run_job(job))

        return web.Response()

    async def create_job(self, request):
        return await asyncio.shield(self.create_job_1(request))

    async def get_job_log(self, request):
        batch_id = int(request.match_info['batch_id'])
        job_id = int(request.match_info['job_id'])
        id = (batch_id, job_id)
        job = self.jobs.get(id)
        if not job:
            raise web.HTTPNotFound()
        return web.json_response(await job.get_log())

    async def get_job_status(self, request):
        batch_id = int(request.match_info['batch_id'])
        job_id = int(request.match_info['job_id'])
        id = (batch_id, job_id)
        job = self.jobs.get(id)
        if not job:
            raise web.HTTPNotFound()
        return web.json_response(await job.status())

    async def delete_job_1(self, request):
        batch_id = int(request.match_info['batch_id'])
        job_id = int(request.match_info['job_id'])
        id = (batch_id, job_id)

        log.info(f'deleting job {id}, removing from jobs')

        job = self.jobs.pop(id, None)
        if job is None:
            raise web.HTTPNotFound()

        self.last_updated = time_msecs()

        self.task_manager.ensure_future(job.delete())

        return web.Response()

    async def delete_job(self, request):
        return await asyncio.shield(self.delete_job_1(request))

    async def healthcheck(self, request):  # pylint: disable=unused-argument
        body = {'name': NAME}
        return web.json_response(body)

    async def run(self):
        app = web.Application(client_max_size=HTTP_CLIENT_MAX_SIZE)
        app.add_routes(
            [
                web.post('/api/v1alpha/kill', self.kill),
                web.post('/api/v1alpha/batches/jobs/create', self.create_job),
                web.delete('/api/v1alpha/batches/{batch_id}/jobs/{job_id}/delete', self.delete_job),
                web.get('/api/v1alpha/batches/{batch_id}/jobs/{job_id}/log', self.get_job_log),
                web.get('/api/v1alpha/batches/{batch_id}/jobs/{job_id}/status', self.get_job_status),
                web.get('/healthcheck', self.healthcheck),
            ]
        )

        try:
            await asyncio.wait_for(self.activate(), MAX_IDLE_TIME_MSECS / 1000)
        except asyncio.TimeoutError:
            log.exception(f'could not activate after trying for {MAX_IDLE_TIME_MSECS} ms, exiting')
            return

        app_runner = web.AppRunner(app)
        await app_runner.setup()
        site = web.TCPSite(app_runner, '0.0.0.0', 5000)
        await site.start()

        self.task_manager.ensure_future(periodically_call(60, self.image_manager.cleanup_old_images, self.pool))
        try:
            while True:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), 15)
                    log.info('received stop event')
                    break
                except asyncio.TimeoutError:
                    idle_duration = time_msecs() - self.last_updated
                    if not self.jobs and idle_duration >= MAX_IDLE_TIME_MSECS:
                        log.info(f'idle {idle_duration} ms, exiting')
                        break
                    log.info(
                        f'n_jobs {len(self.jobs)} free_cores {self.cpu_sem.value / 1000} idle {idle_duration} '
                        f'free worker data disk storage {self.data_disk_space_remaining.value}Gi'
                    )
        finally:
            self.active = False
            log.info('shutting down')
            await site.stop()
            log.info('stopped site')
            await app_runner.cleanup()
            log.info('cleaned up app runner')
            await self.deactivate()
            log.info('deactivated')

    async def deactivate(self):
        # Don't retry.  If it doesn't go through, the driver
        # monitoring loops will recover.  If the driver is
        # gone (e.g. testing a PR), this would go into an
        # infinite loop and the instance won't be deleted.
        await self.client_session.post(
            deploy_config.url('batch-driver', '/api/v1alpha/instances/deactivate'), headers=self.headers
        )

    async def kill_1(self, request):  # pylint: disable=unused-argument
        log.info('killed')
        self.stop_event.set()

    async def kill(self, request):
        return await asyncio.shield(self.kill_1(request))

    async def post_job_complete_1(self, job):
        run_duration = job.end_time - job.start_time

        full_status = await retry_all_errors(f'error while getting status for {job}')(job.status)

        if job.format_version.has_full_status_in_gcs():
            await retry_all_errors(f'error while writing status file to gcs for {job}')(
                self.file_store.write_status_file, job.batch_id, job.job_id, job.attempt_id, json.dumps(full_status)
            )

        db_status = job.format_version.db_status(full_status)

        status = {
            'version': full_status['version'],
            'batch_id': full_status['batch_id'],
            'job_id': full_status['job_id'],
            'attempt_id': full_status['attempt_id'],
            'state': full_status['state'],
            'start_time': full_status['start_time'],
            'end_time': full_status['end_time'],
            'resources': full_status['resources'],
            'status': db_status,
        }

        body = {'status': status}

        start_time = time_msecs()
        delay_secs = 0.1
        while True:
            try:
                await self.client_session.post(
                    deploy_config.url('batch-driver', '/api/v1alpha/instances/job_complete'),
                    json=body,
                    headers=self.headers,
                )
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if isinstance(e, aiohttp.ClientResponseError) and e.status == 404:  # pylint: disable=no-member
                    raise
                log.warning(f'failed to mark {job} complete, retrying', exc_info=True)

            # unlist job after 3m or half the run duration
            now = time_msecs()
            elapsed = now - start_time
            if job.id in self.jobs and elapsed > 180 * 1000 and elapsed > run_duration / 2:
                log.info(f'too much time elapsed marking {job} complete, removing from jobs, will keep retrying')
                del self.jobs[job.id]
                self.last_updated = time_msecs()

            await asyncio.sleep(delay_secs * random.uniform(0.7, 1.3))
            # exponentially back off, up to (expected) max of 2m
            delay_secs = min(delay_secs * 2, 2 * 60.0)

    async def post_job_complete(self, job):
        try:
            await self.post_job_complete_1(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(f'error while marking {job} complete', stack_info=True)
        finally:
            log.info(f'{job} marked complete, removing from jobs')
            if job.id in self.jobs:
                del self.jobs[job.id]
                self.last_updated = time_msecs()

    async def post_job_started_1(self, job):
        full_status = await job.status()

        status = {
            'version': full_status['version'],
            'batch_id': full_status['batch_id'],
            'job_id': full_status['job_id'],
            'attempt_id': full_status['attempt_id'],
            'start_time': full_status['start_time'],
            'resources': full_status['resources'],
        }

        body = {'status': status}

        await request_retry_transient_errors(
            self.client_session,
            'POST',
            deploy_config.url('batch-driver', '/api/v1alpha/instances/job_started'),
            json=body,
            headers=self.headers,
        )

    async def post_job_started(self, job):
        try:
            await self.post_job_started_1(job)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(f'error while posting {job} started')

    async def activate(self):
        resp = await request_retry_transient_errors(
            self.client_session,
            'GET',
            deploy_config.url('batch-driver', '/api/v1alpha/instances/credentials'),
            headers={'X-Hail-Instance-Name': NAME, 'Authorization': f'Bearer {os.environ["ACTIVATION_TOKEN"]}'},
        )
        resp_json = await resp.json()

        credentials_file = '/worker-key.json'
        with open(credentials_file, 'w') as f:
            f.write(json.dumps(resp_json['key']))

        self.fs = RouterAsyncFS(
            'file',
            filesystems=[
                LocalAsyncFS(self.pool),
                get_cloud_async_fs(credentials_file=credentials_file),
            ],
        )

        fs = get_cloud_async_fs(credentials_file=credentials_file)
        self.file_store = FileStore(fs, BATCH_LOGS_STORAGE_URI, INSTANCE_ID)

        self.compute_client = get_compute_client(credentials_file=credentials_file)

        resp = await request_retry_transient_errors(
            self.client_session,
            'POST',
            deploy_config.url('batch-driver', '/api/v1alpha/instances/activate'),
            json={'ip_address': os.environ['IP_ADDRESS']},
            headers={'X-Hail-Instance-Name': NAME, 'Authorization': f'Bearer {os.environ["ACTIVATION_TOKEN"]}'},
        )
        resp_json = await resp.json()
        self.last_updated = time_msecs()

        self.headers = {'X-Hail-Instance-Name': NAME, 'Authorization': f'Bearer {resp_json["token"]}'}
        self.active = True


async def async_main():
    image_manager = get_image_manager()
    worker = Worker(httpx.client_session(), image_manager)
    await get_network_allocator().reserve()

    try:
        await worker.run()
    finally:
        try:
            await worker.shutdown()
            log.info('worker shutdown', exc_info=True)
        finally:
            try:
                await image_manager.close()
            finally:
                asyncio.get_event_loop().set_debug(True)
                log.debug('Tasks immediately after docker close')
                dump_all_stacktraces()
                other_tasks = [t for t in asyncio.all_tasks() if t != asyncio.current_task()]
                if other_tasks:
                    _, pending = await asyncio.wait(other_tasks, timeout=10 * 60, return_when=asyncio.ALL_COMPLETED)
                    for t in pending:
                        log.debug('Dangling task:')
                        t.print_stack()
                        t.cancel()


def run():
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGUSR1, dump_all_stacktraces)
    loop.run_until_complete(async_main())
    log.info('closing loop')
    loop.close()
    log.info('closed')
    sys.exit(0)
