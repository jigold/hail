from typing import Optional, TYPE_CHECKING

import logging
import os
import asyncio
import concurrent.futures
import tempfile
import errno
import traceback
import shutil

from hailtop import aiotools
from hailtop.utils import time_msecs, check_shell_output, blocking_to_async, check_shell

from ..config import ACCEPTABLE_QUERY_JAR_URL_PREFIX
from ..utils import Timings, populate_secret_host_path
from ..credentials import CloudUserCredentials
from ..jvm import JVM
from ..exceptions import JVMUserError

from .base import Job

if TYPE_CHECKING:
    from ..worker import Worker  # pylint: disable=cyclic-import


log = logging.getLogger('jvm_job')


class JVMJob(Job):
    def __init__(
        self,
        batch_id: int,
        user: str,
        credentials: CloudUserCredentials,
        job_spec,
        format_version,
        task_manager: aiotools.BackgroundTaskManager,
        pool: concurrent.futures.ThreadPoolExecutor,
        worker: 'Worker',
    ):
        super().__init__(batch_id, user, credentials, job_spec, format_version, task_manager, pool, worker)
        assert job_spec['process']['type'] == 'jvm'
        assert worker is not None

        input_files = job_spec.get('input_files')
        output_files = job_spec.get('output_files')
        if input_files or output_files:
            raise Exception("i/o not supported")

        self.user_command_string = job_spec['process']['command']
        assert len(self.user_command_string) >= 3, self.user_command_string
        self.revision = self.user_command_string[1]
        self.jar_url = self.user_command_string[2]

        self.deleted = False
        self.timings = Timings(lambda: self.deleted)
        self.state = 'pending'
        self.log: Optional[str] = None

        self.jvm: Optional[JVM] = None
        self.jvm_name: Optional[str] = None

    def step(self, name):
        return self.timings.step(name)

    def verify_is_acceptable_query_jar_url(self, url: str):
        if not url.startswith(ACCEPTABLE_QUERY_JAR_URL_PREFIX):
            log.error(f'user submitted unacceptable JAR url: {url} for {self}. {ACCEPTABLE_QUERY_JAR_URL_PREFIX}')
            raise ValueError(f'unacceptable JAR url: {url}')

    def secret_host_path(self, secret):
        return f'{self.scratch}/secrets/{secret["mount_path"]}'

    async def run(self):
        async with self.worker.cpu_sem(self.cpu_in_mcpu):
            self.start_time = time_msecs()
            os.makedirs(f'{self.scratch}/')

            try:
                with self.step('connecting_to_jvm'):
                    self.jvm = await self.worker.borrow_jvm()
                    self.jvm_name = str(self.jvm)

                self.task_manager.ensure_future(self.worker.post_job_started(self))

                log.info(f'{self}: initializing')
                self.state = 'initializing'

                await check_shell_output(f'xfs_quota -x -c "project -s -p {self.scratch} {self.project_id}" /host/')
                await check_shell_output(
                    f'xfs_quota -x -c "limit -p bsoft={self.data_disk_storage_in_gib} bhard={self.data_disk_storage_in_gib} {self.project_id}" /host/'
                )

                if self.secrets:
                    for secret in self.secrets:
                        populate_secret_host_path(self.secret_host_path(secret), secret['data'])

                self.state = 'running'

                log.info(f'{self}: downloading JAR')
                with self.step('downloading_jar'):
                    async with self.worker.jar_download_locks[self.revision]:
                        local_jar_location = f'/hail-jars/{self.revision}.jar'
                        if not os.path.isfile(local_jar_location):
                            self.verify_is_acceptable_query_jar_url(self.jar_url)
                            temporary_file = tempfile.NamedTemporaryFile(delete=False)
                            try:
                                async with await self.worker.fs.open(self.jar_url) as jar_data:
                                    while True:
                                        b = await jar_data.read(256 * 1024)
                                        if not b:
                                            break
                                        written = await blocking_to_async(self.worker.pool, temporary_file.write, b)
                                        assert written == len(b)
                                temporary_file.close()
                                os.rename(temporary_file.name, local_jar_location)
                            finally:
                                temporary_file.close()  # close is idempotent
                                try:
                                    os.remove(temporary_file.name)
                                except OSError as err:
                                    if err.errno != errno.ENOENT:
                                        raise

                log.info(f'{self}: running jvm process')
                with self.step('running'):
                    await self.jvm.execute(local_jar_location, self.scratch, self.user_command_string)
                self.state = 'succeeded'
                log.info(f'{self} main: {self.state}')
            except asyncio.CancelledError:
                raise
            except JVMUserError:
                self.state = 'failed'
                self.error = traceback.format_exc()
                await self.cleanup()
            except Exception:
                # FIXME: this can also be a Hail Query driver error, not a Hail Batch error
                log.exception(f'while running {self}')

                self.state = 'error'
                self.error = traceback.format_exc()
                await self.cleanup()
            else:
                await self.cleanup()

    async def cleanup(self):
        if self.jvm is not None:
            # I really want this to be a timed step but I can't skip this ITS CLEAN UP
            # with self.step('retrieve_output'):
            log.info(f'{self}: retrieving log')
            self.log = self.jvm.retrieve_and_clear_output()
            self.worker.return_jvm(self.jvm)
            self.jvm = None

        job_log = self.log
        if job_log is None:
            job_log = ''
        # I really want this to be a timed step but I CANT RAISE EXCEPTIONS IN CLEANUP!!
        # with self.step('uploading_log'):
        log.info(f'{self}: uploading log')
        await self.worker.file_store.write_log_file(
            self.format_version, self.batch_id, self.job_id, self.attempt_id, 'main', job_log
        )

        self.end_time = time_msecs()

        if not self.deleted:
            log.info(f'{self}: marking complete')
            self.task_manager.ensure_future(self.worker.post_job_complete(self))

        log.info(f'{self}: cleaning up')
        try:
            await check_shell(f'xfs_quota -x -c "limit -p bsoft=0 bhard=0 {self.project_id}" /host')
            await blocking_to_async(self.pool, shutil.rmtree, self.scratch, ignore_errors=True)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception('while deleting volumes')

    async def get_log(self):
        if self.log is not None:
            return {'main': self.log}
        return {'main': self.jvm.output()}

    async def delete(self):
        log.info(f'deleting {self} {self.jvm}')
        self.deleted = True
        if self.jvm is not None:
            log.info(f'{self.jvm} interrupting')
            self.jvm.interrupt()

    # {
    #   version: int,
    #   worker: str,
    #   batch_id: int,
    #   job_id: int,
    #   attempt_id: int,
    #   user: str,
    #   state: str, (pending, initializing, running, succeeded, error, failed)
    #   format_version: int
    #   error: str, (optional)
    #   container_statuses: [Container.status],
    #   start_time: int,
    #   end_time: int,
    #   resources: list of dict, {name: str, quantity: int},
    #   jvm: str
    # }
    async def status(self):
        status = await super().status()
        status['container_statuses'] = dict()
        status['container_statuses']['main'] = {'name': 'main', 'state': self.state, 'timing': self.timings.to_dict()}
        status['jvm'] = self.jvm_name
        return status

    def __str__(self):
        return f'job {self.id}'
