from typing import TYPE_CHECKING

import logging
import os
import uuid
import concurrent.futures

from hailtop import aiotools

from ...batch_format_version import BatchFormatVersion
from ...globals import STATUS_FORMAT_VERSION, RESERVED_STORAGE_GB_PER_CORE
from ...cloud.resource_utils import is_valid_storage_request

from ..config import NAME, CLOUD, instance_config
from ..credentials import CloudUserCredentials

if TYPE_CHECKING:
    from ..worker import Worker  # pylint: disable=cyclic-import


log = logging.getLogger('job')


class Job:
    quota_project_id = 100

    @staticmethod
    def get_next_xfsquota_project_id():
        project_id = Job.quota_project_id
        Job.quota_project_id += 1
        return project_id

    def secret_host_path(self, secret) -> str:
        return f'{self.scratch}/secrets/{secret["name"]}'

    def io_host_path(self) -> str:
        return f'{self.scratch}/io'

    def cloudfuse_base_path(self):
        # Make sure this path isn't in self.scratch to avoid accidental bucket deletions!
        path = f'/cloudfuse/{self.token}'
        assert os.path.commonpath([path, self.scratch]) == '/'
        return path

    def cloudfuse_data_path(self, bucket: str) -> str:
        # Make sure this path isn't in self.scratch to avoid accidental bucket deletions!
        path = f'{self.cloudfuse_base_path()}/{bucket}/data'
        assert os.path.commonpath([path, self.scratch]) == '/'
        return path

    def cloudfuse_tmp_path(self, bucket: str) -> str:
        # Make sure this path isn't in self.scratch to avoid accidental bucket deletions!
        path = f'{self.cloudfuse_base_path()}/{bucket}/tmp'
        assert os.path.commonpath([path, self.scratch]) == '/'
        return path

    def cloudfuse_credentials_path(self, bucket: str) -> str:
        return f'{self.scratch}/cloudfuse/{bucket}'

    def credentials_host_dirname(self) -> str:
        return f'{self.scratch}/{self.credentials.secret_name}'

    def credentials_host_file_path(self) -> str:
        return f'{self.credentials_host_dirname()}/{self.credentials.file_name}'
    #
    # @staticmethod
    # def create(
    #     batch_id,
    #     user,
    #     credentials: CloudUserCredentials,
    #     job_spec: dict,
    #     format_version: BatchFormatVersion,
    #     task_manager: aiotools.BackgroundTaskManager,
    #     pool: concurrent.futures.ThreadPoolExecutor,
    #     client_session: httpx.ClientSession,
    #     worker: 'Worker',
    # ) -> 'Job':
    #     type = job_spec['process']['type']
    #     if type == 'docker':
    #         return DockerJob(
    #             batch_id, user, credentials, job_spec, format_version, task_manager, pool, client_session, worker
    #         )
    #     assert type == 'jvm'
    #     return JVMJob(batch_id, user, credentials, job_spec, format_version, task_manager, pool, worker)

    def __init__(
        self,
        batch_id: int,
        user: str,
        credentials: CloudUserCredentials,
        job_spec,
        format_version: BatchFormatVersion,
        task_manager: aiotools.BackgroundTaskManager,
        pool: concurrent.futures.ThreadPoolExecutor,
        worker: 'Worker',
    ):
        self.batch_id = batch_id
        self.user = user
        self.credentials = credentials
        self.job_spec = job_spec
        self.format_version = format_version
        self.task_manager = task_manager
        self.pool = pool
        self.worker = worker

        self.deleted = False

        self.token = uuid.uuid4().hex
        self.scratch = f'/batch/{self.token}'

        self.disk = None
        self.state = 'pending'
        self.error = None

        self.start_time = None
        self.end_time = None

        self.cpu_in_mcpu = job_spec['resources']['cores_mcpu']
        self.memory_in_bytes = job_spec['resources']['memory_bytes']
        extra_storage_in_gib = job_spec['resources']['storage_gib']
        assert extra_storage_in_gib == 0 or is_valid_storage_request(CLOUD, extra_storage_in_gib)

        if instance_config.job_private:
            self.external_storage_in_gib = 0
            self.data_disk_storage_in_gib = extra_storage_in_gib
        else:
            self.external_storage_in_gib = extra_storage_in_gib
            # The reason for not giving each job 5 Gi (for example) is the
            # maximum number of simultaneous jobs on a worker is 64 which
            # basically fills the disk not allowing for caches etc. Most jobs
            # would need an external disk in that case.
            self.data_disk_storage_in_gib = min(
                RESERVED_STORAGE_GB_PER_CORE, self.cpu_in_mcpu / 1000 * RESERVED_STORAGE_GB_PER_CORE
            )

        self.resources = instance_config.quantified_resources(
            self.cpu_in_mcpu, self.memory_in_bytes, self.external_storage_in_gib
        )

        self.input_volume_mounts = []
        self.main_volume_mounts = []
        self.output_volume_mounts = []

        io_volume_mount = {
            'source': self.io_host_path(),
            'destination': '/io',
            'type': 'none',
            'options': ['rbind', 'rw'],
        }
        self.input_volume_mounts.append(io_volume_mount)
        self.main_volume_mounts.append(io_volume_mount)
        self.output_volume_mounts.append(io_volume_mount)

        cloudfuse = job_spec.get('cloudfuse') or job_spec.get('gcsfuse')
        self.cloudfuse = cloudfuse
        if cloudfuse:
            for config in cloudfuse:
                config['mounted'] = False
                bucket = config['bucket']
                assert bucket
                self.main_volume_mounts.append(
                    {
                        'source': f'{self.cloudfuse_data_path(bucket)}',
                        'destination': config['mount_path'],
                        'type': 'none',
                        'options': ['rbind', 'rw', 'shared'],
                    }
                )

        secrets = job_spec.get('secrets')
        self.secrets = secrets
        self.env = job_spec.get('env', [])

        self.project_id = Job.get_next_xfsquota_project_id()

    @property
    def job_id(self):
        return self.job_spec['job_id']

    @property
    def attempt_id(self):
        return self.job_spec['attempt_id']

    @property
    def id(self):
        return (self.batch_id, self.job_id)

    async def run(self):
        pass

    async def get_log(self):
        pass

    async def delete(self):
        log.info(f'deleting {self}')
        self.deleted = True

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
    #   resources: list of dict, {name: str, quantity: int}
    # }
    async def status(self):
        status = {
            'version': STATUS_FORMAT_VERSION,
            'worker': NAME,
            'batch_id': self.batch_id,
            'job_id': self.job_spec['job_id'],
            'attempt_id': self.job_spec['attempt_id'],
            'user': self.user,
            'state': self.state,
            'format_version': self.format_version.format_version,
            'resources': self.resources,
        }
        if self.error:
            status['error'] = self.error

        status['start_time'] = self.start_time
        status['end_time'] = self.end_time

        return status

    def __str__(self):
        return f'job {self.id}'
