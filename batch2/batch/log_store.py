import logging
import os

import google
import hailtop.gear.auth as hj

from .google_storage import GCS
from .utils import check_shell, CalledProcessError


log = logging.getLogger('logstore')


class LogStore:
    @staticmethod
    def _parse_uri(uri):
        assert uri.startswith('gs://')
        uri = uri.lstrip('gs://').split('/')
        bucket = uri[0]
        path = '/'.join(uri[1:])
        return bucket, path

    @staticmethod
    def container_log_path(directory, container_name):
        return f'{directory}{container_name}/job.log'

    @staticmethod
    def container_status_path(directory, container_name):
        return f'{directory}{container_name}/status'

    def __init__(self, blocking_pool, instance_id, batch_gsa_key=None, batch_bucket_name=None):
        self.instance_id = instance_id
        self.gcs = GCS(blocking_pool, batch_gsa_key)

        if batch_bucket_name is None:
            batch_jwt = os.environ.get('BATCH_JWT', '/batch-jwt/jwt')
            with open(batch_jwt, 'r') as f:
                batch_bucket_name = hj.JWTClient.unsafe_decode(f.read())['bucket_name']
        self.batch_bucket_name = batch_bucket_name

    def gs_job_output_directory(self, batch_id, job_id, token):
        return f'gs://{self.batch_bucket_name}/{self.instance_id}/{batch_id}/{job_id}/{token}/'

    async def write_gs_file(self, file_path, data):
        bucket, path = LogStore._parse_uri(file_path)
        return await self.gcs.upload_private_gs_file_from_string(bucket, path, data)

    async def read_gs_file(self, file_path):
        bucket, path = LogStore._parse_uri(file_path)
        return await self.gcs.download_gs_file_as_string(bucket, path)

    async def delete_gs_file(self, file_path):
        bucket, path = LogStore._parse_uri(file_path)
        err = await self.gcs.delete_gs_file(bucket, path)
        if isinstance(err, google.api_core.exceptions.NotFound):
            log.info(f'ignoring: cannot delete file that does not exist: {err}')
            err = None
        return err

    async def delete_gs_files(self, directory): ## FIXME!!!
        try:
            await check_shell(f'gsutil rm -r {directory}')
            return None
        except Exception as err:
            return err


# class LogStore:
#     log_file_name = 'container_logs'
#     pod_status_file_name = 'pod_status'
#
#     files = (log_file_name, pod_status_file_name)
#
#     @staticmethod
#     def _parse_uri(uri):
#         assert uri.startswith('gs://')
#         uri = uri.lstrip('gs://').split('/')
#         bucket = uri[0]
#         path = '/'.join(uri[1:])
#         return bucket, path
#
#     def __init__(self, blocking_pool, instance_id, batch_gsa_key=None, batch_bucket_name=None):
#         self.instance_id = instance_id
#         self.gcs = GCS(blocking_pool, batch_gsa_key)
#
#         if batch_bucket_name is None:
#             batch_jwt = os.environ.get('BATCH_JWT', '/batch-jwt/jwt')
#             with open(batch_jwt, 'r') as f:
#                 batch_bucket_name = hj.JWTClient.unsafe_decode(f.read())['bucket_name']
#         self.batch_bucket_name = batch_bucket_name
#
#     def gs_job_output_directory(self, batch_id, job_id, token):
#         return f'gs://{self.batch_bucket_name}/{self.instance_id}/{batch_id}/{job_id}/{token}/'
#
#     async def write_gs_file(self, directory, file_name, data):
#         assert file_name in LogStore.files
#         bucket, path = LogStore._parse_uri(f'{directory}{file_name}')
#         return await self.gcs.upload_private_gs_file_from_string(bucket, path, data)
#
#     async def read_gs_file(self, directory, file_name):
#         assert file_name in LogStore.files
#         bucket, path = LogStore._parse_uri(f'{directory}{file_name}')
#         return await self.gcs.download_gs_file_as_string(bucket, path)
#
#     async def delete_gs_file(self, directory, file_name):
#         assert file_name in LogStore.files
#         bucket, path = LogStore._parse_uri(f'{directory}{file_name}')
#         err = await self.gcs.delete_gs_file(bucket, path)
#         if isinstance(err, google.api_core.exceptions.NotFound):
#             log.info(f'ignoring: cannot delete file that does not exist: {err}')
#             err = None
#         return err
#
#     async def delete_gs_files(self, directory):
#         errors = []
#         for file in LogStore.files:
#             err = await self.delete_gs_file(directory, file)
#             errors.append((file, err))
#         return errors
