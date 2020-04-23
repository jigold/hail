import logging
from functools import wraps

import google.api_core.exceptions
import google.oauth2.service_account
import google.cloud.storage
from hailtop.utils import blocking_to_async, retry_transient_errors


logging.getLogger("google").setLevel(logging.WARNING)


class GCS:
    @staticmethod
    def _parse_uri(uri):
        assert uri.startswith('gs://'), uri
        uri = uri.lstrip('gs://').split('/')
        bucket = uri[0]
        path = '/'.join(uri[1:])
        return bucket, path

    def __init__(self, blocking_pool, *, project=None, credentials=None):
        self.blocking_pool = blocking_pool
        # project=None doesn't mean default, it means no project:
        # https://github.com/googleapis/google-cloud-python/blob/master/storage/google/cloud/storage/client.py#L86
        if project:
            self.gcs_client = google.cloud.storage.Client(
                project=project, credentials=credentials)
        else:
            self.gcs_client = google.cloud.storage.Client(
                credentials=credentials)
        self._wrapped_write_gs_file_from_string = self._wrap_network_call(GCS._write_gs_file_from_string)
        self._wrapped_write_gs_file_from_file = self._wrap_network_call(GCS._write_gs_file_from_file)
        self._wrapped_read_gs_file = self._wrap_network_call(GCS._read_gs_file)
        self._wrapped_read_binary_gs_file = self._wrap_network_call(GCS._read_binary_gs_file)
        self._wrapped_read_gs_file_to_file = self._wrap_network_call(GCS._read_gs_file_to_file)
        self._wrapped_delete_gs_file = self._wrap_network_call(GCS._delete_gs_file)
        self._wrapped_delete_gs_files = self._wrap_network_call(GCS._delete_gs_files)
        self._wrapped_copy_gs_file = self._wrap_network_call(GCS._copy_gs_file)
        self._wrapped_list_gs_files = self._wrap_network_call(GCS._list_gs_files)
        self._wrapped_compose_gs_file = self._wrap_network_call(GCS._compose_gs_file)
        self._wrapped_get_blob = self._wrap_network_call(GCS._get_blob)

    async def write_gs_file_from_string(self, uri, string, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_write_gs_file_from_string,
                                            self, uri, string, *args, **kwargs)

    async def write_gs_file_from_file(self, uri, file_name, start, end, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_write_gs_file_from_file,
                                            self, uri, file_name, start, end, *args, **kwargs)

    async def read_gs_file(self, uri, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_read_gs_file,
                                            self, uri, *args, **kwargs)

    async def read_binary_gs_file(self, uri, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_read_binary_gs_file,
                                            self, uri, *args, **kwargs)

    async def read_gs_file_to_file(self, uri, file_name, offset, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_read_gs_file_to_file,
                                            self, uri, file_name, offset, *args, **kwargs)

    async def delete_gs_file(self, uri):
        return await retry_transient_errors(self._wrapped_delete_gs_file,
                                            self, uri)

    async def delete_gs_files(self, uri_prefix):
        return await retry_transient_errors(self._wrapped_delete_gs_files,
                                            self, uri_prefix)

    async def copy_gs_file(self, src, dest, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_copy_gs_file,
                                            self, src, dest, *args, **kwargs)

    async def compose_gs_file(self, sources, dest, *args, **kwargs):
        return await retry_transient_errors(self._wrapped_compose_gs_file,
                                            self, sources, dest, *args, **kwargs)

    async def list_gs_files(self, uri, max_results=None):
        return await retry_transient_errors(self._wrapped_list_gs_files,
                                            self, uri, max_results=max_results)

    async def get_blob(self, uri):
        return await retry_transient_errors(self._wrapped_get_blob,
                                            self, uri)

    def _wrap_network_call(self, fun):
        @wraps(fun)
        async def wrapped(*args, **kwargs):
            return await blocking_to_async(self.blocking_pool,
                                           fun,
                                           *args,
                                           **kwargs)
        return wrapped

    def _write_gs_file_from_string(self, uri, string, *args, **kwargs):
        f = self._get_blob(uri)
        f.metadata = {'Cache-Control': 'no-cache'}
        f.upload_from_string(string, *args, **kwargs)

    def _write_gs_file_from_file(self, uri, file_name, *args, **kwargs):
        with open(file_name, 'r') as file:
            f = self._get_blob(uri)
            f.metadata = {'Cache-Control': 'no-cache'}
            f.upload_from_file(file, *args, **kwargs)

    def _read_gs_file(self, uri, *args, **kwargs):
        content = self._read_binary_gs_file(uri, *args, **kwargs)
        return content.decode('utf-8')

    def _read_binary_gs_file(self, uri, *args, **kwargs):
        f = self._get_blob(uri)
        f.metadata = {'Cache-Control': 'no-cache'}
        content = f.download_as_string(*args, **kwargs)
        return content

    def _read_gs_file_to_file(self, uri, file_name, offset, *args, **kwargs):
        with open(file_name, 'r+b') as file:
            file.seek(offset)
            f = self._get_blob(uri)
            f.metadata = {'Cache-Control': 'no-cache'}
            f.download_to_file(file, *args, **kwargs)

    def _delete_gs_files(self, prefix):
        bucket, prefix = GCS._parse_uri(prefix)
        bucket = self.gcs_client.bucket(bucket)
        for blob in bucket.list_blobs(prefix=prefix):
            try:
                blob.delete()
            except google.api_core.exceptions.NotFound:
                continue

    def _delete_gs_file(self, uri):
        f = self._get_blob(uri)
        try:
            f.delete()
        except google.api_core.exceptions.NotFound:
            return

    def _copy_gs_file(self, src, dest, *args, **kwargs):
        src_bucket, src_path = GCS._parse_uri(src)
        src_bucket = self.gcs_client.bucket(src_bucket)
        dest_bucket, dest_path = GCS._parse_uri(dest)
        dest_bucket = self.gcs_client.bucket(dest_bucket)
        src_f = src_bucket.blob(src_path)
        src_bucket.copy_blob(src_f, dest_bucket, new_name=dest_path, *args, **kwargs)

    def _list_gs_files(self, uri, max_results=None):
        bucket_name, prefix = GCS._parse_uri(uri)
        bucket = self.gcs_client.bucket(bucket_name)
        return iter(bucket.list_blobs(prefix=prefix, max_results=max_results))

    def _compose_gs_file(self, sources, dest, *args, **kwargs):
        assert sources
        sources = [self._get_blob(src) for src in sources]
        dest = self._get_blob(dest)
        dest.compose(sources, *args, **kwargs)

    def _get_blob(self, uri):
        bucket, path = GCS._parse_uri(uri)
        bucket = self.gcs_client.bucket(bucket)
        return bucket.blob(path)
