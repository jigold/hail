import sys
import os
import re
import io
import asyncio
import shutil
import argparse
import time
import logging
import shlex
import functools
import glob
import fnmatch
import concurrent
import uuid
import json
import google.oauth2.service_account
from google.resumable_media._upload import get_total_bytes, _CONTENT_TYPE_HEADER, _POST

import batch.google_storage
from hailtop.utils import AsyncWorkerPool, WaitableSharedPool, blocking_to_async, bounded_gather


log = logging.getLogger('copy_files')

thread_pool = None
gcs_client = None
copy_failure = False
BLOCK_SIZE = 64 * 1024 * 1024


class CopyFileTimer:
    def __init__(self, src, dest):
        self.src = src
        self.dest = dest
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        finish_time = time.time()
        total = finish_time - self.start_time
        if exc_type is None:
            print(f'copied {self.src} to {self.dest} in {total:.3f}s')
        else:
            print(f'failed to copy {self.src} to {self.dest} in {total:.3f}s due to {exc_type} {exc!r}')


# # ResumableUpload
# def _prepare_initiate_request(self, stream, metadata, content_type, total_bytes=None, stream_final=True):
#     if self.resumable_url is not None:
#         raise ValueError(u"This upload has already been initiated.")
#     self._stream = stream
#     self._content_type = content_type
#     headers = {
#         _CONTENT_TYPE_HEADER: u"application/json; charset=UTF-8",
#         u"x-upload-content-type": content_type,
#     }
#     # Set the total bytes if possible.
#     if total_bytes is not None:
#         self._total_bytes = total_bytes
#     elif stream_final:
#         self._total_bytes = get_total_bytes(stream)
#     print(f'total_bytes={self._total_bytes}')
#
#     # Add the total bytes to the headers if set.
#     if self._total_bytes is not None:
#         content_length = u"{:d}".format(self._total_bytes)
#         headers[u"x-upload-content-length"] = content_length
#     headers.update(self._headers)
#     payload = json.dumps(metadata).encode(u"utf-8")
#     return _POST, self.upload_url, payload, headers
#
#
# google.resumable_media._upload.ResumableUpload._prepare_initiate_request = _prepare_initiate_request


# class FileShard(io.RawIOBase):
# #     def __init__(self, f, start, end):
# #         self.f = f
# #         self.start = start
# #         self.end = end
# #
# #         self.f.seek(start)
# #        super(io.RawIOBase, self).__init__()
#
#
#     #
#     # def read(self, size=-1):
#     #     return self.f.read(size)
#     #
#     # def readall(self):
#     #     return self.f.readall()
#     #
#     # def readinto(self, b):
#     #     return self.f.readinto(b)
#     #
#     # def write(self, b):
#     #     return self.f.write(b)


class FileShard(io.FileIO):
    pass


def is_gcs_path(file):
    return file.startswith('gs://')


def flatten(its):
    return [x for it in its for x in it]


def listdir(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if os.path.isfile(path):
        return [(path, os.path.getsize(path))]
    # gsutil doesn't copy empty directories
    return flatten([listdir(path.rstrip('/') + '/' + f) for f in os.listdir(path)])


def get_dest_path(file, src, include_recurse_dir):
    src = src.rstrip('/').split('/')
    file = file.split('/')
    if len(src) == len(file):
        return file[-1]

    # https://cloud.google.com/storage/docs/gsutil/commands/cp#how-names-are-constructed_1
    if include_recurse_dir:
        recurse_point = len(src) - 1
    else:
        recurse_point = len(src)

    return '/'.join(file[recurse_point:])


async def copy_file_within_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        await gcs_client.copy_gs_file(src, dest)


async def write_file_to_gcs(src, dest, size):
    async def _write(tmp_dest, start, end):
        size = end - start
        print(f'src={src} dest={dest} start={start} end={end} size={size}')
        with open(src, 'rb') as src_file:
            src_file.seek(start)
            await gcs_client.write_gs_file_from_file(tmp_dest, src_file, size=size)

    async with CopyFileTimer(src, dest):
        try:
            starts = [i for i in range(0, size, BLOCK_SIZE)]
            if not starts:
                starts = [0]
            starts.append(size)

            tmp_dests = [dest + f'/tmp/_{uuid.uuid4().hex[:8]}' for _ in range(len(starts) - 1)]

            work = [functools.partial(_write, tmp_dests[i], starts[i], starts[i+1]) for i in range(len(starts) - 1)]
            await bounded_gather(*work, parallelism=5)
            await gcs_client.compose_gs_file(tmp_dests, dest)
        finally:
            await gcs_client.delete_gs_files(dest + '/tmp/')


async def read_file_from_gcs(src, dest, size):
    async def _read(start, end):
        with open(dest, 'wb') as dest_file:
            dest_file.seek(start)
            print(f'reading from {src} to {dest} for bytes {start}-{end} starting at dest {dest_file.tell()}')
            await gcs_client.read_gs_file_to_file(src, dest_file, start=start, end=end)

    async with CopyFileTimer(src, dest):
        try:
            dest = os.path.abspath(dest)
            await blocking_to_async(thread_pool, os.makedirs, os.path.dirname(dest), exist_ok=True)

            starts = [i for i in range(0, size, BLOCK_SIZE)]
            if not starts:
                starts = [0]
            starts.append(size)

            work = [functools.partial(_read, starts[i], starts[i+1]) for i in range(len(starts) - 1)]
            await bounded_gather(*work, parallelism=5)
        except Exception:
            if os.path.exists(dest):
                os.remove(dest)
            raise


async def copy_local_files(src, dest):
    async with CopyFileTimer(src, dest):
        dest = os.path.abspath(dest)
        await blocking_to_async(thread_pool, os.makedirs, os.path.dirname(dest), exist_ok=True)
        await blocking_to_async(thread_pool, shutil.copy, src, dest)


async def copies(copy_pool, src, dest):
    if is_gcs_path(src):
        src_prefix = re.split('\\*|\\[\\?', src)[0].rstrip('/')
        maybe_src_paths = [(path, size) for path, size in await gcs_client.list_gs_files(src_prefix)]
        non_recursive_matches = [(path, size) for path, size in maybe_src_paths if fnmatch.fnmatchcase(path, src)]
        if not src.endswith('/') and non_recursive_matches:
            src_paths = non_recursive_matches
        else:
            src_paths = [(path, size) for path, size in maybe_src_paths
                         if fnmatch.fnmatchcase(path, src.rstrip('/') + '/*') or
                         fnmatch.fnmatchcase(path, src.rstrip('/'))]
    else:
        src = os.path.abspath(src)
        src_paths = glob.glob(src, recursive=True)
        src_paths = flatten([listdir(src_path) for src_path in src_paths])

    if len(src_paths) == 1:
        file, size = src_paths[0]
        if dest.endswith('/'):
            paths = [(file, f'{dest}{os.path.basename(file)}', size)]
        else:
            paths = [(file, dest, size)]
    elif src_paths:
        if is_gcs_path(dest):
            include_recurse_dir = dest.endswith('/') or next(await gcs_client.list_gs_files(dest, max_results=1), None) is not None
        else:
            include_recurse_dir = True
        dest = dest.rstrip('/') + '/'
        paths = [(src_path, dest + get_dest_path(src_path, src, include_recurse_dir), size) for src_path, size in src_paths]
    else:
        raise FileNotFoundError(src)

    for src_path, dest_path, size in paths:
        if is_gcs_path(src_path) and is_gcs_path(dest_path):
            await copy_pool.call(copy_file_within_gcs, src_path, dest_path)
        elif is_gcs_path(src_path) and not is_gcs_path(dest_path):
            await copy_pool.call(read_file_from_gcs, src_path, dest_path, size)
        elif not is_gcs_path(src_path) and is_gcs_path(dest_path):
            await copy_pool.call(write_file_to_gcs, src_path, dest_path, size)
        else:
            assert not is_gcs_path(src_path) and not is_gcs_path(dest_path)
            await copy_pool.call(copy_local_files, src_path, dest_path)


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--key-file', type=str, required=True)
    parser.add_argument('--project', type=str, required=True)
    parser.add_argument('--parallelism', type=int, default=10)

    args = parser.parse_args()

    global thread_pool, gcs_client

    thread_pool = concurrent.futures.ThreadPoolExecutor()
    credentials = google.oauth2.service_account.Credentials.from_service_account_file(args.key_file)
    gcs_client = batch.google_storage.GCS(thread_pool, project=args.project, credentials=credentials)
    worker_pool = AsyncWorkerPool(args.parallelism)
    copy_pool = WaitableSharedPool(worker_pool, ignore_errors=False)

    try:
        coros = []
        for line in sys.stdin:
            src, dest = shlex.split(line.rstrip())
            if '**' in src:
                raise NotImplementedError(f'** not supported; got {src}')
            coros.append(copies(copy_pool, src, dest))
        await asyncio.gather(*coros)
        await copy_pool.wait()
    finally:
        await worker_pool.cancel()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
