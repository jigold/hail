import sys
import os
import re
import asyncio
import shutil
import argparse
import time
import logging
import shlex
import glob
import fnmatch
import concurrent
import google.oauth2.service_account

import batch.google_storage
from hailtop.utils import AsyncWorkerPool, WaitableSharedPool, blocking_to_async


log = logging.getLogger('copy_files')

thread_pool = None
gcs_client = None
copy_failure = False


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


def is_gcs_path(file):
    return file.startswith('gs://')


def flatten(its):
    return [x for it in its for x in it]


def listdir(path, dest):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if os.path.isfile(path):
        return [(path, f'{dest}/{os.path.basename(path)}')]
    return flatten([listdir(path + '/' + f, f'{dest}/{f}') for f in os.listdir(path)])


def get_dest_path(file, template):
    file = file.split('/')
    template = template.rstrip('/').split('/')
    template_length = len(template)
    if template_length == len(file):
        return file[-1]
    return '/'.join(file[template_length:])


async def copy_file_within_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        await gcs_client.copy_gs_file(src, dest)


async def write_file_to_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        await gcs_client.write_gs_file_from_filename(dest, src)


async def read_file_from_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        dest = os.path.abspath(dest)
        await blocking_to_async(thread_pool, os.makedirs, os.path.dirname(dest), exist_ok=True)
        await gcs_client.read_gs_file_to_filename(src, dest)


async def copy_local_files(src, dest):
    async with CopyFileTimer(src, dest):
        dest = os.path.abspath(dest)
        await blocking_to_async(thread_pool, os.makedirs, os.path.dirname(dest), exist_ok=True)
        await blocking_to_async(thread_pool, shutil.copy, src, dest)


async def copies(copy_pool, src, dest):
    if is_gcs_path(src):
        print(f'src={src}')
        src_prefix = re.split('\\*|\\[\\?', src)[0]
        print(f'src_prefix = {src_prefix}')
        src_paths = [path for path, _ in await gcs_client.list_gs_files(src_prefix)
                     if fnmatch.fnmatchcase(path, src)]
        print(f'src_paths={src_paths}')
        if len(src_paths) == 1:
            file = src_paths[0]
            if dest.endswith('/'):
                paths = [(file, f'{dest}{os.path.basename(file)}')]
            else:
                paths = [(file, dest)]
        elif src_paths:
            dest = dest.rstrip('/') + '/'
            paths = [(src_path, dest + get_dest_path(src_path, src)) for src_path in src_paths]
        else:
            raise FileNotFoundError(src)
    else:
        src = os.path.abspath(src)
        print(f'src={src}')
        src_paths = glob.glob(src, recursive=True)
        print(f'src_paths={src_paths}')
        if len(src_paths) == 1 and os.path.isfile(src_paths[0]):
            file = src_paths[0]
            if dest.endswith('/'):
                paths = [(file, f'{dest}{os.path.basename(file)}')]
            else:
                paths = [(file, dest)]
        elif src_paths:
            dest = dest.rstrip('/')
            paths = flatten([listdir(src_path, dest) for src_path in src_paths])
        else:
            raise FileNotFoundError(src)

    print(f'paths={paths}')
    for src_path, dest_path in paths:
        if is_gcs_path(src_path) and is_gcs_path(dest_path):
            await copy_pool.call(copy_file_within_gcs, src_path, dest_path)
        elif is_gcs_path(src_path) and not is_gcs_path(dest_path):
            await copy_pool.call(read_file_from_gcs, src_path, dest_path)
        elif not is_gcs_path(src_path) and is_gcs_path(dest_path):
            await copy_pool.call(write_file_to_gcs, src_path, dest_path)
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
            coros.append(copies(copy_pool, src, dest))
        await asyncio.gather(*coros)
        await copy_pool.wait()
    finally:
        await worker_pool.cancel()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
