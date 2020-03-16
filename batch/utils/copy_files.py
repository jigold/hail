import sys
import os
import asyncio
import shutil
import argparse
import time
import logging
from shlex import split
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
            log.info(f'copied {self.src} to {self.dest} in {total:.3f}s')
        else:
            log.info(f'failed to copy {self.src} to {self.dest} in {total:.3f}s')


def is_gcs_file(file):
    return file.startswith('gs://')


def flatten(its):
    return [x for it in its for x in it]


def listdir(path):
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    if os.path.isfile(path):
        return [path]
    return flatten([listdir(path + '/' + f) for f in os.listdir(path)])


async def copy_file_within_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        await gcs_client.copy_gs_file(src, dest)


async def write_file_to_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        await gcs_client.write_gs_file_from_filename(dest, src)


async def read_file_from_gcs(src, dest):
    async with CopyFileTimer(src, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        await gcs_client.read_gs_file_to_filename(src, dest)


async def copy_local_files(src, dest):
    async with CopyFileTimer(src, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        await blocking_to_async(thread_pool, shutil.copy, src, dest)


async def copies(copy_pool, src, dest):
    if is_gcs_file(src):
        src_paths = await gcs_client.list_gs_files(src)
    else:
        src = os.path.abspath(src)
        src_paths = listdir(src)

    is_dir_src = len(src_paths) != 1 or src_paths[0] != src
    is_dir_dest = is_dir_src or dest.endswith('/')

    if is_dir_dest:
        dest = dest.rstrip('/')

    for src_path in src_paths:
        rel_path = os.path.relpath(src_path, src)
        if rel_path == '.':
            if is_dir_dest:
                dest_path = f'{dest}/{os.path.basename(src_path)}'
            else:
                dest_path = dest
        else:
            dest_path = f'{dest}/{rel_path}'

        if is_gcs_file(src_path) and is_gcs_file(dest_path):
            await copy_pool.call(copy_file_within_gcs, src_path, dest_path)
        elif is_gcs_file(src_path) and not is_gcs_file(dest_path):
            await copy_pool.call(read_file_from_gcs, src_path, dest_path)
        elif not is_gcs_file(src_path) and is_gcs_file(dest_path):
            await copy_pool.call(write_file_to_gcs, src_path, dest_path)
        else:
            assert not is_gcs_file(src_path) and not is_gcs_file(dest_path)
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
    copy_pool = WaitableSharedPool(AsyncWorkerPool(args.parallelism), ignore_errors=False)

    coros = []
    for line in open(sys.stdin):
        src, dest = split(line.rstrip())
        coros.append(copies(copy_pool, src, dest))

    await asyncio.gather(*coros)
    await copy_pool.wait()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
