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
import humanize
import glob
import fnmatch
import concurrent
import uuid
import google.oauth2.service_account

import batch.google_storage
from batch.utils import parse_memory_in_bytes
from hailtop.utils import AsyncWorkerPool, MultiWaitableSharedPool, blocking_to_async, \
    bounded_gather, WaitableSharedPool


log = logging.getLogger('copy_files')

thread_pool = None
gcs_client = None
copy_failure = False
file_lock_store = None

MIN_PARTITION_SIZE = None
MAX_PARTITIONS = None


class FileLock:
    def __init__(self, lock_store, file, lock):
        self.lock_store = lock_store
        self.file = file
        self.lock = lock

    async def __aenter__(self):
        await self.lock.acquire()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()
        self.lock_store.release_lock(self.file)


class FileLockStore:
    def __init__(self):
        self.locks = {}
        self.n_holders = {}

    def get_lock(self, file):
        lock = self.locks.get(file)

        if lock is None:
            lock = asyncio.Lock()
            self.locks[file] = lock
            self.n_holders[file] = 0

        self.n_holders[file] += 1

        return FileLock(self, file, lock)

    def release_lock(self, file):
        assert file in self.locks
        assert file in self.n_holders
        self.n_holders[file] -= 1
        if self.n_holders[file] == 0:
            del self.n_holders[file]
            del self.locks[file]


class CopyFileTimerStep:
    def __init__(self, timer, name):
        self.timer = timer
        self.name = name
        self.start_time = None

    async def __aenter__(self):
        self.start_time = time.time()

    async def __aexit__(self, exc_type, exc, tb):
        finish_time = time.time()
        self.timer.timing[self.name] = finish_time - self.start_time


class CopyFileTimer:
    def __init__(self, src, dest, size):
        self.src = src
        self.dest = dest
        self.size = humanize.naturalsize(size)
        self.start_time = None
        self.timing = {}

    def step(self, name):
        return CopyFileTimerStep(self, name)

    async def __aenter__(self):
        print(f'copying {self.src} to {self.dest} of size {self.size}...')
        self.start_time = time.time()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        finish_time = time.time()
        total = finish_time - self.start_time
        if exc_type is None:
            msg = f'copied {self.src} to {self.dest} of size {self.size} in {total:.3f}s'
            if self.timing:
                msg += '\n  ' + '\n  '.join([f'{k}: {v:.3f}s' for k, v in self.timing.items()])
            print(msg)
        else:
            print(f'failed to copy {self.src} to {self.dest} of size {self.size} in {total:.3f}s due to {exc!r}')


class FilePart(io.IOBase):
    def __init__(self, filename, offset, length):
        self._fp = open(filename, 'rb')
        self.length = length
        self._start = offset
        self._end = self._start + self.length
        self._fp.seek(self._start)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def tell(self):
        return self._fp.tell() - self._start

    def read(self, size=-1):
        if size < 0:
            size = self.length
        size = min(size, self._end - self._fp.tell())
        return self._fp.read(max(0, size))

    def seek(self, offset, whence=os.SEEK_SET):
        if whence == os.SEEK_END:
            return self._fp.seek(offset + self._end)
        elif whence == os.SEEK_CUR:
            return self._fp.seek(offset, whence)
        else:
            assert whence == os.SEEK_SET
            return self._fp.seek(self._start + offset)

    def close(self):
        self._fp.close()

    def flush(self, size=None):
        raise NotImplementedError('flush is not implemented in FilePart.')

    def fileno(self, size=None):
        raise NotImplementedError('fileno is not implemented in FilePart.')

    def isatty(self, size=None):
        raise NotImplementedError('isatty is not implemented in FilePart.')

    def next(self, size=None):
        raise NotImplementedError('next is not implemented in FilePart.')

    def readline(self, size=None):
        raise NotImplementedError('readline is not implemented in FilePart.')

    def readlines(self, size=None):
        raise NotImplementedError('readlines is not implemented in FilePart.')

    def xreadlines(self, size=None):
        raise NotImplementedError('xreadlines is not implemented in FilePart.')

    def truncate(self, size=None):
        raise NotImplementedError('truncate is not implemented in FilePart.')

    def write(self, size=None):
        raise NotImplementedError('write is not implemented in FilePart.')

    def writelines(self, size=None):
        raise NotImplementedError('writelines is not implemented in FilePart.')


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


def get_partition_starts(file_size):
    if file_size == 0:
        return [0, 0]

    if file_size / MIN_PARTITION_SIZE > MAX_PARTITIONS:
        partition_size = (file_size + MAX_PARTITIONS - 1) // MAX_PARTITIONS
    else:
        partition_size = MIN_PARTITION_SIZE

    partition_starts = [i for i in range(0, file_size, partition_size)]
    partition_starts.append(file_size)
    return partition_starts


async def copy_file_within_gcs(copy_pool, src, dest, size):
    async def _copy(src, dest):
        async with file_lock_store.get_lock(dest):
            async with CopyFileTimer(src, dest, size):
                await gcs_client.copy_gs_file(src, dest)

    copy_token = uuid.uuid4().hex[:5]
    await copy_pool.call(copy_token, _copy, src, dest)
    await copy_pool.wait(copy_token)


async def write_file_to_gcs(copy_pool, src, dest, size):
    async def _write(timer, tmp_dest, start, end):
        async with timer.step(f'upload {start}-{end - 1}'):
            part_size = end - start
            with FilePart(src, start, part_size) as fp:
                await gcs_client.write_gs_file_from_file(tmp_dest, fp)

    async with file_lock_store.get_lock(dest):
        async with CopyFileTimer(src, dest, size) as timer:
            token = uuid.uuid4().hex[:8]
            try:
                starts = get_partition_starts(size)
                tmp_dests = [dest + f'/{token}/{uuid.uuid4().hex[:8]}' for _ in range(len(starts) - 1)]

                copy_token = uuid.uuid4().hex[:5]
                for i in range(len(starts) - 1):
                    await copy_pool.call(copy_token, _write, timer, tmp_dests[i], starts[i], starts[i+1])
                await copy_pool.wait(copy_token)

                async with timer.step('compose'):
                    await gcs_client.compose_gs_file(tmp_dests, dest)
            finally:
                async with timer.step('delete temp files'):
                    await gcs_client.delete_gs_files(dest + f'/{token}/')


async def read_file_from_gcs(copy_pool, src, dest, size):
    async def _read(timer, start, end):
        async with timer.step(f'download {start}-{end}'):
            with open(dest, 'r+b') as dest_file:
                await gcs_client.read_gs_file_to_file(src, dest_file, offset=start, start=start, end=end)

    async with file_lock_store.get_lock(dest):
        async with CopyFileTimer(src, dest, size) as timer:
            try:
                async with timer.step('setup'):
                    dest = os.path.abspath(dest)
                    await blocking_to_async(thread_pool, os.makedirs, os.path.dirname(dest), exist_ok=True)

                    if os.path.exists(dest):
                        os.remove(dest)

                    with open(dest, 'ab') as fp:
                        fp.truncate(size)

                starts = get_partition_starts(size)

                copy_token = uuid.uuid4().hex[:5]
                for i in range(len(starts) - 1):
                    await copy_pool.call(copy_token, _read, timer, starts[i], starts[i+1] - 1)
                await copy_pool.wait(copy_token)
            except Exception:
                if os.path.exists(dest):
                    os.remove(dest)
                raise


async def copy_local_files(copy_pool, src, dest, size):
    async def _copy(src, dest):
        async with file_lock_store.get_lock(dest):
            async with CopyFileTimer(src, dest, size):
                dest = os.path.abspath(dest)
                await blocking_to_async(thread_pool, os.makedirs, os.path.dirname(dest), exist_ok=True)
                await blocking_to_async(thread_pool, shutil.copy, src, dest)

    copy_token = uuid.uuid4().hex[:5]
    await copy_pool.call(copy_token, _copy, src, dest)
    await copy_pool.wait(copy_token)


async def copies(file_pool, copy_pool, src, dest):
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
            await file_pool.call(copy_file_within_gcs, copy_pool, src_path, dest_path, size)
        elif is_gcs_path(src_path) and not is_gcs_path(dest_path):
            await file_pool.call(read_file_from_gcs, copy_pool, src_path, dest_path, size)
        elif not is_gcs_path(src_path) and is_gcs_path(dest_path):
            await file_pool.call(write_file_to_gcs, copy_pool, src_path, dest_path, size)
        else:
            assert not is_gcs_path(src_path) and not is_gcs_path(dest_path)
            await file_pool.call(copy_local_files, copy_pool, src_path, dest_path, size)


async def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--key-file', type=str, required=True)
    parser.add_argument('--project', type=str, required=True)
    parser.add_argument('--parallelism', type=int, default=10)
    parser.add_argument('--concurrent-downloads', type=int, default=3)
    parser.add_argument('--max-partitions', type=int, default=32)
    parser.add_argument('--min-partition-size', type=str, default='256Mi')

    args = parser.parse_args()

    global thread_pool, gcs_client, file_lock_store, MAX_PARTITIONS, MIN_PARTITION_SIZE

    MAX_PARTITIONS = args.max_partitions
    MIN_PARTITION_SIZE = parse_memory_in_bytes(args.min_partition_size)

    thread_pool = concurrent.futures.ThreadPoolExecutor()
    credentials = google.oauth2.service_account.Credentials.from_service_account_file(args.key_file)
    gcs_client = batch.google_storage.GCS(thread_pool, project=args.project, credentials=credentials)

    file_worker_pool = AsyncWorkerPool(args.concurrent_downloads)
    file_pool = WaitableSharedPool(file_worker_pool)

    copy_worker_pool = AsyncWorkerPool(args.parallelism)
    copy_pool = MultiWaitableSharedPool(copy_worker_pool)

    file_lock_store = FileLockStore()

    try:
        coros = []
        for line in sys.stdin:
            src, dest = shlex.split(line.rstrip())
            if '**' in src:
                raise NotImplementedError(f'** not supported; got {src}')
            coros.append(copies(file_pool, copy_pool, src, dest))
        await asyncio.gather(*coros)
        await file_pool.wait()
    finally:
        await copy_worker_pool.cancel()
        await file_worker_pool.cancel()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
loop.close()
