import sys
import humanize
import asyncio
import aiohttp
from hailtop import aiotools
from hailtop.aiocloud import aiogoogle
from hailtop.utils import OnlineBoundedGather2, parse_timestamp_msecs, time_msecs, flatten, time_msecs_str


class AsyncIOExecutor:
    def __init__(self, parallelism):
        self._semaphore = asyncio.Semaphore(parallelism)
        self.task_manager = aiotools.BackgroundTaskManager()

    def shutdown(self):
        self.task_manager.shutdown()

    async def _run(self, fut, aw):
        async with self._semaphore:
            try:
                fut.set_result(await aw)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception as e:  # pylint: disable=broad-except
                fut.set_exception(e)

    def submit(self, aw):
        fut = asyncio.Future()
        self.task_manager.ensure_future(self._run(fut, aw))
        return fut

    async def gather(self, aws):
        futs = [self.submit(aw) for aw in aws]
        return [await fut for fut in futs]


class ListTrackedDigests:
    def __init__(self, client):
        self._executor = AsyncIOExecutor(8)
        self._client = client

    def shutdown(self):
        self._executor.shutdown()

    async def list_digests(self, image):
        print(f'listing digests for {image}')
        result = await self._executor.submit(self._client.get(f'/{image}/tags/list'))
        manifests = result['manifest']
        return list(manifests.keys())

    async def run(self):
        images = await self._executor.submit(self._client.get('/tags/list'))
        known_digests = await asyncio.gather(*[
            self.list_digests(image)
            for image in images['child']
        ])
        return flatten(known_digests)


class StorageDigest:
    def __init__(self, status, size):
        self.path = 'gs://' + status['id']
        self.digest = status['name'].rsplit('/', maxsplit=1)[1]
        self.updated = parse_timestamp_msecs(status['updated'])
        self.size = size


class ListAllStorageDigests:
    def __init__(self, fs: aiogoogle.GoogleStorageAsyncFS, project: str):
        self.fs = fs
        self.project = project

    async def run(self):
        entries = []
        async for entry in await self.fs.listfiles(f'gs://artifacts.{self.project}.appspot.com/containers/images/', recursive=True):
            status = await entry.status()
            size = await status.size()
            storage_digest = StorageDigest(status._items, size)
            entries.append(storage_digest)
        return entries


async def main():
    if len(sys.argv) != 2:
        raise ValueError('usage: prune.py <project>')
    project = sys.argv[1]

    async with aiogoogle.GoogleContainerClient(
            project=project,
            timeout=aiohttp.ClientTimeout(total=5)) as container_client:
        async with aiogoogle.GoogleStorageAsyncFS(project=project) as fs:
            list_all_digests = ListAllStorageDigests(fs, project)
            list_tracked_digests = ListTrackedDigests(container_client)
            try:
                tracked_digests = set(await list_tracked_digests.run())
                all_digests = await list_all_digests.run()

                digests_to_delete = []
                size_to_delete = 0
                for digest in all_digests:
                    if not digest.digest.startswith('sha256:'):
                        continue
                    recent = digest.updated >= (time_msecs() - 7 * 24 * 60 * 60 * 1000)
                    if digest not in tracked_digests and not recent:
                        size_to_delete += digest.size
                        digests_to_delete.append(digest.path)
                        print(f'digest={digest.digest} path={digest.path} updated={time_msecs_str(digest.updated)} recent={recent}')

                size_str = humanize.naturalsize(size_to_delete, binary=True)
                print(f'deleting {len(digests_to_delete)} digests with total size {size_str}')

                async def rm(path):
                    assert path.startswith(f'gs://artifacts.{project}.appspot.com/containers/images/sha256:')
                    await fs.remove(path)
                    print(f'deleted {path}')

                async with asyncio.Semaphore(8) as sema:
                    async with OnlineBoundedGather2(sema) as pool:
                        tasks = [pool.call(rm, digest_path) for digest_path in digests_to_delete]
                        if tasks:
                            await pool.wait(tasks)
            finally:
                list_tracked_digests.shutdown()


asyncio.get_event_loop().run_until_complete(main())
