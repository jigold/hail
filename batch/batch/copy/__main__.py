from typing import Union, List, Optional
import sys
import json
import asyncio
import resource
import logging
import humanize
from concurrent.futures import ThreadPoolExecutor
from hailtop.aiotools.fs import RouterAsyncFS, LocalAsyncFS, Transfer
from hailtop.aiogoogle import GoogleStorageAsyncFS
from hailtop.hail_logging import configure_logging
from hailtop.aiotools import BackgroundTaskManager

configure_logging()

log = logging.getLogger('copy')

# import tracemalloc
#
#
# tracemalloc.start()


class MemoryMonitor:
    def __init__(self):
        self.keep_measuring = True

    async def measure_usage(self):
        max_usage = 0
        while self.keep_measuring:
            usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            max_usage = max(
                max_usage,
                usage
            )
            log.info(f'memory usage {humanize.naturalsize(usage * 1024, binary=True)} '
                     f'max memory usage {humanize.naturalsize(max_usage * 1024, binary=True)}')
            await asyncio.sleep(15)

        return max_usage


async def copy(requester_pays_project: Optional[str], transfer: Union[Transfer, List[Transfer]]) -> None:
    if requester_pays_project:
        params = {'userProject': requester_pays_project}
    else:
        params = None
    with ThreadPoolExecutor() as thread_pool:
        async with RouterAsyncFS('file', [LocalAsyncFS(thread_pool), GoogleStorageAsyncFS(params=params)]) as fs:
            sema = asyncio.Semaphore(50)
            async with sema:
                copy_report = await fs.copy(sema, transfer)
                copy_report.summarize()


async def main() -> None:
    task_manager = BackgroundTaskManager()

    try:
        assert len(sys.argv) == 3
        requster_pays_project = json.loads(sys.argv[1])
        files = json.loads(sys.argv[2])

        memory_monitor = MemoryMonitor()
        task_manager.ensure_future(memory_monitor.measure_usage())
        await copy(
            requster_pays_project, [Transfer(f['from'], f['to'], treat_dest_as=Transfer.DEST_IS_TARGET) for f in files]
        )
    finally:
        print(sys.exc_info())
        task_manager.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
