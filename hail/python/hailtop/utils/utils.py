import asyncio


def unzip(l):
    a = []
    b = []
    for x, y in l:
        a.append(x)
        b.append(y)
    return a, b


def async_to_blocking(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


async def blocking_to_async(thread_pool, fun, *args, **kwargs):
    return await asyncio.get_event_loop().run_in_executor(
        thread_pool, lambda: fun(*args, **kwargs))


class AsyncWorkerPool:
    def __init__(self, parallelism):
        self._sem = asyncio.Semaphore(parallelism)
        self._count = 0
        self._done = asyncio.Event()

    async def _call(self, f, args, kwargs):
        async with self._sem:
            try:
                await f(*args, **kwargs)
            finally:
                assert self._count > 0
                self._count -= 1
                if self._count == 0:
                    self._done.set()

    async def call(self, f, *args, **kwargs):
        if self._count == 0:
            self._done.clear()
        self._count += 1
        asyncio.ensure_future(self._call(f, args, kwargs))

    async def wait(self):
        await self._done.wait()
