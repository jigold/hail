import errno
import random
import functools
import logging
import asyncio
import aiohttp
from aiohttp import web
import threading

log = logging.getLogger('hailtop.utils')


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


class GatheringFuture(asyncio.futures.Future):
    def __init__(self, loop=None):
        super().__init__(loop=loop)
        self._children = []

    def add_child(self, fut):
        self._children.append(fut)

    def cancel(self):
        if self.done():
            return False
        ret = False
        for child in self._children:
            if child.cancel():
                ret = True
        return ret


def throttled_gather(*coros, parallelism=10, return_exceptions=False):
    gatherer = AsyncThrottledGather(*coros, parallelism=parallelism, return_exceptions=return_exceptions)
    return gatherer.wait()


class AsyncThrottledGather:
    def __init__(self, *coros, parallelism=10, return_exceptions=False):
        self._queue = asyncio.Queue(maxsize=1000)
        self._count = len(coros)
        self._outer = GatheringFuture()
        self.return_exceptions = return_exceptions
        self.results = [None] * len(coros)
        self.n_finished = 0

        self._futures = []
        for _ in range(parallelism):
            self._futures.append(asyncio.ensure_future(self._worker()))

        self._futures.append(asyncio.ensure_future(self._fill_queue(*coros)))

    def _callback(self, i, fut):
        if self._outer.done():
            if not fut.cancelled():
                # Mark exception retrieved.
                fut.exception()
            return

        if fut.cancelled():
            res = asyncio.futures.CancelledError()
            if not self.return_exceptions:
                self._outer.set_exception(res)
                return
        elif fut._exception is not None:
            res = fut.exception()  # Mark exception retrieved.
            if not self.return_exceptions:
                self._outer.set_exception(res)
                return
        else:
            res = fut.result()

        self.results[i] = res
        self.n_finished += 1

        if self.n_finished == self._count:
            [fut.cancel() for fut in self._futures]
            self._outer.set_result(self.results)

    async def _worker(self):
        while True:
            i, coro = await self._queue.get()
            fut = asyncio.ensure_future(coro)
            self._outer.add_child(fut)
            fut.add_done_callback(functools.partial(self._callback, i))
            await asyncio.wait_for(fut, timeout=None)

    async def _fill_queue(self, *coros):
        for i, coro in enumerate(coros):
            await self._queue.put((i, coro))

    def wait(self):
        return self._outer


class AsyncWorkerPool:
    def __init__(self, parallelism, queue_size=1000):
        self._queue = asyncio.Queue(maxsize=queue_size)

        for _ in range(parallelism):
            asyncio.ensure_future(self._worker())

    async def _worker(self):
        while True:
            f, args, kwargs = await self._queue.get()
            try:
                await f(*args, **kwargs)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception(f'worker pool caught exception')

    async def call(self, f, *args, **kwargs):
        await self._queue.put((f, args, kwargs))


def is_transient_error(e):
    # observed exceptions:
    # aiohttp.client_exceptions.ClientConnectorError: Cannot connect to host <host> ssl:None [Connect call failed ('<ip>', 80)]
    #
    # concurrent.futures._base.TimeoutError
    #   from aiohttp/helpers.py:585:TimerContext: raise asyncio.TimeoutError from None
    #
    # Connected call failed caused by:
    # OSError: [Errno 113] Connect call failed ('<ip>', 80)
    # 113 is EHOSTUNREACH: No route to host
    if isinstance(e, aiohttp.ClientResponseError):
        # nginx returns 502 if it cannot connect to the upstream server
        # 408 request timeout, 502 bad gateway, 503 service unavailable, 504 gateway timeout
        if e.status == 408 or e.status == 502 or e.status == 503 or e.status == 504:
            return True
    elif isinstance(e, aiohttp.ClientOSError):
        if (e.errno == errno.ETIMEDOUT or
                e.errno == errno.ECONNREFUSED or
                e.errno == errno.EHOSTUNREACH):
            return True
    elif isinstance(e, aiohttp.ServerTimeoutError):
        return True
    elif isinstance(e, asyncio.TimeoutError):
        return True
    return False


async def request_retry_transient_errors(session, method, url, **kwargs):
    delay = 0.1
    while True:
        try:
            return await session.request(method, url, **kwargs)
        except Exception as e:  # pylint: disable=broad-except
            if is_transient_error(e):
                pass
            else:
                raise
        # exponentially back off, up to (expected) max of 30s
        delay = min(delay * 2, 60.0)
        t = delay * random.random()
        await asyncio.sleep(t)


async def request_raise_transient_errors(session, method, url, **kwargs):
    try:
        return await session.request(method, url, **kwargs)
    except Exception as e:  # pylint: disable=broad-except
        if is_transient_error(e):
            log.exception('request failed with transient exception: {method} {url}')
            raise web.HTTPServiceUnavailable()
        raise
