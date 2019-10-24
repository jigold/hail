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


async def throttled_gather(*coros, parallelism=10, return_exceptions=False):
    gatherer = AsyncThrottledGather(*coros,
                                    parallelism=parallelism,
                                    return_exceptions=return_exceptions)
    return await gatherer.wait()


class AsyncThrottledGather:
    def __init__(self, *coros, parallelism=10, return_exceptions=False):
        self.count = len(coros)
        self.n_finished = 0

        self._queue = asyncio.Queue(maxsize=1000)
        self._done = asyncio.Event()
        self._return_exceptions = return_exceptions
        self._futures = []

        self._results = [None] * len(coros)
        self._error = None

        if self.count == 0:
            self._done.set()
            return

        for _ in range(parallelism):
            self._futures.append(asyncio.ensure_future(self._worker()))

        self._futures.append(asyncio.ensure_future(self._fill_queue(*coros)))

    def _cancel_futures(self):
        for fut in self._futures:
            fut.cancel()

    async def _worker(self):
        while True:
            i, coro = await self._queue.get()

            try:
                res = await coro
            except asyncio.CancelledError:
                raise
            except Exception as err:
                res = err
                if not self._return_exceptions:
                    self._error = err
                    self._done.set()
                    return

            self._results[i] = res
            self.n_finished += 1

            if self.n_finished == self.count:
                self._done.set()

    async def _fill_queue(self, *coros):
        for i, coro in enumerate(coros):
            self._done.clear()
            await self._queue.put((i, coro))

    async def wait(self):
        await self._done.wait()
        self._cancel_futures()

        if self._error:
            raise self._error
        else:
            return self._results


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
