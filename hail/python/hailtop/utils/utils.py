import errno
import random
import logging
import time
import asyncio
import aiohttp
from aiohttp import web

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


class AsyncWorkerPool:
    def __init__(self, parallelism):
        self._sem = asyncio.Semaphore(parallelism)
        self._count = 0
        self._done = asyncio.Event()

    async def _call(self, f, args, kwargs):
        async with self._sem:
            try:
                await f(*args, **kwargs)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception(f'worker pool caught exception')
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


def is_transient_error(e):
    # observed exceptions:
    # aiohttp.client_exceptions.ClientConnectorError: Cannot connect to host <host> ssl:None [Connect call failed ('<ip>', 80)]
    # concurrent.futures._base.TimeoutError
    #   from aiohttp/helpers.py:585:TimerContext: raise asyncio.TimeoutError from None
    if isinstance(e, aiohttp.ClientResponseError):
        # 408 request timeout, 503 service unavailable, 504 gateway timeout
        if e.status == 408 or e.status == 503 or e.status == 504:
            return True
    elif isinstance(e, aiohttp.ClientOSError):
        if e.errno == errno.ETIMEDOUT or e.errno == errno.ECONNREFUSED:
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
            start = time.time()
            result = await session.request(method, url, **kwargs)
            log.info(f'submitted request in {round(time.time() - start, 3)} seconds')
            return result
        except Exception as e:  # pylint: disable=broad-except
            if is_transient_error(e):
                log.info(f'got transient error {e}')
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
