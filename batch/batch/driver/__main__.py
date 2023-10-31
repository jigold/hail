import asyncio
import logging

from hailtop.hail_logging import configure_logging

# configure logging before importing anything else
configure_logging(logging.DEBUG)

import sys  # noqa: E402 pylint: disable=wrong-import-position
import traceback  # noqa: E402 pylint: disable=wrong-import-position

import aiohttp  # noqa: E402 pylint: disable=wrong-import-position
import uvloop

from .main import run  # noqa: E402 pylint: disable=wrong-import-position

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

oldinit = aiohttp.ClientSession.__init__  # type: ignore


def newinit(self, *args, **kwargs):
    oldinit(self, *args, **kwargs)
    self._source_traceback = traceback.extract_stack(sys._getframe(1))


aiohttp.ClientSession.__init__ = newinit  # type: ignore


run()
