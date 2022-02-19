import warnings

from hailtop.hail_logging import configure_logging

# configure logging before importing anything else
configure_logging()


oldwarn = warnings.warn


def deeper_stack_level_warn(*args, **kwargs):
    if 'stacklevel' in kwargs:
        kwargs['stacklevel'] = max(kwargs['stacklevel'], 5)
    else:
        kwargs['stacklevel'] = 5
    return oldwarn(*args, **kwargs)


warnings.warn = deeper_stack_level_warn


from .worker import run  # noqa: E402 pylint: disable=wrong-import-position

run()
