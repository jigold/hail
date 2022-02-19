from typing import Callable, Dict, Optional

import os
import base64

from aiodocker.exceptions import DockerError  # type: ignore

from hailtop.utils import time_msecs, CalledProcessError

from .exceptions import ImageNotFound, ImageCannotBePulled, StepNotRunnableError


def populate_secret_host_path(host_path: str, secret_data: Optional[Dict[str, bytes]]):
    os.makedirs(host_path, exist_ok=True)
    if secret_data is not None:
        for filename, data in secret_data.items():
            with open(f'{host_path}/{filename}', 'wb') as f:
                f.write(base64.b64decode(data))


def user_error(e):
    if isinstance(e, DockerError):
        if e.status == 404 and 'pull access denied' in e.message:
            return True
        if e.status == 404 and ('not found: manifest unknown' in e.message or 'no such image' in e.message):
            return True
        if e.status == 400 and 'executable file not found' in e.message:
            return True
    if isinstance(e, CalledProcessError):
        # Opening GCS connection...\n', b'daemonize.Run: readFromProcess: sub-process: mountWithArgs: mountWithConn:
        # fs.NewServer: create file system: SetUpBucket: OpenBucket: Bad credentials for bucket "BUCKET". Check the
        # bucket name and your credentials.\n')
        if b'Bad credentials for bucket' in e.stderr:
            return True
    if isinstance(e, (ImageNotFound, ImageCannotBePulled)):
        return True
    return False


class Timings:
    def __init__(self, is_cancelled: Callable[[], bool]):
        self.timings: Dict[str, Dict[str, float]] = dict()
        self.is_cancelled = is_cancelled

    def step(self, name: str, ignore_cancellation: bool = False):
        assert name not in self.timings
        self.timings[name] = dict()
        return StepManager(self.timings[name], self.is_cancelled, ignore_cancellation=ignore_cancellation)

    def to_dict(self):
        return self.timings


class StepManager:
    def __init__(self, timing: Dict[str, float], is_cancelled: Callable[[], bool], ignore_cancellation: bool = False):
        self.timing: Dict[str, float] = timing
        self.is_cancelled = is_cancelled
        self.ignore_cancellation = ignore_cancellation

    def __enter__(self):
        if self.is_cancelled() and not self.ignore_cancellation:
            raise StepNotRunnableError()
        self.timing['start_time'] = time_msecs()

    def __exit__(self, exc_type, exc, tb):
        if self.is_cancelled() and not self.ignore_cancellation:
            return
        finish_time = time_msecs()
        self.timing['finish_time'] = finish_time
        self.timing['duration'] = finish_time - self.timing['start_time']
