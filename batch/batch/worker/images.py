from typing import Optional, Dict, Any, Union, MutableMapping

import os
import asyncio
import logging
import aiorwlock
import shutil
import json
import re
import base64
import aiohttp.client_exceptions
from collections import defaultdict
import concurrent.futures

import aiodocker  # type: ignore
import aiodocker.images
from aiodocker.exceptions import DockerError  # type: ignore

import hailtop.httpx as httpx
from hailtop.batch.hail_genetics_images import HAIL_GENETICS_IMAGES
from hailtop.utils import parse_docker_image_reference, time_msecs, time_msecs_str, blocking_to_async, check_shell, check_exec_output, sleep_and_backoff

from .config import DOCKER_PREFIX, CLOUD, PUBLIC_IMAGES, CLOUD_WORKER_API, BATCH_WORKER_IMAGE_ID
from .credentials import CloudUserCredentials
from .exceptions import ImageCannotBePulled, ImageNotFound

log = logging.getLogger('images')

MAX_DOCKER_IMAGE_PULL_SECS = 20 * 60
MAX_DOCKER_WAIT_SECS = 5 * 60
MAX_DOCKER_OTHER_OPERATION_SECS = 1 * 60

image_manager: Optional['ImageManager'] = None


def compose_auth_header_urlsafe(orig_f):
    def compose(auth: Union[MutableMapping, str, bytes], registry_addr: str = None):
        orig_auth_header = orig_f(auth, registry_addr=registry_addr)
        auth = json.loads(base64.b64decode(orig_auth_header))
        auth_json = json.dumps(auth).encode('ascii')
        return base64.urlsafe_b64encode(auth_json).decode('ascii')

    return compose


# We patched aiodocker's utility function `compose_auth_header` because it does not base64 encode strings
# in urlsafe mode which is required for Azure's credentials.
# https://github.com/aio-libs/aiodocker/blob/17e08844461664244ea78ecd08d1672b1779acc1/aiodocker/utils.py#L297
aiodocker.images.compose_auth_header = compose_auth_header_urlsafe(aiodocker.images.compose_auth_header)


def docker_call_retry(timeout, name):
    async def wrapper(f, *args, **kwargs):
        delay = 0.1
        while True:
            try:
                return await asyncio.wait_for(f(*args, **kwargs), timeout)
            except DockerError as e:
                # 408 request timeout, 503 service unavailable
                if e.status == 408 or e.status == 503:
                    log.warning(f'in docker call to {f.__name__} for {name}, retrying', stack_info=True, exc_info=True)
                # DockerError(500, 'Get https://registry-1.docker.io/v2/: net/http: request canceled while waiting for connection (Client.Timeout exceeded while awaiting headers)
                # DockerError(500, 'error creating overlay mount to /var/lib/docker/overlay2/545a1337742e0292d9ed197b06fe900146c85ab06e468843cd0461c3f34df50d/merged: device or resource busy'
                # DockerError(500, 'Get https://gcr.io/v2/: dial tcp: lookup gcr.io: Temporary failure in name resolution')
                elif e.status == 500 and (
                    "request canceled while waiting for connection" in e.message
                    or re.match("error creating overlay mount.*device or resource busy", e.message)
                    or "Temporary failure in name resolution" in e.message
                ):
                    log.warning(f'in docker call to {f.__name__} for {name}, retrying', stack_info=True, exc_info=True)
                else:
                    raise
            except (aiohttp.client_exceptions.ServerDisconnectedError, asyncio.TimeoutError):
                log.warning(f'in docker call to {f.__name__} for {name}, retrying', stack_info=True, exc_info=True)
                delay = await sleep_and_backoff(delay)

    return wrapper


class ImageData:
    def __init__(self):
        self.ref_count = 0
        self.time_created = time_msecs()
        self.last_accessed = time_msecs()
        self.lock = asyncio.Lock()
        self.extracted = False

    def __add__(self, other):
        self.ref_count += other
        self.last_accessed = time_msecs()
        return self

    def __sub__(self, other):
        self.ref_count -= other
        assert self.ref_count >= 0
        self.last_accessed = time_msecs()
        return self

    def __str__(self):
        data = {
            'ref_count': self.ref_count,
            'time_created': time_msecs_str(self.time_created),
            'last_accessed': time_msecs_str(self.last_accessed)
        }
        return f'{data}'


class Image:
    def __init__(self, name: str, credentials: CloudUserCredentials):
        self.image_name = name
        self.credentials = credentials
        self.image_manager = get_image_manager()

        image_ref = parse_docker_image_reference(name)
        if image_ref.tag is None and image_ref.digest is None:
            log.info(f'adding latest tag to image {name} for {self}')
            image_ref.tag = 'latest'

        if image_ref.name() in HAIL_GENETICS_IMAGES:
            # We want the "hailgenetics/python-dill" translate to (based on the prefix):
            # * gcr.io/hail-vdc/hailgenetics/python-dill
            # * us-central1-docker.pkg.dev/hail-vdc/hail/hailgenetics/python-dill
            image_ref.path = image_ref.name()
            image_ref.domain = DOCKER_PREFIX.split('/', maxsplit=1)[0]
            image_ref.path = '/'.join(DOCKER_PREFIX.split('/')[1:] + [image_ref.path])

        self.image_ref = image_ref
        self.image_ref_str = str(image_ref)
        self.image_config: Optional[Dict[str, Any]] = None
        self.image_id: Optional[str] = None

    @property
    def rootfs_path(self) -> Optional[str]:
        if self.image_id is None:
            return None
        return f'/host/rootfs/{self.image_id}'

    async def _pull_image(self, client_session: httpx.ClientSession):
        is_cloud_image = (CLOUD == 'gcp' and self.image_ref.hosted_in('google')) or (
            CLOUD == 'azure' and self.image_ref.hosted_in('azure')
        )
        is_public_image = self.image_ref.name() in PUBLIC_IMAGES

        try:
            if not is_cloud_image:
                await self._ensure_image_is_pulled()
            elif is_public_image:
                auth = await self._batch_worker_access_token(client_session)
                await self._ensure_image_is_pulled(auth=auth)
            else:
                # Pull to verify this user has access to this
                # image.
                # FIXME improve the performance of this with a
                # per-user image cache.
                auth = self._current_user_access_token()
                await docker_call_retry(MAX_DOCKER_IMAGE_PULL_SECS, f'{self}')(
                    self.image_manager.docker.images.pull, self.image_ref_str, auth=auth
                )
        except DockerError as e:
            if e.status == 404 and 'pull access denied' in e.message:
                raise ImageCannotBePulled() from e
            if 'not found: manifest unknown' in e.message:
                raise ImageNotFound() from e
            raise

        image_config, _ = await check_exec_output('docker', 'inspect', self.image_ref_str)
        self.image_manager.image_configs[self.image_ref_str] = json.loads(image_config)[0]

    async def _ensure_image_is_pulled(self, auth: Optional[Dict[str, str]] = None):
        try:
            await docker_call_retry(MAX_DOCKER_OTHER_OPERATION_SECS, f'{self}')(self.image_manager.docker.images.get, self.image_ref_str)
        except DockerError as e:
            if e.status == 404:
                await docker_call_retry(MAX_DOCKER_IMAGE_PULL_SECS, f'{self}')(
                    self.image_manager.docker.images.pull, self.image_ref_str, auth=auth
                )
            else:
                raise

    async def _batch_worker_access_token(self, client_session: httpx.ClientSession) -> Dict[str, str]:
        return await CLOUD_WORKER_API.worker_access_token(client_session)

    def _current_user_access_token(self) -> Dict[str, str]:
        return {'username': self.credentials.username, 'password': self.credentials.password}

    async def _extract_rootfs(self):
        assert self.rootfs_path
        os.makedirs(self.rootfs_path)
        await check_shell(
            f'id=$(docker create {self.image_id}) && docker export $id | tar -C {self.rootfs_path} -xf - && docker rm $id'
        )

    async def _localize_rootfs(self, client_session: httpx.ClientSession, pool: concurrent.futures.ThreadPoolExecutor):
        async with self.image_manager.image_lock.reader_lock:
            # FIXME Authentication is entangled with pulling images. We need a way to test
            # that a user has access to a cached image without pulling.
            await self._pull_image(client_session)
            self.image_config = self.image_manager.image_configs[self.image_ref_str]
            self.image_id = self.image_config['Id'].split(":")[1]
            assert self.image_id

            self.image_manager.image_data[self.image_id] += 1

            image_data = self.image_manager.image_data[self.image_id]
            async with image_data.lock:
                if not image_data.extracted:
                    try:
                        await self._extract_rootfs()
                        image_data.extracted = True
                        log.info(
                            f'Added expanded image to cache: {self.image_ref_str}, ID: {self.image_id}'
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        log.exception(f'while extracting image {self.image_ref_str}, ID: {self.image_id}')
                        await blocking_to_async(pool, shutil.rmtree, self.rootfs_path)

    async def pull(self, client_session: httpx.ClientSession, pool: concurrent.futures.ThreadPoolExecutor):
        await asyncio.shield(self._localize_rootfs(client_session, pool))

    def prune(self):
        if self.image_id is not None:
            self.image_manager.image_data[self.image_id] -= 1


class ImageManager:
    def __init__(self):
        self.docker = aiodocker.Docker()
        self.image_lock = aiorwlock.RWLock()
        self.image_configs: Dict[str, Dict[str, Any]] = dict()
        self.image_data: Dict[str, ImageData] = defaultdict(ImageData)

    async def close(self):
        await self.docker.close()
        log.info('docker closed')

    async def cleanup_old_images(self, pool: concurrent.futures.ThreadPoolExecutor):
        try:
            async with self.image_lock.writer_lock:
                log.info(f"Obtained writer lock. The image ref counts are: {self.image_data}")
                for image_id in list(self.image_data.keys()):
                    now = time_msecs()
                    image_data = self.image_data[image_id]
                    if image_data.ref_count == 0 and (now - image_data.last_accessed) > 10 * 60 * 1000:
                        assert image_id != BATCH_WORKER_IMAGE_ID
                        log.info(f'Found an unused image with ID {image_id}')
                        await check_shell(f'docker rmi -f {image_id}')
                        image_path = f'/host/rootfs/{image_id}'
                        await blocking_to_async(pool, shutil.rmtree, image_path)
                        del self.image_data[image_id]
                        log.info(f'Deleted image from cache with ID {image_id}')
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.exception(f'Error while deleting unused image: {e}')


def get_image_manager() -> ImageManager:
    global image_manager
    if image_manager is None:
        image_manager = ImageManager()
    return image_manager
