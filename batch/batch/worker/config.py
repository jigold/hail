import os
import json
import logging
import base64

from ..publicly_available_images import publicly_available_images
from ..worker.worker_api import CloudWorkerAPI
from ..cloud.gcp.worker.worker_api import GCPWorkerAPI
from ..cloud.azure.worker.worker_api import AzureWorkerAPI


log = logging.getLogger('env')

with open('/subdomains.txt', 'r') as subdomains_file:
    HAIL_SERVICES = [line.rstrip() for line in subdomains_file.readlines()]

CLOUD = os.environ['CLOUD']
CORES = int(os.environ['CORES'])
NAME = os.environ['NAME']
NAMESPACE = os.environ['NAMESPACE']
# ACTIVATION_TOKEN
IP_ADDRESS = os.environ['IP_ADDRESS']
INTERNAL_GATEWAY_IP = os.environ['INTERNAL_GATEWAY_IP']
BATCH_LOGS_STORAGE_URI = os.environ['BATCH_LOGS_STORAGE_URI']
INSTANCE_ID = os.environ['INSTANCE_ID']
DOCKER_PREFIX = os.environ['DOCKER_PREFIX']
PUBLIC_IMAGES = publicly_available_images(DOCKER_PREFIX)
INSTANCE_CONFIG = json.loads(base64.b64decode(os.environ['INSTANCE_CONFIG']).decode())
MAX_IDLE_TIME_MSECS = int(os.environ['MAX_IDLE_TIME_MSECS'])
BATCH_WORKER_IMAGE = os.environ['BATCH_WORKER_IMAGE']
BATCH_WORKER_IMAGE_ID = os.environ['BATCH_WORKER_IMAGE_ID']
INTERNET_INTERFACE = os.environ['INTERNET_INTERFACE']
UNRESERVED_WORKER_DATA_DISK_SIZE_GB = int(os.environ['UNRESERVED_WORKER_DATA_DISK_SIZE_GB'])
assert UNRESERVED_WORKER_DATA_DISK_SIZE_GB >= 0
ACCEPTABLE_QUERY_JAR_URL_PREFIX = os.environ['ACCEPTABLE_QUERY_JAR_URL_PREFIX']
assert len(ACCEPTABLE_QUERY_JAR_URL_PREFIX) > 3  # x:// where x is one or more characters

CLOUD_WORKER_API: CloudWorkerAPI = GCPWorkerAPI.from_env() if CLOUD == 'gcp' else AzureWorkerAPI.from_env()

log.info(f'CLOUD {CLOUD}')
log.info(f'CORES {CORES}')
log.info(f'NAME {NAME}')
log.info(f'NAMESPACE {NAMESPACE}')
# ACTIVATION_TOKEN
log.info(f'IP_ADDRESS {IP_ADDRESS}')
log.info(f'BATCH_LOGS_STORAGE_URI {BATCH_LOGS_STORAGE_URI}')
log.info(f'INSTANCE_ID {INSTANCE_ID}')
log.info(f'DOCKER_PREFIX {DOCKER_PREFIX}')
log.info(f'INSTANCE_CONFIG {INSTANCE_CONFIG}')
log.info(f'CLOUD_WORKER_API {CLOUD_WORKER_API}')
log.info(f'MAX_IDLE_TIME_MSECS {MAX_IDLE_TIME_MSECS}')
log.info(f'INTERNET_INTERFACE {INTERNET_INTERFACE}')
log.info(f'UNRESERVED_WORKER_DATA_DISK_SIZE_GB {UNRESERVED_WORKER_DATA_DISK_SIZE_GB}')
log.info(f'ACCEPTABLE_QUERY_JAR_URL_PREFIX {ACCEPTABLE_QUERY_JAR_URL_PREFIX}')

instance_config = CLOUD_WORKER_API.instance_config_from_config_dict(INSTANCE_CONFIG)

assert instance_config.cores == CORES
assert instance_config.cloud == CLOUD
