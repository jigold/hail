import asyncio
import sortedcontainers
import logging
import time
import math

from .utils import new_token
from .batch_configuration import BATCH_NAMESPACE, BATCH_IMAGE, INSTANCE_ID, PROJECT, ZONE
from .instance import Instance
from .globals import get_db

log = logging.getLogger('instance_pool')
db = get_db()


class InstancePool:
    def __init__(self, driver, worker_type, worker_cores,
                 worker_disk_size_gb, pool_size, max_instances):
        self.driver = driver
        self.worker_type = worker_type
        self.worker_cores = worker_cores

        if worker_type == 'standard':
            m = 3.75
        elif worker_type == 'highmem':
            m = 6.5
        else:
            assert worker_type == 'highcpu', worker_type
            m = 0.9
        self.worker_memory = 0.9 * m

        self.worker_capacity = 2 * worker_cores
        self.worker_disk_size_gb = worker_disk_size_gb
        self.pool_size = pool_size
        self.max_instances = max_instances

        self.token = new_token()
        # self.machine_name_prefix = f'batch2-agent-{BATCH_NAMESPACE}-{INSTANCE_ID[:8]}-'
        self.machine_name_prefix = f'batch2-agent-{BATCH_NAMESPACE}-'

        self.worker_logs_directory = f'gs://{self.driver.batch_bucket}/{BATCH_NAMESPACE}/{INSTANCE_ID}'
        log.info(f'writing worker logs to {self.worker_logs_directory}')

        self.instances = sortedcontainers.SortedSet(key=lambda inst: inst.last_updated)

        # for active instances only
        self.instances_by_free_cores = sortedcontainers.SortedSet(key=lambda inst: inst.free_cores)

        self.n_pending_instances = 0
        self.n_active_instances = 0

        # for pending and active
        self.free_cores = 0

        self.token_inst = {}

    def token_machine_name(self, inst_token):
        return f'{self.machine_name_prefix}{inst_token}'

    async def initialize(self):
        log.info('initializing instance pool')
        for record in await db.instances.get_all_records():
            inst = Instance.from_record(self, record)
            self.token_inst[inst.token] = inst
            self.instances.add(inst)

    async def start(self):
        log.info('healing instance pool')
        await asyncio.gather(*[inst.heal() for inst in self.instances])

        log.info('starting instance pool')
        asyncio.ensure_future(self.control_loop())
        asyncio.ensure_future(self.event_loop())
        asyncio.ensure_future(self.heal_loop())
        log.info('instance pool started')

    async def create_instance(self):
        while True:
            inst_token = new_token()
            if inst_token not in self.token_inst:
                break
        # reserve
        self.token_inst[inst_token] = None

        log.info(f'creating instance {inst_token}')

        machine_name = self.token_machine_name(inst_token)
        config = {
            'name': machine_name,
            'machineType': f'projects/{PROJECT}/zones/{ZONE}/machineTypes/n1-{self.worker_type}-{self.worker_cores}',
            'labels': {
                'role': 'batch2-agent',
                'inst_token': inst_token,
                'batch_instance': INSTANCE_ID,
                'namespace': BATCH_NAMESPACE
            },

            'disks': [{
                'boot': True,
                'autoDelete': True,
                'diskSizeGb': self.worker_disk_size_gb,
                'initializeParams': {
                    'sourceImage': 'projects/hail-vdc/global/images/batch-agent-2',
                }
            }],

            'networkInterfaces': [{
                'network': 'global/networks/default',
                'networkTier': 'PREMIUM',
                'accessConfigs': [{
                    'type': 'ONE_TO_ONE_NAT',
                    'name': 'external-nat'
                }]
            }],

            'scheduling': {
                'automaticRestart': False,
                'onHostMaintenance': "TERMINATE",
                'preemptible': True
            },

            'serviceAccounts': [{
                'email': 'batch2-agent@hail-vdc.iam.gserviceaccount.com',
                'scopes': [
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            }],

            'metadata': {
                'items': [{
                    'key': 'startup-script',
                    'value': f'''
#!/bin/bash
set -ex

export BATCH_IMAGE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_image")
export HOME=/root

docker run -v /var/run/docker.sock:/var/run/docker.sock \
           -v /batch:/batch \
           -p 5000:5000 \
           -d --entrypoint "/bin/bash" \
           $BATCH_IMAGE \
           -c "sh /run-worker.sh"
'''
                }, {
                    'key': 'inst_token',
                    'value': inst_token
                }, {
                    'key': 'batch_image',
                    'value': BATCH_IMAGE
                }, {
                    'key': 'batch_instance',
                    'value': INSTANCE_ID
                }, {
                    'key': 'namespace',
                    'value': BATCH_NAMESPACE
                }, {
                    'key': 'worker_logs_directory',
                    'value': self.worker_logs_directory
                }]
            },
            'tags': {
                'items': [
                    "batch2-agent"
                ]
            },
        }

        await self.driver.gservices.create_instance(config)
        log.info(f'created machine {machine_name}')

        inst = await Instance.create(self, machine_name, inst_token)

        # inst = Instance.create(
        #     inst_pool=self,
        #     name=machine_name,
        #     token=inst_token,
        #     cores=self.worker_cores,
        #     capacity=self.worker_capacity,
        #     memory=self.worker_memory,
        #     disk_size=self.worker_disk_size_gb,
        #     machine_type=self.worker_type
        # )

        self.token_inst[inst_token] = inst
        self.instances.add(inst)

        log.info(f'created {inst}')

        return inst

    async def handle_event(self, event):
        if not event.payload:
            log.warning(f'event has no payload')
            return

        payload = event.payload
        version = payload['version']
        if version != '1.2':
            log.warning('unknown event verison {version}')
            return

        resource_type = event.resource.type
        if resource_type != 'gce_instance':
            log.warning(f'unknown event resource type {resource_type}')
            return

        event_type = payload['event_type']
        event_subtype = payload['event_subtype']
        resource = payload['resource']
        name = resource['name']

        log.info(f'event {version} {resource_type} {event_type} {event_subtype} {name}')

        if not name.startswith(self.machine_name_prefix):
            log.warning(f'event for unknown machine {name}')
            return

        inst_token = name[len(self.machine_name_prefix):]
        inst = self.token_inst.get(inst_token)
        if not inst:
            log.warning(f'event for unknown instance {inst_token}')
            return

        if event_subtype == 'compute.instances.preempted':
            log.info(f'event handler: handle preempt {inst}')
            await inst.handle_preempt_event()
        elif event_subtype == 'compute.instances.delete':
            if event_type == 'GCE_OPERATION_DONE':
                log.info(f'event handler: remove {inst}')
                await inst.remove()
            elif event_type == 'GCE_API_CALL':
                log.info(f'event handler: handle call delete {inst}')
                await inst.handle_call_delete_event()
            else:
                log.warning(f'unknown event type {event_type}')
        else:
            log.warning(f'unknown event subtype {event_subtype}')

    async def event_loop(self):
        while True:
            try:
                async for event in await self.driver.gservices.stream_entries():
                    await self.handle_event(event)
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception('event loop failed due to exception')
            await asyncio.sleep(15)

    async def heal_loop(self):
        while True:
            try:
                if self.instances:
                    # 0 is the smallest (oldest)
                    inst = self.instances[0]
                    inst_age = time.time() - inst.last_updated
                    if inst_age > 60:
                        log.info(f'heal: oldest {inst} age {inst_age}s')
                        await inst.heal()
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception('instance pool heal loop: caught exception')

            await asyncio.sleep(1)

    async def control_loop(self):
        while True:
            try:
                log.info(f'n_pending_instances {self.n_pending_instances}'
                         f' n_active_instances {self.n_active_instances}'
                         f' pool_size {self.pool_size}'
                         f' n_instances {len(self.instances)}'
                         f' max_instances {self.max_instances}'
                         f' free_cores {self.free_cores}'
                         f' ready_cores {self.driver.ready_cores}')

                if self.driver.ready_cores > 0:
                    instances_needed = (math.ceil(self.driver.ready_cores - self.free_cores) + self.worker_capacity - 1) // self.worker_capacity
                    instances_needed = min(instances_needed,
                                           self.pool_size - (self.n_pending_instances + self.n_active_instances),
                                           self.max_instances - len(self.instances),
                                           # 20 queries/s; our GCE long-run quota
                                           300)
                    if instances_needed > 0:
                        log.info(f'creating {instances_needed} new instances')
                        # parallelism will be bounded by thread pool
                        await asyncio.gather(*[self.create_instance() for _ in range(instances_needed)])
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:  # pylint: disable=broad-except
                log.exception('instance pool control loop: caught exception')

            await asyncio.sleep(15)
