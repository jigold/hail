import asyncio
import sortedcontainers
import random
import logging
import secrets
import base64
import json

from hailtop import aiotools
from hailtop.utils import secret_alnum_string, retry_long_running

from .instance import Instance
from .scheduler import Scheduler
from ..batch_configuration import DEFAULT_NAMESPACE, PROJECT, \
    WORKER_MAX_IDLE_TIME_MSECS, BATCH_WORKER_IMAGE
from ..worker_config import WorkerConfig

log = logging.getLogger('pool')


# class PoolConfig:
#     def __init__(self, family, typ, cores, preemptible, private):
#         self.family = family
#         self.type = typ
#         self.cores = cores
#         self.preemptible = preemptible
#         self.private = private
#
    # def is_pool_compatible(self, pool, standing=False):
    #     if (pool.family == self.family and
    #             pool.type == self.type and
    #             pool.preemptible == self.preemptible and
    #             pool.private == self.private):
    #         if not standing and self.cores <= pool.cores:
    #             return True
    #         elif standing and self.cores == pool.cores:
    #             return True
    #     return False
    #
    # def find_optimal_pool(self, pools, standing=False):
    #     best_pool = None
    #     for pool in pools:
    #         if not (pool.family == self.family and
    #                 pool.type == self.type and
    #                 pool.preemptible == self.preemptible and
    #                 pool.private == self.private):
    #             continue
    #
    #         if standing or self.private:
    #             if self.cores == pool.cores:
    #                 best_pool = pool
    #         else:
    #             if not best_pool and self.cores <= pool.cores:
    #                 best_pool = pool
    #             elif best_pool and best_pool.cores < pool.cores:
    #                 best_pool = pool
    #     return best_pool
    #
    # def to_dict(self):
    #     return {
    #         'family': self.family,
    #         'type': self.type,
    #         'cores': self.cores,
    #         'preemptible': self.preemptible,
    #         'private': self.private
    #     }
    #
    # def __str__(self):
    #     return f'{self.to_dict()}'

# worker_disk_size_gb, worker_local_ssd_data_disk, worker_pd_ssd_data_disk_size_gb

class Pool:
    @staticmethod
    def from_record(app, record):
        return Pool(app, record['id'], record['family'], record['type'],
                    record['cores'], record['preemptible'], record['disk_size_gb'],
                    record['local_ssd_data_disk'], record['pd_ssd_data_disk_size_gb'],
                    record['pool_size'], record['max_instances'])

    def __init__(self, app, id, family, typ, cores, preemptible,
                 disk_size_gb, local_ssd_data_disk, pd_ssd_data_disk_size_gb,
                 pool_size, max_instances):
        self.app = app
        self.db = app['db']
        self.zone_monitor = app['zone_monitor']
        self.instance_monitor = app['inst_monitor']
        self.pool_manager = app['pool_manager']
        self.log_store = app['log_store']
        self.compute_client = app['compute_client']

        self.id = id
        self.family = family
        self.type = typ
        self.cores = cores
        self.preemptible = preemptible
        self.disk_size_gb = disk_size_gb
        self.local_ssd_data_disk = local_ssd_data_disk
        self.pd_ssd_data_disk_size_gb = pd_ssd_data_disk_size_gb
        self.pool_size = pool_size
        self.max_instances = max_instances

        self.scheduler = None

        self.healthy_instances_by_free_cores = sortedcontainers.SortedSet(
            key=lambda instance: instance.free_cores_mcpu)

        self.n_instances = 0

        self.n_instances_by_state = {
            'pending': 0,
            'active': 0,
            'inactive': 0,
            'deleted': 0
        }

        # pending and active
        self.live_free_cores_mcpu = 0
        self.live_total_cores_mcpu = 0

        self.task_manager = aiotools.BackgroundTaskManager()

    def shutdown(self):
        try:
            self.scheduler.shutdown()
        finally:
            self.task_manager.shutdown()

    async def async_init(self):
        self.scheduler = Scheduler(self.app, self)

    async def run(self):
        await self.scheduler.async_init()

        self.task_manager.ensure_future(retry_long_running(
            'control_loop',
            self.control_loop))

    def config(self):
        return {
            'id': self.id,
            'family': self.family,
            'type': self.type,
            'cores': self.cores,
            'preemptible': self.preemptible
        }

    def adjust_for_remove_instance(self, instance):
        self.n_instances_by_state[instance.state] -= 1
        self.n_instances -= 1

        if instance.state in ('pending', 'active'):
            self.live_free_cores_mcpu -= max(0, instance.free_cores_mcpu)
            self.live_total_cores_mcpu -= instance.cores_mcpu
        if instance in self.healthy_instances_by_free_cores:
            self.healthy_instances_by_free_cores.remove(instance)

    def adjust_for_add_instance(self, instance):
        self.n_instances_by_state[instance.state] += 1
        self.n_instances += 1

        if instance.state in ('pending', 'active'):
            self.live_free_cores_mcpu += max(0, instance.free_cores_mcpu)
            self.live_total_cores_mcpu += instance.cores_mcpu
        if (instance.state == 'active'
                and instance.failed_request_count <= 1):
            self.healthy_instances_by_free_cores.add(instance)

    async def create_instance(self, max_idle_time_msecs=None):
        if max_idle_time_msecs is None:
            max_idle_time_msecs = WORKER_MAX_IDLE_TIME_MSECS

        while True:
            # 36 ** 5 = ~60M
            suffix = secret_alnum_string(5, case='lower')
            machine_name = f'{self.instance_monitor.machine_name_prefix}{suffix}'
            if machine_name not in self.instance_monitor.name_instance:
                break

        if self.instance_monitor.live_total_cores_mcpu // 1000 < 4_000:
            zones = ['us-central1-a', 'us-central1-b', 'us-central1-c', 'us-central1-f']
            zone = random.choice(zones)
        else:
            zone_prob_weights = [
                min(w, 10) * self.zone_monitor.zone_success_rate.zone_success_rate(z)
                for z, w in zip(self.zone_monitor.zones, self.zone_monitor.zone_weights)]

            log.info(f'zones {self.zone_monitor.zones}')
            log.info(f'zone_weights {self.zone_monitor.zone_weights}')
            log.info(f'zone_success_rate {self.zone_monitor.zone_success_rate}')
            log.info(f'zone_prob_weights {zone_prob_weights}')

            zone = random.choices(self.zone_monitor.zones, zone_prob_weights)[0]

        activation_token = secrets.token_urlsafe(32)
        instance = await Instance.create(self.app, machine_name, activation_token,
                                         self.cores * 1000, zone, self.id)
        self.instance_monitor.add_instance(instance)

        log.info(f'created {instance}')

        if self.local_ssd_data_disk:
            worker_data_disk = {
                'type': 'SCRATCH',
                'autoDelete': True,
                'interface': 'NVME',
                'initializeParams': {
                    'diskType': f'zones/{zone}/diskTypes/local-ssd'
                }}
            worker_data_disk_name = 'nvme0n1'
        else:
            worker_data_disk = {
                'autoDelete': True,
                'initializeParams': {
                    'diskType': f'projects/{PROJECT}/zones/{zone}/diskTypes/pd-ssd',
                    'diskSizeGb': str(self.pd_ssd_data_disk_size_gb)
                }
            }
            worker_data_disk_name = 'sdb'

        config = {
            'name': machine_name,
            'machineType': f'projects/{PROJECT}/zones/{zone}/machineTypes/{self.family}-{self.type}-{self.cores}',
            'labels': {
                'role': 'batch2-agent',
                'namespace': DEFAULT_NAMESPACE
            },

            'disks': [{
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': f'projects/{PROJECT}/global/images/batch-worker-12',
                    'diskType': f'projects/{PROJECT}/zones/{zone}/diskTypes/pd-ssd',
                    'diskSizeGb': str(self.disk_size_gb)
                }
            }, worker_data_disk],

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
                'preemptible': self.preemptible
            },

            'serviceAccounts': [{
                'email': f'batch2-agent@{PROJECT}.iam.gserviceaccount.com',
                'scopes': [
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            }],

            'metadata': {
                'items': [{
                    'key': 'startup-script',
                    'value': '''
#!/bin/bash
set -x

curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/run_script"  >./run.sh

nohup /bin/bash run.sh >run.log 2>&1 &
'''
                }, {
                    'key': 'run_script',
                    'value': rf'''
#!/bin/bash
set -x

# only allow udp/53 (dns) to metadata server
# -I inserts at the head of the chain, so the ACCEPT rule runs first
iptables -I DOCKER-USER -d 169.254.169.254 -j DROP
iptables -I DOCKER-USER -d 169.254.169.254 -p udp -m udp --destination-port 53 -j ACCEPT

docker network create public --opt com.docker.network.bridge.name=public
docker network create private --opt com.docker.network.bridge.name=private
# make the internal network not route-able
iptables -I DOCKER-USER -i public -d 10.0.0.0/8 -j DROP
# make other docker containers not route-able
iptables -I DOCKER-USER -i public -d 172.16.0.0/12 -j DROP
# not used, but ban it anyway!
iptables -I DOCKER-USER -i public -d 192.168.0.0/16 -j DROP

WORKER_DATA_DISK_NAME="{worker_data_disk_name}"

# format local SSD
sudo mkfs.xfs -m reflink=1 /dev/$WORKER_DATA_DISK_NAME
sudo mkdir -p /mnt/disks/$WORKER_DATA_DISK_NAME
sudo mount -o prjquota /dev/$WORKER_DATA_DISK_NAME /mnt/disks/$WORKER_DATA_DISK_NAME
sudo chmod a+w /mnt/disks/$WORKER_DATA_DISK_NAME
XFS_DEVICE=$(xfs_info /mnt/disks/$WORKER_DATA_DISK_NAME | head -n 1 | awk '{{ print $1 }}' | awk  'BEGIN {{ FS = "=" }}; {{ print $2 }}')

# reconfigure docker to use local SSD
sudo service docker stop
sudo mv /var/lib/docker /mnt/disks/$WORKER_DATA_DISK_NAME/docker
sudo ln -s /mnt/disks/$WORKER_DATA_DISK_NAME/docker /var/lib/docker
sudo service docker start

# reconfigure /batch and /logs and /gcsfuse to use local SSD
sudo mkdir -p /mnt/disks/$WORKER_DATA_DISK_NAME/batch/
sudo ln -s /mnt/disks/$WORKER_DATA_DISK_NAME/batch /batch

sudo mkdir -p /mnt/disks/$WORKER_DATA_DISK_NAME/logs/
sudo ln -s /mnt/disks/$WORKER_DATA_DISK_NAME/logs /logs

sudo mkdir -p /mnt/disks/$WORKER_DATA_DISK_NAME/gcsfuse/
sudo ln -s /mnt/disks/$WORKER_DATA_DISK_NAME/gcsfuse /gcsfuse

sudo mkdir -p /mnt/disks/$WORKER_DATA_DISK_NAME/xfsquota/
sudo ln -s /mnt/disks/$WORKER_DATA_DISK_NAME/xfsquota /xfsquota

touch /xfsquota/projects
touch /xfsquota/projid

ln -s /xfsquota/projects /etc/projects
ln -s /xfsquota/projid /etc/projid

export HOME=/root

CORES=$(nproc)
NAMESPACE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/namespace")
ACTIVATION_TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/activation_token")
IP_ADDRESS=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip")
PROJECT=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/project/project-id")

BATCH_LOGS_BUCKET_NAME=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_logs_bucket_name")
WORKER_LOGS_BUCKET_NAME=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/worker_logs_bucket_name")
INSTANCE_ID=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/instance_id")
WORKER_CONFIG=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/worker_config")
MAX_IDLE_TIME_MSECS=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/max_idle_time_msecs")
NAME=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
ZONE=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')

BATCH_WORKER_IMAGE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_worker_image")

# Setup fluentd
touch /worker.log
touch /run.log

sudo rm /etc/google-fluentd/config.d/*  # remove unused config files

sudo tee /etc/google-fluentd/config.d/syslog.conf <<EOF
<source>
  @type tail
  format syslog
  path /var/log/syslog
  pos_file /var/lib/google-fluentd/pos/syslog.pos
  read_from_head true
  tag syslog
</source>
EOF

sudo tee /etc/google-fluentd/config.d/worker-log.conf <<EOF {{
<source>
    @type tail
    format json
    path /worker.log
    pos_file /var/lib/google-fluentd/pos/worker-log.pos
    read_from_head true
    tag worker.log
</source>

<filter worker.log>
    @type record_transformer
    enable_ruby
    <record>
        severity \${{ record["levelname"] }}
        timestamp \${{ record["asctime"] }}
    </record>
</filter>
EOF

sudo tee /etc/google-fluentd/config.d/run-log.conf <<EOF
<source>
    @type tail
    format none
    path /run.log
    pos_file /var/lib/google-fluentd/pos/run-log.pos
    read_from_head true
    tag run.log
</source>
EOF

sudo cp /etc/google-fluentd/google-fluentd.conf /etc/google-fluentd/google-fluentd.conf.bak
head -n -1 /etc/google-fluentd/google-fluentd.conf.bak | sudo tee /etc/google-fluentd/google-fluentd.conf
sudo tee -a /etc/google-fluentd/google-fluentd.conf <<EOF
  labels {{
    "namespace": "$NAMESPACE",
    "instance_id": "$INSTANCE_ID"
  }}
</match>
EOF
rm /etc/google-fluentd/google-fluentd.conf.bak

sudo service google-fluentd restart

# retry once
docker pull $BATCH_WORKER_IMAGE || \
    (echo 'pull failed, retrying' && sleep 15 && docker pull $BATCH_WORKER_IMAGE)

# So here I go it's my shot.
docker run \
    -e CORES=$CORES \
    -e NAME=$NAME \
    -e NAMESPACE=$NAMESPACE \
    -e ACTIVATION_TOKEN=$ACTIVATION_TOKEN \
    -e IP_ADDRESS=$IP_ADDRESS \
    -e BATCH_LOGS_BUCKET_NAME=$BATCH_LOGS_BUCKET_NAME \
    -e WORKER_LOGS_BUCKET_NAME=$WORKER_LOGS_BUCKET_NAME \
    -e INSTANCE_ID=$INSTANCE_ID \
    -e PROJECT=$PROJECT \
    -e WORKER_CONFIG=$WORKER_CONFIG \
    -e MAX_IDLE_TIME_MSECS=$MAX_IDLE_TIME_MSECS \
    -e WORKER_DATA_DISK_MOUNT=/mnt/disks/$WORKER_DATA_DISK_NAME \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /usr/bin/docker:/usr/bin/docker \
    -v /usr/sbin/xfs_quota:/usr/sbin/xfs_quota \
    -v /batch:/batch \
    -v /logs:/logs \
    -v /gcsfuse:/gcsfuse:shared \
    -v /xfsquota:/xfsquota \
    --mount type=bind,source=/mnt/disks/$WORKER_DATA_DISK_NAME,target=/host \
    -p 5000:5000 \
    --device /dev/fuse \
    --device $XFS_DEVICE \
    --cap-add SYS_ADMIN \
    --security-opt apparmor:unconfined \
    $BATCH_WORKER_IMAGE \
    python3 -u -m batch.worker.worker >worker.log 2>&1

while true; do
  gcloud -q compute instances delete $NAME --zone=$ZONE
  sleep 1
done
'''
                }, {
                    'key': 'shutdown-script',
                    'value': '''
set -x

WORKER_LOGS_BUCKET_NAME=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/worker_logs_bucket_name")
INSTANCE_ID=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/instance_id")
NAME=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')

journalctl -u docker.service > dockerd.log

# this has to match LogStore.worker_log_path
gsutil -m cp dockerd.log gs://$WORKER_LOGS_BUCKET_NAME/batch/logs/$INSTANCE_ID/worker/$NAME/
'''
                }, {
                    'key': 'activation_token',
                    'value': activation_token
                }, {
                    'key': 'batch_worker_image',
                    'value': BATCH_WORKER_IMAGE
                }, {
                    'key': 'namespace',
                    'value': DEFAULT_NAMESPACE
                }, {
                    'key': 'batch_logs_bucket_name',
                    'value': self.log_store.batch_logs_bucket_name
                }, {
                    'key': 'worker_logs_bucket_name',
                    'value': self.log_store.worker_logs_bucket_name
                }, {
                    'key': 'instance_id',
                    'value': self.log_store.instance_id
                }, {
                    'key': 'max_idle_time_msecs',
                    'value': max_idle_time_msecs
                }]
            },
            'tags': {
                'items': [
                    "batch2-agent"
                ]
            },
        }

        worker_config = WorkerConfig.from_instance_config(config)
        assert worker_config.is_valid_configuration(self.app['resources'])
        config['metadata']['items'].append({
            'key': 'worker_config',
            'value': base64.b64encode(json.dumps(worker_config.config).encode()).decode()
        })

        await self.compute_client.post(
            f'/zones/{zone}/instances', json=config)

        log.info(f'created machine {machine_name} for {instance} in pool {self.id}')

    async def control_loop(self):
        log.info(f'starting control loop for pool {self}')
        while True:
            try:
                ready_cores = await self.db.select_and_fetchone(
                    '''
SELECT CAST(COALESCE(SUM(ready_cores_mcpu), 0) AS SIGNED) AS ready_cores_mcpu
FROM user_pool_resources
WHERE pool = %s
LOCK IN SHARED MODE;
''',
                    (self.id,))
                ready_cores_mcpu = ready_cores['ready_cores_mcpu']

                free_cores_mcpu = sum([
                    worker.free_cores_mcpu
                    for worker in self.healthy_instances_by_free_cores
                ])
                free_cores = free_cores_mcpu / 1000

                log.info(f'pool {self} n_instances {self.n_instances} {self.n_instances_by_state}'
                         f' free_cores {free_cores} live_free_cores {self.live_free_cores_mcpu / 1000}'
                         f' ready_cores {ready_cores_mcpu / 1000}')

                if ready_cores_mcpu > 0 and free_cores < 500:
                    n_live_instances = self.n_instances_by_state['pending'] + self.n_instances_by_state['active']

                    instances_needed = (
                            (ready_cores_mcpu - self.live_free_cores_mcpu + (self.cores * 1000) - 1)
                            // (self.cores * 1000))

                    instances_needed = min(instances_needed,
                                           self.pool_size - n_live_instances,
                                           self.max_instances - self.n_instances,
                                           long_run_quota,
                                           excess_scheduling_rate)
                    if instances_needed > 0:
                        log.info(f'creating {instances_needed} new instances for pool {self}')
                        # parallelism will be bounded by thread pool
                        await asyncio.gather(*[self.create_instance() for _ in range(instances_needed)])
            except asyncio.CancelledError:  # pylint: disable=try-except-raise
                raise
            except Exception:
                log.exception('in control loop')
            await asyncio.sleep(15)

    def __str__(self):
        return f'{self.config()}'
