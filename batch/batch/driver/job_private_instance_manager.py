from .scheduler import JobPrivateInstanceScheduler

from hailtop import aiotools

from ..batch import schedule_job


class JobPrivateInstanceManager:
    def __init__(self, app, max_instances):
        self.app = app
        self.db = app['db']
        self.inst_monitor = app['inst_monitor']
        self.scheduler = None

        self.n_instances = 0
        self.max_instances = max_instances

        self.task_manager = aiotools.BackgroundTaskManager()

    async def async_init(self):
        self.scheduler = JobPrivateInstanceScheduler(self.app)

    def configure(self, max_instances):
        self.max_instances = max_instances

    def shutdown(self):
        pass

    async def activate_instance(self, instance):
        record = await self.db.select_and_fetchone(f'''
SELECT jobs.* FROM instance_private_jobs
LEFT JOIN jobs ON
  jobs.batch_id = instance_private_jobs.batch_id AND jobs.job_id = instance_private_jobs.job_id
WHERE instance = %s
ORDER BY time_created DESC
LOCK IN SHARE MODE;
''',
                             (instance.name,))

        await schedule_job(self.app, record, instance)

    async def create_instance(self, batch_id, job_id, spec):
        private_resources = spec['private_resources']
        machine_type = private_resources['machine_type']
        preemptible = private_resources['preemptible']
        storage_gb = private_resources['storage']  # FIXME: make sure this is in gb

        instance = await self.inst_monitor.create(None, machine_type, preemptible, local_ssd_data_disk=False,
                                                  storage_gb, worker_disk_size_gb=10)

        await self.db.insert(f'''
INSERT INTO instance_private_jobs (instance, batch_id, job_id)
  VALUES (%s, %s, %s);
''',
                             (instance.name, batch_id, job_id))