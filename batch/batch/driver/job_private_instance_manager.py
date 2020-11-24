from .scheduler import JobPrivateInstanceScheduler

from hailtop import aiotools


class JobPrivateInstanceManager:
    def __init__(self, app, max_instances):
        self.app = app
        self.scheduler = JobPrivateInstanceScheduler(app)

        self.n_instances = 0
        self.max_instances = max_instances

        self.task_manager = aiotools.BackgroundTaskManager()

    async def async_init(self):
        pass

    def configure(self, max_instances):
        self.max_instances = max_instances

    def shutdown(self):
        pass

    async def activate_instance(self):
        pass

    async def create_instance(self):
        pass
