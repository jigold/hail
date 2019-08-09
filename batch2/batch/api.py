import asyncio


INSTANCE_NAME = 'batch-agent-8'


class Driver:
    def __init__(self, app, db):
        self.app = app
        self.db = db
        self.event_queue = asyncio.Queue()

    async def create_pod(self, spec):
        pass

        # add instance name to jobs table

    async def delete_pod(self, name):
        pass

    async def read_log(self, pod_name, container_name):
        return "foo"

    async def read_pod_status(self, pod_name, container_name):
        return "foo"

    async def list_pods(self):
        pass
