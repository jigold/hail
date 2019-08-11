import asyncio


class Driver:
    # needs an event queue
    # needs a scheduler instance
    
    def create_pod(self, spec, secrets):
        # get free instance
        # submit request to that instance
        # update db
        pass

    def delete_pod(self, name):
        pass

    def read_pod_log(self, name, container):
        pass

    def read_pod_status(self, name):
        pass

    def list_pods(self):
        pass
