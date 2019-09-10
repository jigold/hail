import asyncio
from .database import BatchDatabase

states = {'Pending', 'Running', 'Cancelled', 'Error', 'Failed', 'Success'}

complete_states = ('Cancelled', 'Error', 'Failed', 'Success')

valid_state_transitions = {
    'Pending': {'Running'},
    'Running': {'Running', 'Cancelled', 'Error', 'Failed', 'Success'},
    'Cancelled': set(),
    'Error': set(),
    'Failed': set(),
    'Success': set(),
}

tasks = ('setup', 'main', 'cleanup')

# db = BatchDatabase('/batch-user-secret/sql-config.json')

db = None


def get_db():
    global db
    if not db:
        loop = asyncio.get_event_loop()
        db = loop.run_until_complete(BatchDatabase('/batch-user-secret/sql-config.json'))
        # db = BatchDatabase.create_synchronous('/batch-user-secret/sql-config.json')
    return db
