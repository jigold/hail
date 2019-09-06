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


db = None


def init_db():
    global db
    db = BatchDatabase.create_synchronous('/batch-user-secret/sql-config.json')
