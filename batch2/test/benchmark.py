import pymysql.cursors
import json
import time
import statistics

config_file = '/batch-user-secret/sql-config.json'

batch_id = 1


userdata = {'username': 'jigold',
            'service-account': 'foo@gmail.com',
            'k8s_service_account': 'dfsfasdads@k8s.com'}

batch_field_names = {'attributes', 'callback', 'userdata', 'user', 'deleted',
                     'cancelled', 'closed', 'n_jobs'}


# Connect to the database
with open(config_file, 'r') as f:
    config = json.loads(f.read().strip())

connection = pymysql.connect(host=config['host'],
                             user=config['user'],
                             password=config['password'],
                             db=config['db'],
                             charset='utf8',
                             cursorclass=pymysql.cursors.DictCursor,
                             autocommit=True)


def new_record_template(table_name, *field_names):
    names = ", ".join([f'`{name.replace("`", "``")}`' for name in field_names])
    values = ", ".join([f"%({name})s" for name in field_names])
    sql = f"INSERT INTO `{table_name}` ({names}) VALUES ({values})"
    return sql


def insert_batch(**data):
    start = time.time()
    with connection.cursor() as cursor:
        sql = new_record_template('batch', *data)
        cursor.execute(sql, data)
        id = cursor.lastrowid  # This returns 0 unless an autoincrement field is in the table
        return id, time.time() - start


def insert_job(**data):
    start = time.time()
    with connection.cursor() as cursor:
        sql = new_record_template('jobs', *data)
        cursor.execute(sql, data)
        return time.time() - start


def job_spec(image, env=None, command=None, args=None, ports=None,
             resources=None, volumes=None, tolerations=None,
             security_context=None, service_account_name=None):
    if env:
        env = [{'name': k, 'value': v} for (k, v) in env.items()]
    else:
        env = []
    env.extend([{
        'name': 'POD_IP',
        'valueFrom': {
            'fieldRef': {'fieldPath': 'status.podIP'}
        }
    }, {
        'name': 'POD_NAME',
        'valueFrom': {
            'fieldRef': {'fieldPath': 'metadata.name'}
        }
    }])

    container = {
        'image': image,
        'name': 'main'
    }
    if command:
        container['command'] = command
    if args:
        container['args'] = args
    if env:
        container['env'] = env
    if ports:
        container['ports'] = [{
            'containerPort': p,
            'protocol': 'TCP'
        } for p in ports]
    if resources:
        container['resources'] = resources
    if volumes:
        container['volumeMounts'] = [v['volume_mount'] for v in volumes]
    spec = {
        'containers': [container],
        'restartPolicy': 'Never'
    }
    if volumes:
        spec['volumes'] = [v['volume'] for v in volumes]
    if tolerations:
        spec['tolerations'] = tolerations
    if security_context:
        spec['securityContext'] = security_context
    if service_account_name:
        spec['serviceAccountName'] = service_account_name

    return spec

try:
    insert_batch_timings = []
    insert_jobs_timings = []
    batch_n = 100
    jobs_n = 100
    for i in range(batch_n):
        data = {'userdata': json.dumps(userdata),
                'user': 'jigold',
                'deleted': False,
                'cancelled': False,
                'closed': False,
                'n_jobs': 5}
        batch_id, timing = insert_batch(**data)
        insert_batch_timings.append(timing)

        for job_id in range(jobs_n):
            spec = job_spec('alpine', command=['sleep', '40'])
            data = {'batch_id': batch_id,
                    'job_id': job_id,
                    'state': 'Running',
                    'pvc_size': '100M',
                    'callback': 'http://foo.com/callback',
                    'attributes': json.dumps({'job': f'{job_id}',
                                              'app': 'batch'}),
                    'always_run': False,
                    'token': 'dfsafdsasd',
                    'directory': f'gs://fdsaofsaoureqr/fadsfafdsfdasfd/{batch_id}/{job_id}/',
                    'pod_spec': json.dumps(spec),
                    'input_files': json.dumps(['foo.txt', 'gs://ffadsfe/fadsfad/foo.txt']),
                    'output_files': json.dumps(['gs://fdsfadfefadf/afdsasfewa/wip/a.txt']),
                    'exit_codes': json.dumps([None, None, None])}
            timing = insert_job(**data)
            insert_jobs_timings.append(timing)

    print(f'insert batch: n={batch_n} mean={statistics.mean(insert_batch_timings)} variance={statistics.variance(insert_batch_timings)}')
    print(f'insert jobs: n={batch_n * jobs_n} mean={statistics.mean(insert_jobs_timings)} variance={statistics.variance(insert_jobs_timings)}')

finally:
    connection.close()
