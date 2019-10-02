import pymysql.cursors
import json
import time
import statistics
import asyncio
import aiomysql
import sys

config_file = '/batch-user-secret/sql-config.json'


userdata = {'username': 'jigold',
            'service-account': 'foo@gmail.com',
            'k8s_service_account': 'dfsfasdads@k8s.com'}

batch_field_names = {'attributes', 'callback', 'userdata', 'user', 'deleted',
                     'cancelled', 'closed', 'n_jobs'}


# Connect to the database
with open(config_file, 'r') as f:
    config = json.loads(f.read().strip())


# pymysql.cursors.DictCursor.max_stmt_length *= 5
# print(pymysql.cursors.DictCursor.max_stmt_length)


def new_record_template(table_name, *field_names):
    names = ", ".join([f'`{name.replace("`", "``")}`' for name in field_names])
    values = ", ".join([f"%({name})s" for name in field_names])
    sql = f"INSERT INTO `{table_name}` ({names}) VALUES ({values})"
    return sql


async def insert_batch(pool, **data):
    async with pool.acquire() as conn:
        start = time.time()
        async with conn.cursor() as cursor:
            sql = new_record_template('batch', *data)
            await cursor.execute(sql, data)
            id = cursor.lastrowid  # This returns 0 unless an autoincrement field is in the table
            return id, time.time() - start


async def insert_jobs(pool, sem, data):
    async with sem:
        async with pool.acquire() as conn:
            start = time.time()
            async with conn.cursor() as cursor:
                sql = new_record_template('jobs', *(data[0]))
                await cursor.executemany(sql, data)
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


async def test():
    pool = await aiomysql.create_pool(
        host=config['host'],
        port=config['port'],
        db=config['db'],
        user=config['user'],
        password=config['password'],
        charset='utf8',
        cursorclass=aiomysql.cursors.DictCursor,
        autocommit=True,
        maxsize=100,
        connect_timeout=100)

    sem = asyncio.Semaphore(2)

    try:
        insert_batch_timings = []
        insert_jobs_timings = {}
        batch_n = 10

        for jobs_n in (1000, 10000, 100000):
            print(jobs_n)
            insert_jobs_timings[jobs_n] = []

            for i in range(batch_n):
                data = {'userdata': json.dumps(userdata),
                        'user': 'jigold',
                        'deleted': False,
                        'cancelled': False,
                        'closed': False,
                        'n_jobs': 5}
                batch_id, timing = await insert_batch(pool, **data)
                insert_batch_timings.append(timing)

                jobs_data = []
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
                    jobs_data.append(data)

                jobs_data = [jobs_data[i:i + 1000] for i in range(0, len(jobs_data), 1000)]

                start = time.time()
                timings = await asyncio.gather(*[insert_jobs(pool, sem, jd) for jd in jobs_data])
                for timing in timings:
                    print(timing)
                insert_jobs_timings[jobs_n].append(time.time() - start)

        print(f'insert batch: n={len(insert_batch_timings)} mean={statistics.mean(insert_batch_timings)} variance={statistics.variance(insert_batch_timings)}')

        for jobs_n, timings in insert_jobs_timings.items():
            print(f'insert jobs: n={jobs_n} mean={statistics.mean(timings)} variance={statistics.variance(timings)}')

    finally:
        pool.close()

print()
print()
print()

loop = asyncio.get_event_loop()
loop.run_until_complete(test())
loop.close()
time.sleep(60)