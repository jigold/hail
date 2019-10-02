import pymysql.cursors
import json
import time
import statistics

config_file = '/batch-user-secret/sql-config.json'

batch_id = 1


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


try:
    timings = []
    n = 100
    for i in range(n):
        data = {'userdata': {'username': 'jigold',
                             'service-account': 'foo@gmail.com',
                             'k8s_service_account': 'dfsfasdads@k8s.com'},
                'user': 'jigold',
                'deleted': False,
                'cancelled': False,
                'closed': False,
                'n_jobs': 5}
        _, timing = insert_batch()
        timings.append(timing)
    print(f'insert batch: n={n} mean={statistics.mean(timings)} variance={statistics.variance(timings)}')

finally:
    connection.close()
