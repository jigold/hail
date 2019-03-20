import os
import pymysql
import json
import uuid
import aiomysql


def read_config():
    config_file = os.environ.get('CLOUD_SQL_CONFIG_PATH',
                                 '/batch-secrets/batch-production-cloud-sql-config.json')
    with open(config_file, 'r') as f:
        config = json.loads(f.read().strip())

    return config


def connection():
    config = read_config()

    c = pymysql.connect(host=config['host'],
                        port=config['port'],
                        db=config['db'],
                        user=config['user'],
                        password=config['password'],
                        charset='utf8',
                        cursorclass=pymysql.cursors.DictCursor,
                        autocommit=True)
    return c


def get_temp_table(root):
    c = connection()
    suffix = uuid.uuid4().hex[:8]
    name = f'{root}-{suffix}'
    try:
        niter = 0
        while has_table(name):
            suffix = uuid.uuid4().hex[:8]
            name = f'{root}-{suffix}'
            niter += 1
            if niter > 5:
                raise Exception("Too many attempts to get unique temp table.")
    finally:
        c.close()
    return name


def has_table(name):
    c = connection()
    try:
        with c.cursor() as cursor:
            sql = f"SELECT * FROM INFORMATION_SCHEMA.tables " \
                f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s"
            cursor.execute(sql, (c.db, name))
            result = cursor.fetchone()
    finally:
        c.close()

    return result is not None


def drop_table(*names):
    c = connection()
    try:
        with c.cursor() as cursor:
            for name in names:
                cursor.execute(f"DROP TABLE IF EXISTS `{name}`")
    finally:
        c.close()
