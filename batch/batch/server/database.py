import os
import json
import aiomysql
from asyncinit import asyncinit


@asyncinit
class Database:
    async def __init__(self, config_file):
        with open(config_file, 'r') as f:
            config = json.loads(f.read().strip())

        self.host = config['host']
        self.port = config['port']
        self.user = config['user']
        self.db = config['db']
        self.password = config['password']
        self.charset = 'utf8'

        self.pool = await aiomysql.create_pool(host=self.host,
                                               port=self.port,
                                               db=self.db,
                                               user=self.user,
                                               password=self.password,
                                               charset=self.charset,
                                               cursorclass=aiomysql.cursors.DictCursor,
                                               echo=True,
                                               autocommit=True)

        self.jobs = await JobsTable(self, os.environ.get("JOBS_TABLE"))
        self.jobs_parents = await JobsParentsTable(self, os.environ.get("JOBS_PARENTS_TABLE"))
        self.batch = await BatchTable(self, os.environ.get("BATCH_TABLE"))
        self.batch_jobs = await BatchJobsTable(self, os.environ.get("BATCH_JOBS_TABLE"))


class Table:
    def __init__(self, db, table_name):
        self.name = table_name
        self._db = db

    async def _create_table(self, schema, keys):
        assert all([k in schema for k in keys])

        async with self._db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                schema = ", ".join([f"`{n}` {t}" for n, t in schema.items()])

                key_names = ", ".join([f'`{name.replace("`", "``")}`' for name in keys])
                keys = f", PRIMARY KEY( {key_names} )" if keys else ''

                sql = f"CREATE TABLE IF NOT EXISTS `{self.name}` ( {schema} {keys})"
                await cursor.execute(sql)

    async def _new_record(self, items):
        names = ", ".join([f'`{name.replace("`", "``")}`' for name in items.keys()])
        values_template = ", ".join(["%s" for _ in items.values()])
        async with self._db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                sql = f"INSERT INTO `{self.name}` ({names}) VALUES ({values_template})"
                await cursor.execute(sql, tuple(items.values()))
                id = cursor.lastrowid
        return id

    async def _update_record(self, key, items):
        async with self._db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                if len(items) != 0:
                    items_template = ", ".join([f'`{k.replace("`", "``")}` = %s' for k, v in items.items()])
                    key_template = ", ".join([f'`{k.replace("`", "``")}` = %s' for k, v in key.items()])

                    values = items.values()
                    key_values = key.values()

                    sql = f"UPDATE `{self.name}` SET {items_template} WHERE {key_template}"
                    await cursor.execute(sql, (*values, *key_values))

    async def _get_records(self, key, fields=None):
        assert fields is None or len(fields) != 0
        async with self._db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                key_template = []
                key_values = []
                for k, v in key.items():
                    if isinstance(v, list):
                        if len(v) == 0:
                            key_template.append("FALSE")
                        else:
                            key_template.append(f'`{k.replace("`", "``")}` IN %s')
                            key_values.append(v)
                    else:
                        key_template.append(f'`{k.replace("`", "``")}` = %s')
                        key_values.append(v)

                key_template = "AND".join(key_template)
                fields = ",".join(fields) if fields is not None else "*"

                sql = f"SELECT {fields} FROM `{self.name}` WHERE {key_template}"
                await cursor.execute(sql, key_values)
                result = cursor.fetchall()
        return result

    async def _get_record(self, key, fields=None):
        records = await self._get_records(key, fields)
        assert len(records.result()) == 1
        return records.result()[0]
        # assert fields is None or len(fields) != 0
        # async with self._db.pool.acquire() as conn:
        #     async with conn.cursor() as cursor:
        #         key_template = ", ".join([f'`{k.replace("`", "``")}` = %s' for k, v in key.items()])
        #         key_values = key.values()
        #         fields = ",".join(fields) if fields is not None else "*"
        #         sql = f"SELECT {fields} FROM `{self.name}` WHERE {key_template}"
        #         await cursor.execute(sql, tuple(key_values))
        #         result = cursor.fetchone()
        # return result

    async def _has_record(self, key):
        async with self._db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                key_template = " AND ".join([f'`{k.replace("`", "``")}` = %s' for k, v in key.items()])
                key_values = key.values()
                sql = f"SELECT COUNT(1) FROM `{self.name}` WHERE {key_template}"
                print(cursor.mogrify(sql, tuple(key_values)))
                count = await cursor.execute(sql, tuple(key_values))
        return count == 1

    async def _delete_records(self, key):
        async with self._db.pool.acquire() as conn:
            async with conn.cursor() as cursor:
                key_template = []
                key_values = []
                for k, v in key.items():
                    if isinstance(v, list):
                        if len(v) == 0:
                            key_template.append("FALSE")
                        else:
                            key_template.append(f'`{k.replace("`", "``")}` IN %s')
                            key_values.append(v)
                    else:
                        key_template.append(f'`{k.replace("`", "``")}` = %s')
                        key_values.append(v)

                key_template = "AND".join(key_template)
                sql = f"DELETE FROM `{self.name}` WHERE {key_template}"
                await cursor.execute(sql, key_values)
                result = cursor.fetchall()
        return result

@asyncinit
class JobsTable(Table):
    async def __init__(self, db, name='jobs'):
        super().__init__(db, name)

        self._schema = {'id': 'BIGINT NOT NULL AUTO_INCREMENT',
                        'state': 'VARCHAR(40) NOT NULL',
                        'exit_code': 'INT',
                        'batch_id': 'BIGINT',
                        'scratch_folder': 'VARCHAR(1000)',
                        'pod_name': 'VARCHAR(1000)',
                        'pvc': 'TEXT(65535)',
                        'callback': 'TEXT(65535)',
                        'task_idx': 'INT NOT NULL',
                        'always_run': 'BOOLEAN',
                        'time_created': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
                        # 'time_ended': 'TIMESTAMP DEFAULT NULL',
                        'user': 'VARCHAR(1000)',  # FIXME: future PR to add user
                        'attributes': 'TEXT(65535)',
                        'tasks': 'TEXT(65535)',
                        # 'child_ids': 'TEXT(65535)',
                        'parent_ids': 'TEXT(65535)',
                        'input_log_uri': 'VARCHAR(1000)', # batch bucket, service account
                        'main_log_uri': 'VARCHAR(1000)',
                        'output_log_uri': 'VARCHAR(1000)'}

        self._keys = ['id']

        await self._create_table(self._schema, self._keys)

    async def new_record(self, **items):
        assert all([k in self._schema for k in items.keys()])
        return await self._new_record(items)

    async def update_record(self, id, **items):
        assert all([k in self._schema for k in items.keys()])
        await self._update_record({'id': id}, items)

    async def get_records(self, ids, fields=None):
        assert isinstance(ids, list)
        return await self._get_records({'id': ids}, fields)

    async def get_record(self, id, fields=None):
        return await self._get_record({'id': id}, fields)

    async def has_record(self, id):
        return await self._has_record({'id': id})

    async def get_incomplete_parents(self, id):
        parent_ids = await self.get_record(id, ['parent_ids'])
        parent_ids = json.loads(parent_ids['parent_ids'])
        parent_records = await self.get_records(parent_ids)
        incomplete_parents = [pr['id'] for pr in parent_records.result()
                              if pr['state'] == 'Created']
        return incomplete_parents


@asyncinit
class JobsParentsTable(Table):
    async def __init__(self, db, name='jobs-parents'):
        super().__init__(db, name)

        self._schema = {'id': 'BIGINT',
                        'parent': 'BIGINT'}

        self._keys = []

        await self._create_table(self._schema, self._keys)

    async def new_record(self, **items):
        assert all([k in self._schema for k in items.keys()])
        return await self._new_record(items)

    async def get_parents(self, id):
        result = await self._get_records({'id': id}, ['parent'])
        return [record['parent'] for record in result.result()]

    async def get_children(self, id):
        result = await self._get_records({'parent': id}, ['id'])
        return [record['id'] for record in result.result()]


@asyncinit
class BatchTable(Table):
    async def __init__(self, db, name='batch'):
        super().__init__(db, name)

        self._schema = {'id': 'BIGINT NOT NULL AUTO_INCREMENT',
                        'attributes': 'TEXT(65535)',
                        'callback': 'TEXT(65535)',
                        'ttl': 'INT',
                        'is_open': 'BOOLEAN NOT NULL'
                        }

        self._keys = ['id']

        await self._create_table(self._schema, self._keys)

    async def new_record(self, **items):
        assert all([k in self._schema for k in items.keys()])
        return await self._new_record(items)

    async def update_record(self, id, **items):
        assert all([k in self._schema for k in items.keys()])
        await self._update_record({'id': id}, items)

    async def get_record(self, id):
        return await self._get_record({'id': id})

    async def has_record(self, id):
        return await self._has_record({'id': id})


@asyncinit
class BatchJobsTable(Table):
    async def __init__(self, db, name='batch-jobs'):
        super().__init__(db, name)

        self._schema = {'id': 'BIGINT',
                        'job': 'BIGINT'}

        self._keys = []

        await self._create_table(self._schema, self._keys)

    async def new_record(self, **items):
        assert all([k in self._schema for k in items.keys()])
        return await self._new_record(items)

    async def get_jobs(self, id):
        result = await self._get_records({'id': id})
        return [record['job'] for record in result.result()]

    async def delete_records(self, batch_id, job_id):
        await self._delete_records({'id': batch_id,
                                    'job': job_id})

    async def has_record(self, batch_id, job_id):
        return await self._has_record({'id': batch_id,
                                       'job': job_id})