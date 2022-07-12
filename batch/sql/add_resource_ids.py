import asyncio
import functools
import os
import random
import time
from typing import List, Optional, Tuple

from gear import Database, transaction
from hailtop.utils import bounded_gather


MYSQL_CONFIG_FILE = os.environ.get('MYSQL_CONFIG_FILE')


class Counter:
    def __init__(self):
        self.n = 0


def attempt_resources_offsets_to_where_statement(start_offset, end_offset):
    # start inclusive, end exclusive
    if start_offset is None and end_offset is None:
        return ('', None)

    assert start_offset != end_offset, str((start_offset, end_offset))    
    
    if start_offset is None:
        assert end_offset
        end_batch_id, end_job_id, end_attempt_id, end_resource = end_offset
        where_cond = 'WHERE attempt_resources.batch_id < %s OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id < %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id < %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id = %s AND attempt_resources.resource < %s)'
        query_args = (end_batch_id, end_batch_id, end_job_id, end_batch_id, end_job_id, end_attempt_id, end_batch_id, end_job_id, end_attempt_id, end_resource)
    elif end_offset is None:
        assert start_offset
        start_batch_id, start_job_id, start_attempt_id, start_resource = start_offset
        where_cond = 'WHERE attempt_resources.batch_id > %s OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id > %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id > %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id = %s AND attempt_resources.resource >= %s)'
        query_args = (start_batch_id, start_batch_id, start_job_id, start_batch_id, start_job_id, start_attempt_id, start_batch_id, start_job_id, start_attempt_id, start_resource)
    else:
        start_batch_id, start_job_id, start_attempt_id, start_resource = start_offset
        end_batch_id, end_job_id, end_attempt_id, end_resource = end_offset
        where_cond = 'WHERE (attempt_resources.batch_id > %s OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id > %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id > %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id = %s AND attempt_resources.resource >= %s)) ' \
                     'AND (attempt_resources.batch_id < %s OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id < %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id < %s) OR ' \
                     '(attempt_resources.batch_id = %s AND attempt_resources.job_id = %s AND attempt_resources.attempt_id = %s AND attempt_resources.resource < %s))'
        query_args = (start_batch_id, start_batch_id, start_job_id, start_batch_id, start_job_id, start_attempt_id, start_batch_id, start_job_id, start_attempt_id, start_resource,
                      end_batch_id, end_batch_id, end_job_id, end_batch_id, end_job_id, end_attempt_id, end_batch_id, end_job_id, end_attempt_id, end_resource)

    return (where_cond, query_args)


def agg_billing_project_resources_offsets_to_where_statement(start_offset, end_offset):
    # start inclusive, end exclusive
    if start_offset is None and end_offset is None:
        return ('', None)

    assert start_offset != end_offset, str((start_offset, end_offset))

    if start_offset is None:
        assert end_offset
        end_bp, end_resource, end_token = end_offset
        where_cond = 'WHERE aggregated_billing_project_resources.billing_project < %s OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource < %s) OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource = %s AND aggregated_billing_project_resources.token < %s)'
        query_args = (end_bp, end_bp, end_resource, end_bp, end_resource, end_token)
    elif end_offset is None:
        assert start_offset
        start_bp, start_resource, start_token = start_offset
        where_cond = 'WHERE aggregated_billing_project_resources.billing_project > %s OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource > %s) OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource = %s AND aggregated_billing_project_resources.token >= %s)'
        query_args = (start_bp, start_bp, start_resource, start_bp, start_resource, start_token)
    else:
        start_bp, start_resource, start_token = start_offset
        end_bp, end_resource, end_token = end_offset
        where_cond = 'WHERE (aggregated_billing_project_resources.billing_project > %s OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource > %s) OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource = %s AND aggregated_billing_project_resources.token >= %s)) ' \
                     'AND (aggregated_billing_project_resources.billing_project < %s OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource < %s) OR ' \
                     '(aggregated_billing_project_resources.billing_project = %s AND aggregated_billing_project_resources.resource = %s AND aggregated_billing_project_resources.token < %s))'
        query_args = (start_bp, start_bp, start_resource, start_bp, start_resource, start_token,
                      end_bp, end_bp, end_resource, end_bp, end_resource, end_token)

    return (where_cond, query_args)


def agg_batch_resources_offsets_to_where_statement(start_offset, end_offset):
    # start inclusive, end exclusive
    if start_offset is None and end_offset is None:
        return ('', None)

    assert start_offset != end_offset, str((start_offset, end_offset))

    if start_offset is None:
        assert end_offset
        end_batch_id, end_resource, end_token = end_offset
        where_cond = 'WHERE aggregated_batch_resources.batch_id < %s OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource < %s) OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource = %s AND aggregated_batch_resources.token < %s)'
        query_args = (end_batch_id, end_batch_id, end_resource, end_batch_id, end_resource, end_token)
    elif end_offset is None:
        assert start_offset
        start_batch_id, start_resource, start_token = start_offset
        where_cond = 'WHERE aggregated_batch_resources.batch_id > %s OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource > %s) OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource = %s AND aggregated_batch_resources.token >= %s)'
        query_args = (start_batch_id, start_batch_id, start_resource, start_batch_id, start_resource, start_token)
    else:
        start_batch_id, start_resource, start_token = start_offset
        end_batch_id, end_resource, end_token = end_offset
        where_cond = 'WHERE (aggregated_batch_resources.batch_id > %s OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource > %s) OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource = %s AND aggregated_batch_resources.token >= %s)) ' \
                     'AND (aggregated_batch_resources.batch_id < %s OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource < %s) OR ' \
                     '(aggregated_batch_resources.batch_id = %s AND aggregated_batch_resources.resource = %s AND aggregated_batch_resources.token < %s))'
        query_args = (start_batch_id, start_batch_id, start_resource, start_batch_id, start_resource, start_token,
                      end_batch_id, end_batch_id, end_resource, end_batch_id, end_resource, end_token)

    return (where_cond, query_args)


def agg_job_resources_offsets_to_where_statement(start_offset, end_offset):
    # start inclusive, end exclusive
    if start_offset is None and end_offset is None:
        return ('', None)

    assert start_offset != end_offset, str((start_offset, end_offset))

    if start_offset is None:
        assert end_offset
        end_batch_id, end_job_id, end_resource = end_offset
        where_cond = 'WHERE aggregated_job_resources.batch_id < %s OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id < %s) OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id = %s AND aggregated_job_resources.resource < %s)'
        query_args = (end_batch_id, end_batch_id, end_job_id, end_batch_id, end_job_id, end_resource)
    elif end_offset is None:
        assert start_offset
        start_batch_id, start_job_id, start_resource = start_offset
        where_cond = 'WHERE aggregated_job_resources.batch_id > %s OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id > %s) OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id = %s AND aggregated_job_resources.resource >= %s)'
        query_args = (start_batch_id, start_batch_id, start_job_id, start_batch_id, start_job_id, start_resource)
    else:
        start_batch_id, start_job_id, start_resource = start_offset
        end_batch_id, end_job_id, end_resource = end_offset
        where_cond = 'WHERE (aggregated_job_resources.batch_id > %s OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id > %s) OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id = %s AND aggregated_job_resources.resource >= %s)) ' \
                     'AND (aggregated_job_resources.batch_id < %s OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id < %s) OR ' \
                     '(aggregated_job_resources.batch_id = %s AND aggregated_job_resources.job_id = %s AND aggregated_job_resources.resource < %s))'
        query_args = (start_batch_id, start_batch_id, start_job_id, start_batch_id, start_job_id, start_resource,
                      end_batch_id, end_batch_id, end_job_id, end_batch_id, end_job_id, end_resource)

    return (where_cond, query_args)


async def process_chunk(counter, db, table_name, where_cond, query_args, start_offset, end_offset, quiet=True):
    start_time = time.time()

    await db.just_execute(
        f'''
UPDATE {table_name}
SET resource_id = (
    SELECT resource_id
    FROM resources
    WHERE {table_name}.resource = resources.resource
)
{where_cond};
''',
        query_args)

    if not quiet and counter.n % 100 == 0:
        print(f'processed chunk ({start_offset}, {end_offset}) in {time.time() - start_time}s')

    counter.n += 1
    if counter.n % 500 == 0:
        print(f'processed {counter.n} complete chunks')


async def process_attempt_resources_chunk(counter, db, start_offset, end_offset, quiet=True):
    where_cond, query_args = attempt_resources_offsets_to_where_statement(start_offset, end_offset)
    await process_chunk(counter, db, 'attempt_resources', where_cond, query_args, start_offset, end_offset, quiet)


async def process_agg_billing_project_resources_chunk(counter, db, start_offset, end_offset, quiet=True):
    where_cond, query_args = agg_billing_project_resources_offsets_to_where_statement(start_offset, end_offset)
    await process_chunk(counter, db, 'aggregated_billing_project_resources', where_cond, query_args, start_offset, end_offset, quiet)


async def process_agg_batch_resources_chunk(counter, db, start_offset, end_offset, quiet=True):
    where_cond, query_args = agg_batch_resources_offsets_to_where_statement(start_offset, end_offset)
    await process_chunk(counter, db, 'aggregated_batch_resources', where_cond, query_args, start_offset, end_offset, quiet)


async def process_agg_job_resources_chunk(counter, db, start_offset, end_offset, quiet=True):
    where_cond, query_args = agg_job_resources_offsets_to_where_statement(start_offset, end_offset)
    await process_chunk(counter, db, 'aggregated_job_resources', where_cond, query_args, start_offset, end_offset, quiet)


# async def process_chunk(counter, db, start_offset, end_offset, quiet=True):
#     start_time = time.time()
#
#     where_cond, query_args = attempts_offsets_to_where_statement(start_offset, end_offset)
#
#     await db.just_execute(
#         f'''
# UPDATE attempt_resources
# SET resource_id = (
#     SELECT resource_id
#     FROM resources
#     WHERE attempt_resources.resource = resources.resource
# )
# {where_cond};
# ''',
#         query_args)
#
#     await db.just_execute(
#         f'''
# UPDATE aggregated_billing_project_resources
# SET resource_id = (
#     SELECT resource_id
#     FROM resources
#     WHERE aggregated_billing_project_resources.resource = resources.resource
# )
# {where_cond};
# ''',
#         query_args)
#
#     await db.just_execute(
#         f'''
# UPDATE aggregated_batch_resources
# SET resource_id = (
#     SELECT resource_id
#     FROM resources
#     WHERE aggregated_batch_resources.resource = resources.resource
# )
# {where_cond};
# ''',
#         query_args)
#
#     await db.just_execute(
#         f'''
# UPDATE aggregated_job_resources
# SET resource_id = (
#     SELECT resource_id
#     FROM resources
#     WHERE aggregated_job_resources.resource = resources.resource
# )
# {where_cond};
# ''',
#         query_args)
#
#     if not quiet and counter.n % 100 == 0:
#         print(f'processed chunk ({start_offset}, {end_offset}) in {time.time() - start_time}s')
#
#     counter.n += 1
#     if counter.n % 500 == 0:
#         print(f'processed {counter.n} complete chunks')


async def audit_changes(db):
    attempt_resources_audit_start = time.time()
    print('starting auditing attempt resources records')

    bad_attempt_resources_records = db.select_and_fetchall(
        '''
SELECT *
FROM attempt_resources
LEFT JOIN resources ON attempt_resources.resource = resources.resource
WHERE attempt_resources.resource_id IS NULL OR attempt_resources.resource_id != resources.resource_id
LIMIT 100;
'''
    )

    bad_attempt_resources_records = [record async for record in bad_attempt_resources_records]
    for record in bad_attempt_resources_records:
        print(f'found bad attempt resources record {record}')

    print(f'finished auditing attempt_resources records in {time.time() - attempt_resources_audit_start}s')

    agg_bp_resources_audit_start = time.time()
    print('starting auditing aggregated billing project records')

    bad_aggregated_billing_project_resources_records = db.select_and_fetchall(
        '''
SELECT *
FROM aggregated_billing_project_resources
LEFT JOIN resources ON aggregated_billing_project_resources.resource = resources.resource
WHERE aggregated_billing_project_resources.resource_id IS NULL OR aggregated_billing_project_resources.resource_id != resources.resource_id
LIMIT 100;
'''
    )

    bad_aggregated_billing_project_resources_records = [record async for record in bad_aggregated_billing_project_resources_records]
    for record in bad_aggregated_billing_project_resources_records:
        print(f'found bad aggregated billing project resources record {record}')

    print(f'finished auditing aggregated billing project records in {time.time() - agg_bp_resources_audit_start}s')

    agg_batch_resources_audit_start = time.time()
    print('starting auditing aggregated batch records')

    bad_aggregated_batch_resources_records = db.select_and_fetchall(
            '''
SELECT *
FROM aggregated_batch_resources
LEFT JOIN resources ON aggregated_batch_resources.resource = resources.resource
WHERE aggregated_batch_resources.resource_id IS NULL OR aggregated_batch_resources.resource_id != resources.resource_id
LIMIT 100;
'''
    )

    bad_aggregated_batch_resources_records = [record async for record in
                                              bad_aggregated_batch_resources_records]
    for record in bad_aggregated_batch_resources_records:
        print(f'found bad aggregated batch resources record {record}')

    print(f'finished auditing aggregated batch records in {time.time() - agg_batch_resources_audit_start}s')

    agg_job_resources_audit_start = time.time()
    print('starting auditing aggregated job records')

    bad_aggregated_job_resources_records = db.select_and_fetchall(
            '''
SELECT *
FROM aggregated_job_resources
LEFT JOIN resources ON aggregated_job_resources.resource = resources.resource
WHERE aggregated_job_resources.resource_id IS NULL OR aggregated_job_resources.resource_id != resources.resource_id
LIMIT 100;
'''
        )

    bad_aggregated_job_resources_records = [record async for record in
                                            bad_aggregated_job_resources_records]
    for record in bad_aggregated_job_resources_records:
        print(f'found bad aggregated job resources record {record}')

    print(f'finished auditing aggregated job records in {time.time() - agg_job_resources_audit_start}s')

    if (bad_attempt_resources_records or
            bad_aggregated_billing_project_resources_records or
            bad_aggregated_batch_resources_records or
            bad_aggregated_job_resources_records):
        raise Exception(f'errors found in audit')


# async def find_complete_chunk_offsets(db, size):
#     @transaction(db)
#     async def _find_chunks(tx) -> List[Optional[Tuple[int, int, str]]]:
#         start_time = time.time()
#
#         await tx.just_execute('SET @rank=0;')
#
#         query = f'''
# SELECT t.batch_id, t.job_id, t.attempt_id FROM (
#   SELECT attempts.batch_id, attempts.job_id, attempts.attempt_id
#   FROM attempts
#   LEFT JOIN batches ON attempts.batch_id = batches.id
#   ORDER BY attempts.batch_id, attempts.job_id, attempts.attempt_id
# ) AS t
# WHERE MOD((@rank := @rank + 1), %s) = 0;
# '''
#
#         offsets = tx.execute_and_fetchall(query, (size,))
#         offsets = [(offset['batch_id'], offset['job_id'], offset['attempt_id']) async for offset in offsets]
#
#         offsets.append(None)
#
#         print(f'found chunk offsets in {round(time.time() - start_time, 4)}s')
#         return offsets
#
#     return await _find_chunks()


# async def find_attempt_resources_offsets(db, query, size):
#     @transaction(db)
#     async def _find_chunks(tx) -> List[Optional[Tuple[int, int, str]]]:
#         start_time = time.time()
#
#         await tx.just_execute('SET @rank=0;')
#
#         query = f'''
# SELECT t.batch_id, t.job_id, t.attempt_id, t.resource FROM (
#   SELECT batch_id, job_id, attempt_id, resource
#   FROM attempt_resources
#   ORDER BY batch_id, job_id, attempt_id, resource
# ) AS t
# WHERE MOD((@rank := @rank + 1), %s) = 0;
# '''
#
#         offsets = tx.execute_and_fetchall(query, (size,))
#         offsets = [(offset['batch_id'], offset['job_id'], offset['attempt_id'], offset['resource']) async for offset in offsets]
#
#         offsets.append(None)
#
#         print(f'found chunk offsets in {round(time.time() - start_time, 4)}s')
#         return offsets
#
#     return await _find_chunks()


async def find_offsets(db, query, query_args):
    @transaction(db)
    async def _find_chunks(tx) -> List[Optional[Tuple[int, int, str]]]:
        start_time = time.time()

        await tx.just_execute('SET @rank=0;')

        offsets = tx.execute_and_fetchall(query, query_args)
        offsets = [offset async for offset in offsets]

        offsets.append(None)

        print(f'found chunk offsets in {round(time.time() - start_time, 4)}s')
        return offsets

    return await _find_chunks()


async def find_attempt_resources_offsets(db, size):
    query = f'''
SELECT t.batch_id, t.job_id, t.attempt_id, t.resource FROM (
  SELECT batch_id, job_id, attempt_id, resource
  FROM attempt_resources
  ORDER BY batch_id, job_id, attempt_id, resource
) AS t
WHERE MOD((@rank := @rank + 1), %s) = 0;
'''
    query_args = (size,)

    offsets = await find_offsets(db, query, query_args)

    def unpack_offset(offset):
        if offset is None:
            return None
        return (offset['batch_id'], offset['job_id'], offset['attempt_id'], offset['resource'])

    return [unpack_offset(offset) for offset in offsets]


async def find_agg_billing_project_resources_offsets(db, size):
    query = f'''
SELECT t.billing_project, t.resource, t.token FROM (
  SELECT billing_project, resource, token
  FROM aggregated_billing_project_resources
  ORDER BY billing_project, resource, token
) AS t
WHERE MOD((@rank := @rank + 1), %s) = 0;
'''
    query_args = (size,)

    offsets = await find_offsets(db, query, query_args)

    def unpack_offset(offset):
        if offset is None:
            return None
        return (offset['billing_project'], offset['resource'], offset['token'])

    return [unpack_offset(offset) for offset in offsets]


async def find_agg_batch_resources_offsets(db, size):
    query = f'''
SELECT t.batch_id, t.resource, t.token FROM (
  SELECT batch_id, resource, token
  FROM aggregated_batch_resources
  ORDER BY batch_id, resource, token
) AS t
WHERE MOD((@rank := @rank + 1), %s) = 0;
'''
    query_args = (size,)

    offsets = await find_offsets(db, query, query_args)

    def unpack_offset(offset):
        if offset is None:
            return None
        return (offset['batch_id'], offset['resource'], offset['token'])

    return [unpack_offset(offset) for offset in offsets]


async def find_agg_job_resources_offsets(db, size):
    query = f'''
SELECT t.batch_id, t.job_id, t.resource FROM (
  SELECT batch_id, job_id, resource
  FROM aggregated_job_resources
  ORDER BY batch_id, job_id, resource
) AS t
WHERE MOD((@rank := @rank + 1), %s) = 0;
'''
    query_args = (size,)

    offsets = await find_offsets(db, query, query_args)

    def unpack_offset(offset):
        if offset is None:
            return None
        return (offset['batch_id'], offset['job_id'], offset['resource'])

    return [unpack_offset(offset) for offset in offsets]


async def update_resource_ids(db, table_name, get_offsets_f, process_chunk_f, chunk_size):
    populate_start_time = time.time()

    chunk_counter = Counter()
    chunk_offsets = [None]
    for offset in await get_offsets_f(db, chunk_size):
        chunk_offsets.append(offset)

    chunk_offsets = list(zip(chunk_offsets[:-1], chunk_offsets[1:]))

    print(f'found {len(chunk_offsets)} chunks to process for {table_name}')

    if len(chunk_offsets) != 0:
        random.shuffle(chunk_offsets)

        burn_in_start = time.time()
        n_burn_in_chunks = 1000

        burn_in_chunk_offsets = chunk_offsets[:n_burn_in_chunks]
        chunk_offsets = chunk_offsets[n_burn_in_chunks:]

        for start_offset, end_offset in burn_in_chunk_offsets:
            await process_chunk_f(chunk_counter, db, start_offset, end_offset)

        print(f'finished burn-in in {time.time() - burn_in_start}s for {table_name}')

        parallel_insert_start = time.time()

        # 4 core database, parallelism = 10 maxes out CPU
        await bounded_gather(
            *[functools.partial(process_chunk_f, chunk_counter, db, start_offset, end_offset, quiet=False)
              for start_offset, end_offset in chunk_offsets],
            parallelism=10
        )
        print(
            f'took {time.time() - parallel_insert_start}s to update the remaining records in parallel ({(chunk_size * len(chunk_offsets)) / (time.time() - parallel_insert_start)}) attempts / sec for {table_name}')

    print(f'finished populating records in {time.time() - populate_start_time}s for {table_name}')


async def main(chunk_size=100):
    db = Database()
    await db.async_init(config_file=MYSQL_CONFIG_FILE)

    start_time = time.time()

    try:
        populate_start_time = time.time()
        
        await update_resource_ids(db, 'attempt_resources', find_attempt_resources_offsets,
                                  process_attempt_resources_chunk, chunk_size)

        await update_resource_ids(db, 'aggregated_billing_project_resources', find_agg_billing_project_resources_offsets,
                                  process_agg_billing_project_resources_chunk, chunk_size)

        await update_resource_ids(db, 'aggregated_batch_resources', find_agg_batch_resources_offsets,
                                  process_agg_batch_resources_chunk, chunk_size)

        await update_resource_ids(db, 'aggregated_job_resources', find_agg_job_resources_offsets,
                                  process_agg_job_resources_chunk, chunk_size)

        print(f'finished populating records in {time.time() - populate_start_time}s')

        audit_start_time = time.time()
        await audit_changes(db)
        print(f'finished auditing changes in {time.time() - audit_start_time}')
    finally:
        print(f'finished migration in {time.time() - start_time}s')
        await db.async_close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
