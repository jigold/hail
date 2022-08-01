import asyncio
import functools
import os
import random
import time

from gear import Database, transaction
from hailtop.utils import bounded_gather, secret_alnum_string


MYSQL_CONFIG_FILE = os.environ.get('MYSQL_CONFIG_FILE')


class Counter:
    def __init__(self):
        self.n = 0


def offsets_to_where_statement(start_offset, end_offset):
    assert start_offset != end_offset, str((start_offset, end_offset))

    if start_offset is None:
        start_batch_id, start_job_id, start_attempt_id = None, None, None
    else:
        start_batch_id, start_job_id, start_attempt_id = start_offset

    if end_offset is None:
        end_batch_id, end_job_id, end_attempt_id = None, None, None
    else:
        end_batch_id, end_job_id, end_attempt_id = end_offset

    if start_batch_id is None or start_job_id is None or start_attempt_id is None:
        assert end_batch_id
        where_cond = 'WHERE attempts.batch_id < %s OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id < %s) OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id = %s AND attempts.attempt_id < %s)'
        query_args = (end_batch_id, end_batch_id, end_job_id, end_batch_id, end_job_id, end_attempt_id)
    elif end_batch_id is None or end_job_id is None or end_attempt_id is None:
        assert start_batch_id
        where_cond = 'WHERE attempts.batch_id > %s OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id > %s) OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id = %s AND attempts.attempt_id >= %s)'
        query_args = (start_batch_id, start_batch_id, start_job_id, start_batch_id, start_job_id, start_attempt_id)
    else:
        where_cond = 'WHERE (attempts.batch_id > %s OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id > %s) OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id = %s AND attempts.attempt_id >= %s)) ' \
                     'AND (attempts.batch_id < %s OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id < %s) OR ' \
                     '(attempts.batch_id = %s AND attempts.job_id = %s AND attempts.attempt_id < %s))'
        query_args = (start_batch_id, start_batch_id, start_job_id, start_batch_id, start_job_id, start_attempt_id,
                      end_batch_id, end_batch_id, end_job_id, end_batch_id, end_job_id, end_attempt_id)

    return (where_cond, query_args)


async def process_chunk(counter, db, start_offset, end_offset, quiet=True):
    start_time = time.time()

    where_cond, query_args = offsets_to_where_statement(start_offset, end_offset)

    await db.just_execute(
        f'''
UPDATE attempts
SET dummy_aggregated_by_date = dummy_aggregated_by_date + 1
{where_cond}
''',
        query_args)

    if not quiet and counter.n % 100 == 0:
        print(f'processed chunk ({start_offset}, {end_offset}) in {time.time() - start_time}s')

    counter.n += 1
    if counter.n % 500 == 0:
        print(f'processed {counter.n} complete chunks')


async def audit_changes(db):
    job_audit_start = time.time()
    print('starting auditing job records')

    bad_job_records = db.select_and_fetchall(
        '''
SELECT old.batch_id, old.job_id, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT batch_id, job_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_job_resources
  LEFT JOIN batches ON batches.id = aggregated_job_resources.batch_id
  LEFT JOIN resources ON aggregated_job_resources.resource = resources.resource
  WHERE format_version >= 3
  GROUP BY batch_id, job_id
) AS old
LEFT JOIN (
  SELECT batch_id, job_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM (
    SELECT batch_id, job_id, resource_id, CAST(COALESCE(SUM(`usage`), 0) AS SIGNED) AS `usage`
    FROM aggregated_job_resources_by_date
    GROUP BY batch_id, job_id, resource_id
  ) AS t
  LEFT JOIN resources ON t.resource_id = resources.resource_id
  GROUP BY batch_id, job_id
) AS new ON old.batch_id = new.batch_id AND old.job_id = new.job_id
WHERE ABS(new.cost - old.cost) >= 0.000001
LIMIT 100;
''')

    bad_job_records = [record async for record in bad_job_records]
    for record in bad_job_records:
        print(f'found bad job record {record}')

    print(f'finished auditing job records in {time.time() - job_audit_start}s')

    batch_audit_start = time.time()
    print('starting auditing batch records')

    bad_batch_records = db.select_and_fetchall(
        '''
SELECT old.batch_id, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT batch_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM (
    SELECT batch_id, resource, CAST(COALESCE(SUM(`usage`), 0) AS SIGNED) AS `usage`
    FROM aggregated_batch_resources
    LEFT JOIN batches ON batches.id = aggregated_batch_resources.batch_id
    WHERE format_version >= 3
    GROUP BY batch_id, resource
  ) AS t1
  LEFT JOIN resources ON t1.resource = resources.resource
  GROUP BY batch_id
) AS old
LEFT JOIN (
  SELECT batch_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM (
    SELECT batch_id, resource_id, CAST(COALESCE(SUM(`usage`), 0) AS SIGNED) AS `usage`
    FROM aggregated_batch_resources_by_date
    GROUP BY batch_id, resource_id
  ) AS t2
  LEFT JOIN resources ON t2.resource_id = resources.resource_id
  GROUP BY batch_id
) AS new ON old.batch_id = new.batch_id
WHERE ABS(new.cost - old.cost) >= 0.000001
LIMIT 100;
''')

    bad_batch_records = [record async for record in bad_batch_records]
    for record in bad_batch_records:
        print(f'found bad batch record {record}')

    print(f'finished auditing batch records in {time.time() - batch_audit_start}s')

    # cannot audit billing project records because they are partially filled in from batches with format version < 3

    if bad_job_records or bad_batch_records:
        raise Exception(f'errors found in audit')


async def find_complete_chunk_offsets(db, size):
    @transaction(db)
    async def _find_chunks(tx):
        start_time = time.time()

        # find first batch id where the state is running

        records = tx.execute_and_fetchall(
            '''
SELECT id FROM batches
WHERE state = %s
ORDER BY id
LIMIT 1;
''',
            ('running',))

        records = [record async for record in records]

        if len(records) == 0:
            return []

        assert len(records) == 1
        first_running_batch_id = records[0]['id']

        print(f'first running batch id {first_running_batch_id}')

        await tx.just_execute('SET @rank=0;')

        query = f'''
SELECT t.batch_id, t.job_id, t.attempt_id FROM (
  SELECT attempts.batch_id, attempts.job_id, attempts.attempt_id
  FROM attempts
  LEFT JOIN batches ON attempts.batch_id = batches.id
  WHERE batch_id < %s
  ORDER BY attempts.batch_id, attempts.job_id, attempts.attempt_id
) AS t
WHERE MOD((@rank := @rank + 1), %s) = 0;
'''

        offsets = tx.execute_and_fetchall(query, (first_running_batch_id, size,))
        offsets = [(offset['batch_id'], offset['job_id'], offset['attempt_id']) async for offset in offsets]

        last_offset = tx.execute_and_fetchall(
            '''
SELECT batch_id, job_id, attempt_id
FROM attempts
WHERE batch_id >= %s
ORDER BY batch_id ASC, job_id ASC, attempt_id ASC
LIMIT 1;
''',
            (first_running_batch_id,))

        last_offset = [(offset['batch_id'], offset['job_id'], offset['attempt_id']) async for offset in last_offset]
        assert len(last_offset) == 1
        last_offset = last_offset[0]
        offsets.append(last_offset)

        print(f'found chunk offsets in {round(time.time() - start_time, 4)}s')
        return offsets

    return await _find_chunks()


async def main(chunk_size=100):
    db = Database()
    await db.async_init(config_file=MYSQL_CONFIG_FILE)

    start_time = time.time()

    try:
        populate_start_time = time.time()

        chunk_counter = Counter()
        chunk_offsets = [None]
        for offset in await find_complete_chunk_offsets(db, chunk_size):
            chunk_offsets.append(offset)

        chunk_offsets = list(zip(chunk_offsets[:-1], chunk_offsets[1:]))

        print(f'found {len(chunk_offsets)} chunks to process')

        if len(chunk_offsets) != 0:
            random.shuffle(chunk_offsets)

            burn_in_start = time.time()
            n_burn_in_chunks = 1000

            burn_in_chunk_offsets = chunk_offsets[:n_burn_in_chunks]
            chunk_offsets = chunk_offsets[n_burn_in_chunks:]  # processing a chunk is not idempotent

            for start_offset, end_offset in burn_in_chunk_offsets:
                await process_chunk(chunk_counter, db, start_offset, end_offset)

            print(f'finished burn-in in {time.time() - burn_in_start}s')

            parallel_insert_start = time.time()

            # 4 core database, parallelism = 10 maxes out CPU
            await bounded_gather(
                *[functools.partial(process_chunk, chunk_counter, db, start_offset, end_offset, quiet=False)
                  for start_offset, end_offset in chunk_offsets],
                parallelism=10
            )
            print(f'took {time.time() - parallel_insert_start}s to insert the remaining complete records in parallel ({(chunk_size * len(chunk_offsets)) / (time.time() - parallel_insert_start)}) attempts / sec')

        @transaction(db)
        async def finish_processing(tx):
            # no need to lock tables as any new writes during the update will
            # be automatically aggregated and the update is idempotent

            await tx.just_execute(
                '''
CREATE TEMPORARY TABLE unaggregated_attempts AS (
SELECT attempts.batch_id, attempts.job_id, attempts.attempt_id
FROM attempts
LEFT JOIN attempts_aggregated_by_date
  ON attempts.batch_id = attempts_aggregated_by_date.batch_id AND
    attempts.job_id = attempts_aggregated_by_date.job_id AND
    attempts.attempt_id = attempts_aggregated_by_date.attempt_id
WHERE attempts_aggregated_by_date.attempt_id IS NULL
);
''')

            await tx.just_execute(
                '''
UPDATE attempts
INNER JOIN unaggregated_attempts ON
  attempts.batch_id = unaggregated_attempts.batch_id AND
  attempts.job_id = unaggregated_attempts.job_id AND
  attempts.attempt_id = unaggregated_attempts.attempt_id
SET dummy_aggregated_by_date = dummy_aggregated_by_date + 1;
''')

            await tx.just_execute(
                '''
DROP TEMPORARY TABLE unaggregated_attempts;
''')

        await finish_processing()

        print(f'finished populating records in {time.time() - populate_start_time}s')

        audit_start_time = time.time()
        await audit_changes(db)
        print(f'finished auditing changes in {time.time() - audit_start_time}')
    finally:
        print(f'finished migration in {time.time() - start_time}s')
        await db.async_close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
