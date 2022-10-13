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
SET aggregated_in_v3 = TRUE
{where_cond}
''',
        query_args)

    if not quiet and counter.n % 100 == 0:
        print(f'processed chunk ({start_offset}, {end_offset}) in {time.time() - start_time}s')

    counter.n += 1
    if counter.n % 500 == 0:
        print(f'processed {counter.n} complete chunks')


async def audit_changes(db):
    print('starting auditing billing project records')

    bad_bp_user_records = db.select_and_fetchall(
        '''
SELECT old.billing_project, old.user, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT billing_project, `user`, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_user_resources_v2
  LEFT JOIN resources ON aggregated_billing_project_user_resources_v2.resource_id = resources.resource_id
  GROUP BY billing_project, `user`
) AS new ON old.batch_id = new.batch_id AND old.job_id = new.job_id
LEFT JOIN (
  SELECT billing_project, user, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_id_user_resources_v3
  LEFT JOIN resources ON aggregated_billing_project_id_user_resources_v3.resource_id = resources.resource_id
  LEFT JOIN billing_projects ON billing_projects.billing_project_id = aggregated_billing_project_id_user_resources_v3.billing_project_id
  GROUP BY billing_project, user
) AS new ON old.billing_project = new.billing_project AND old.user = new.user
WHERE ABS(new.cost - old.cost) >= 0.000001
LIMIT 100;
''')

    bad_bp_user_records = [record async for record in bad_bp_user_records]
    failing_bp_users = []
    for record in bad_bp_user_records:
        print(f'found bad billing project user record {record}')

    bad_bp_user_records_by_date = db.select_and_fetchall(
        '''
SELECT old.billing_date, old.billing_project, old.user, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT billing_date, billing_project, `user`, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_user_resources_by_date_v2
  LEFT JOIN resources ON aggregated_billing_project_user_resources_by_date_v2.resource_id = resources.resource_id
  GROUP BY billing_date, billing_project, `user`
) AS new ON old.batch_id = new.batch_id AND old.job_id = new.job_id
LEFT JOIN (
  SELECT billing_date, billing_project, user, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_id_user_resources_by_date_v3
  LEFT JOIN resources ON aggregated_billing_project_id_user_resources_by_date_v3.resource_id = resources.resource_id
  LEFT JOIN billing_projects ON billing_projects.billing_project_id = aggregated_billing_project_id_user_resources_by_date_v3.billing_project_id
  GROUP BY billing_date, billing_project, user
) AS new ON old.billing_project = new.billing_project AND old.user = new.user
WHERE ABS(new.cost - old.cost) >= 0.000001
LIMIT 100;
    ''')

    bad_bp_user_records_by_date = [record async for record in bad_bp_user_records_by_date]
    failing_bp_users_by_date = []
    for record in bad_bp_user_records_by_date:
        print(f'found bad billing project user by date record {record}')

    if failing_bp_users or failing_bp_users_by_date:
        raise Exception(f'errors found in audit')


async def find_chunk_offsets(db, size):
    @transaction(db)
    async def _find_chunks(tx) -> List[Optional[Tuple[int, int, str]]]:
        start_time = time.time()

        await tx.just_execute('SET @rank=0;')

        query = f'''
SELECT t.batch_id, t.job_id, t.attempt_id FROM (
  SELECT batch_id, job_id, attempt_id
  FROM attempts
  ORDER BY batch_id, job_id, attempt_id
) AS t
WHERE MOD((@rank := @rank + 1), %s) = 0;
'''

        offsets = tx.execute_and_fetchall(query, (size,))
        offsets = [(offset['batch_id'], offset['job_id'], offset['attempt_id']) async for offset in offsets]
        offsets.append(None)

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
        for offset in await find_chunk_offsets(db, chunk_size):
            chunk_offsets.append(offset)

        chunk_offsets = list(zip(chunk_offsets[:-1], chunk_offsets[1:]))

        if chunk_offsets != [(None, None)]:
            print(f'found {len(chunk_offsets)} chunks to process')

            random.shuffle(chunk_offsets)

            burn_in_start = time.time()
            n_burn_in_chunks = 10000

            burn_in_chunk_offsets = chunk_offsets[:n_burn_in_chunks]
            chunk_offsets = chunk_offsets[n_burn_in_chunks:]

            for start_offset, end_offset in burn_in_chunk_offsets:
                await process_chunk(chunk_counter, db, start_offset, end_offset, quiet=False)

            print(f'finished burn-in in {time.time() - burn_in_start}s')

            parallel_insert_start = time.time()

            # 4 core database, parallelism = 10 maxes out CPU
            await bounded_gather(
                *[functools.partial(process_chunk, chunk_counter, db, start_offset, end_offset, quiet=False)
                  for start_offset, end_offset in chunk_offsets],
                parallelism=10
            )
            print(f'took {time.time() - parallel_insert_start}s to insert the remaining complete records in parallel ({(chunk_size * len(chunk_offsets)) / (time.time() - parallel_insert_start)}) attempts / sec')

        print(f'finished populating records in {time.time() - populate_start_time}s')

        audit_start_time = time.time()
        await audit_changes(db)
        print(f'finished auditing changes in {time.time() - audit_start_time}')
    finally:
        print(f'finished migration in {time.time() - start_time}s')
        await db.async_close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
