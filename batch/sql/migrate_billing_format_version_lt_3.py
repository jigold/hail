import asyncio
import time

from gear import Database, transaction


async def delete_previous_work(db):
    @transaction(db)
    async def _delete(tx):
        await tx.just_execute(
            '''
DELETE FROM `attempt_resources`
WHERE (batch_id, job_id, attempt_id) IN (
  SELECT batch_id, job_id, attempt_id
  FROM attempts
  LEFT JOIN batches ON attempts.batch_id = batches.id
  WHERE format_version < 3
);
''')

        await tx.just_execute(
            '''
UPDATE `aggregated_billing_project_resources`
SET `usage` = 0
WHERE token = -1;
''')
        await tx.just_execute(
            '''
UPDATE `aggregated_batch_resources`
SET `usage` = 0
WHERE token = -1;
''')

        await tx.just_execute(
            '''
UPDATE `aggregated_job_resources`
SET `usage` = 0
WHERE (batch_id, job_id) IN (
  SELECT batch_id, job_id
  FROM jobs
  LEFT JOIN batches ON jobs.batch_id = batches.id
  WHERE format_version < 3
);
''')

    await _delete()


async def process_chunk(db, batch_id, job_id, attempt_id, size):
    @transaction(db)
    async def _process(tx):
        resources = [
            ('compute/n1-preemptible/1', 'cores_mcpu'),
            ('memory/n1-preemptible/1', '3840 * (cores_mcpu / 1000)'),  # standard worker has 3840 mi per core
            ('boot-disk/pd-ssd/1', '100 * 64 * (cores_mcpu / 1000)'),  # worker fraction assumes there are 16 cores and 100 gi of disk
            ('ip-fee/1024/1', '64 * (cores_mcpu / 1000)'),
            ('service-fee/1', 'cores_mcpu'),
        ]

        for resource, quantity in resources:
            if batch_id is None or job_id is None or attempt_id is None:
                query = f'''
INSERT INTO attempt_resources (resource, quantity)
SELECT %s, {quantity} FROM attempts
LEFT JOIN batches ON attempts.batch_id = batches.id
LEFT JOIN jobs ON attempts.batch_id = jobs.batch_id AND attempts.job_id = jobs.job_id
WHERE format_version < 3
ORDER BY attempts.batch_id, attempts.job_id, attempts.attempt_id
LIMIT %s;
'''
                query_args = (resource, size)
            else:
                query = f'''
INSERT INTO attempt_resources (resource, quantity)
SELECT %s, {quantity} FROM attempts
LEFT JOIN batches ON attempts.batch_id = batches.id
LEFT JOIN jobs ON attempts.batch_id = jobs.batch_id AND attempts.job_id = jobs.job_id
WHERE (batch_id, job_id, attempt_id) > (%s, %s, %s) AND format_version < 3
ORDER BY attempts.batch_id, attempts.job_id, attempts.attempt_id
LIMIT %s;
'''
                query_args = (resource, batch_id, job_id, attempt_id, size)

            await tx.just_execute(query, query_args)

        if batch_id is None or job_id is None or attempt_id is None:
            where_cond = 'WHERE format_version < 3'
            query_args = (size,)
        else:
            where_cond = 'WHERE format_version < 3 AND (batch_id, job_id, attempt_id) > (%s, %s, %s)'
            query_args = (batch_id, job_id, attempt_id, size)

        last_attempt = await tx.execute_and_fetchone(
            f'''
SET @rank=0;
SELECT *, @rank:=@rank+1 AS n_processed FROM (
  SELECT batch_id, job_id, attempt_id
  FROM attempts
  LEFT JOIN batches ON attempts.batch_id = batches.id
  {where_cond}
  ORDER BY attempts.batch_id, attempts.job_id, attempts.attempt_id
  LIMIT %s ) AS t
ORDER BY t.batch_id, t.job_id, t.attempt_id DESC
LIMIT 1;
''',
            query_args)

        if last_attempt is None:
            return (None, 0)

        return ((last_attempt['batch_id'], last_attempt['job_id'], last_attempt['attempt_id']), last_attempt['n_processed'])

    return await _process()


async def audit_changes(db):
    bad_job_records = db.select_and_fetchall(
        '''
SELECT old.batch_id, old.job_id, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT batch_id, job_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_job_resources
  LEFT JOIN batches ON aggregated_job_resources.batch_id = batches.id
  LEFT JOIN resources ON aggregated_job_resources.resource = resources.resource
  WHERE format_version < 3
  GROUP BY batch_id, job_id) AS new
LEFT JOIN (
  SELECT batch_id, job_id, (jobs.msec_mcpu * 0.001 * 0.001) * ((0.01 + ((0.17 * 100 / 30.4375 / 24 + 0.004) / 16) + 0.01) / 3600) AS cost
  FROM jobs
  LEFT JOIN batches ON jobs.batch_id = batches.id
  WHERE format_version < 3) AS old ON new.batch_id = old.batch_id AND new.job_id = old.job_id
WHERE ABS(new.cost - old.cost) >= 0.000000001;
''')

    async for record in bad_job_records:
        raise Exception(f'found bad job record {record}')

    bad_batch_records = db.select_and_fetchall(
        '''
SELECT old.batch_id, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT batch_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_batch_resources
  LEFT JOIN batches ON aggregated_batch_resources.batch_id = batches.id
  LEFT JOIN resources ON aggregated_batch_resources.resource = resources.resource
  WHERE format_version < 3
  GROUP BY batch_id) AS new
LEFT JOIN (
  SELECT batch_id, COALESCE(SUM((jobs.msec_mcpu * 0.001 * 0.001) * ((0.01 + ((0.17 * 100 / 30.4375 / 24 + 0.004) / 16) + 0.01) / 3600)), 0) AS cost
  FROM jobs
  LEFT JOIN batches ON jobs.batch_id = batches.id
  WHERE format_version < 3
  GROUP BY batch_id) AS old ON new.batch_id = old.batch_id
WHERE ABS(new.cost - old.cost) >= 0.000000001;
''')

    async for record in bad_batch_records:
        raise Exception(f'found bad batch record {record}')

    bad_billing_project_records = db.select_and_fetchall(
        '''
SELECT old.billing_project, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT billing_project, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_resources
  LEFT JOIN resources ON aggregated_billing_project_resources.resource = resources.resource
  WHERE token = -1
  GROUP BY billing_project) AS new
LEFT JOIN (
  SELECT billing_project, COALESCE(SUM((jobs.msec_mcpu * 0.001 * 0.001) * ((0.01 + ((0.17 * 100 / 30.4375 / 24 + 0.004) / 16) + 0.01) / 3600)), 0) AS cost
  FROM jobs
  LEFT JOIN batches ON jobs.batch_id = batches.id
  WHERE format_version < 3
  GROUP BY billing_project) AS old ON new.billing_project = old.billing_project
WHERE ABS(new.cost - old.cost) >= 0.000000001;
''')

    async for record in bad_billing_project_records:
        raise Exception(f'found bad billing project record {record}')


async def main(chunk_size=1000):
    db = Database()
    await db.async_init()

    start_time = time.time()

    try:
        await delete_previous_work(db)

        count = await db.select_and_fetchone(
            '''
SELECT COUNT(*) as count FROM attempts
LEFT JOIN batches ON attempts.batch_id = batches.id
WHERE format_version < 3;
'''
        )
        n_attempts_expected = count['count']
        print(f'expecting to process {n_attempts_expected} attempts')

        last_batch_id = None
        last_job_id = None
        last_attempt_id = None
        n_attempts_processed = 0
        while True:
            last_attempt, n_processed = await process_chunk(db, last_batch_id, last_job_id, last_attempt_id, chunk_size)

            if n_processed == 0:
                break

            last_batch_id, last_job_id, last_attempt_id = last_attempt

            n_attempts_processed += n_processed
            if n_attempts_processed % 1000 == 0:
                print(f'processed {n_attempts_processed} attempts; at ({last_batch_id}, {last_job_id}, {last_attempt_id})')

        assert n_attempts_expected == n_attempts_processed

        await audit_changes(db)
    except Exception:
        await delete_previous_work(db)
        raise
    finally:
        print(f'finished migration in {time.time() - start_time}s')
        await db.async_close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
