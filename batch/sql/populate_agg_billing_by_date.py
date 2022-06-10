import asyncio
import time

from gear import Database, transaction


orig_attempts_after_update_trigger = '''
CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE job_cores_mcpu INT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT cores_mcpu INTO job_cores_mcpu FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SET msec_diff = (GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0) -
                   GREATEST(COALESCE(OLD.end_time - OLD.start_time, 0), 0));

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  SELECT billing_project, resource, rand_token, msec_diff * quantity
  FROM attempt_resources
  JOIN batches ON batches.id = attempt_resources.batch_id
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  SELECT batch_id, resource, rand_token, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  SELECT batch_id, job_id, resource, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO attempts_time_msecs_diff (batch_id, job_id, attempt_id, msecs_diff)
  VALUES (NEW.batch_id, NEW.job_id, NEW.attempt_id, msec_diff);
END
'''

orig_attempts_after_insert_trigger = '''
CREATE TRIGGER attempts_after_insert AFTER INSERT ON attempts
FOR EACH ROW
BEGIN
  DECLARE msec_diff BIGINT;

  SET msec_diff = GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0);

  INSERT INTO attempts_time_msecs_diff (batch_id, job_id, attempt_id, msecs_diff)
  VALUES (NEW.batch_id, NEW.job_id, NEW.attempt_id, msec_diff);
END
'''


async def revert_to_original_state(db):
    await db.just_execute(
        '''
DELETE FROM aggregated_billing_project_resources_by_date;
DELETE FROM aggregated_batch_resources_by_date;
DELETE FROM aggregated_job_resources_by_date;
DELETE FROM attempts_time_msecs_diff;
DROP TRIGGER IF EXISTS attempts_after_update;
DROP TRIGGER IF EXISTS attempts_after_insert;
''')

    await db.just_execute(orig_attempts_after_update_trigger)
    await db.just_execute(orig_attempts_after_insert_trigger)


async def insert_into_agg_billing_project_by_date(tx, batch_id, job_id, attempt_id, resource, size):
    if batch_id is None or job_id is None or attempt_id is None or resource is None:
        where_cond = ''
        query_args = (size,)
    else:
        where_cond = 'WHERE (batch_id, job_id, attempt_id, resource) > (%s, %s, %s, %s)'
        query_args = (batch_id, job_id, attempt_id, resource, size)

    query = f'''
INSERT INTO `aggregated_billing_project_resources_by_date` (billing_project, start_time, end_time, resource, token, `usage`)
SELECT t.billing_project, t.start_time, t.end_time, t.resource, t.token, t.`usage` FROM (
  SELECT billing_project,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE)) * 1000 AS start_time,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000 AS end_time,
    resource,
    0 AS token,
    GREATEST(COALESCE(end_time - start_time, 0), 0) * quantity AS `usage`
  FROM attempt_resources
  LEFT JOIN attempts ON attempts.batch_id = attempt_resources.batch_id AND attempts.job_id = attempt_resources.job_id AND attempts.attempt_id = attempt_resources.attempt_id
  LEFT JOIN batches ON attempt_resources.batch_id = batches.id
  {where_cond}
  ORDER BY attempt_resources.batch_id, attempt_resources.job_id, attempt_resources.attempt_id, attempt_resources.resource
  LIMIT %s ) AS t
ON DUPLICATE KEY UPDATE aggregated_billing_project_resources_by_date.`usage` = aggregated_billing_project_resources_by_date.`usage` + t.`usage`;
'''

    await tx.just_execute(query, query_args)


async def insert_into_agg_batches_by_date(tx, batch_id, job_id, attempt_id, resource, size):
    if batch_id is None or job_id is None or attempt_id is None or resource is None:
        where_cond = ''
        query_args = (size,)
    else:
        where_cond = 'WHERE (batch_id, job_id, attempt_id, resource) > (%s, %s, %s, %s)'
        query_args = (batch_id, job_id, attempt_id, resource, size)

    query = f'''
INSERT INTO `aggregated_batch_resources_by_date` (batch_id, start_time, end_time, resource, token, `usage`)
SELECT t.batch_id, t.start_time, t.end_time, t.resource, t.token, t.`usage` FROM (
  SELECT attempt_resources.batch_id,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE)) * 1000 AS start_time,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000 AS end_time,
    resource,
    0 AS token,
    GREATEST(COALESCE(end_time - start_time, 0), 0) * quantity AS `usage`
  FROM attempt_resources
  LEFT JOIN attempts ON attempts.batch_id = attempt_resources.batch_id AND attempts.job_id = attempt_resources.job_id AND attempts.attempt_id = attempt_resources.attempt_id
  {where_cond}
  ORDER BY attempt_resources.batch_id, attempt_resources.job_id, attempt_resources.attempt_id, attempt_resources.resource
  LIMIT %s ) AS t
ON DUPLICATE KEY UPDATE aggregated_batch_resources_by_date.`usage` = aggregated_batch_resources_by_date.`usage` + t.`usage`;
'''

    await tx.just_execute(query, query_args)


async def insert_into_agg_jobs_by_date(tx, batch_id, job_id, attempt_id, resource, size):
    if batch_id is None or job_id is None or attempt_id is None or resource is None:
        where_cond = ''
        query_args = (size,)
    else:
        where_cond = 'WHERE (batch_id, job_id, attempt_id, resource) > (%s, %s, %s, %s)'
        query_args = (batch_id, job_id, attempt_id, resource, size)

    query = f'''
INSERT INTO `aggregated_job_resources_by_date` (batch_id, job_id, start_time, end_time, resource, `usage`)
SELECT t.batch_id, t.job_id, t.start_time, t.end_time, t.resource, t.`usage` FROM (
  SELECT attempt_resources.batch_id, attempt_resources.job_id,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE)) * 1000 AS start_time,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000 AS end_time,
    resource,
    GREATEST(COALESCE(end_time - start_time, 0), 0) * quantity AS `usage`
  FROM attempt_resources
  LEFT JOIN attempts ON attempts.batch_id = attempt_resources.batch_id AND attempts.job_id = attempt_resources.job_id AND attempts.attempt_id = attempt_resources.attempt_id
  {where_cond}
  ORDER BY attempt_resources.batch_id, attempt_resources.job_id, attempt_resources.attempt_id, attempt_resources.resource
  LIMIT %s ) AS t
ON DUPLICATE KEY UPDATE aggregated_job_resources_by_date.`usage` = aggregated_job_resources_by_date.`usage` + t.`usage`;
'''

    await tx.just_execute(query, query_args)


async def get_last_attempt_processed(tx, batch_id, job_id, attempt_id, resource, size):
    if batch_id is None or job_id is None or attempt_id is None or resource is None:
        where_cond = ''
        query_args = (size,)
    else:
        where_cond = 'WHERE (batch_id, job_id, attempt_id, resource) > (%s, %s, %s, %s)'
        query_args = (batch_id, job_id, attempt_id, resource, size)

    last_attempt_resource = await tx.execute_and_fetchone(
        f'''
SET @rank=0;
SELECT *, @rank:=@rank+1 AS n_processed FROM (
  SELECT batch_id, job_id, attempt_id, resource
  FROM attempt_resources
  {where_cond}
  ORDER BY batch_id, job_id, attempt_id, resource
  LIMIT %s ) AS t
ORDER BY t.batch_id, t.job_id, t.attempt_id, t.resource DESC
LIMIT 1;
''',
        query_args)

    if last_attempt_resource is None:
        return (None, 0)

    return ((last_attempt_resource['batch_id'], last_attempt_resource['job_id'], last_attempt_resource['attempt_id'], last_attempt_resource['resource']), last_attempt_resource['n_processed'])


async def process_attempt_diffs(tx, batch_id, job_id, attempt_id, resource, last_attempt_diff_counter):
    if batch_id is None or job_id is None or attempt_id is None or resource is None:
        where_cond = 'WHERE counter > %s'
        query_args = (last_attempt_diff_counter,)
    else:
        where_cond = 'WHERE counter > %s AND (batch_id, job_id, attempt_id, resource) > (%s, %s, %s, %s)'
        query_args = (last_attempt_diff_counter, batch_id, job_id, attempt_id, resource)

    await tx.just_execute(
        f'''
INSERT INTO `aggregated_billing_project_resources_by_date` (billing_project, start_time, end_time, resource, token, `usage`)
SELECT * FROM (
  SELECT billing_project,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE)) * 1000,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000,
    resource,
    0,
    msecs_diff * quantity AS `usage`
  FROM attempts_time_msecs_diff
  LEFT JOIN attempt_resources ON attempts_time_msecs_diff.batch_id = attempt_resources.batch_id AND
    attempts_time_msecs_diff.job_id = attempt_resources.job_id AND
    attempts_time_msecs_diff.attempt_id = attempt_resources.attempt_id
  LEFT JOIN batches ON attempts_time_msecs_diff.batch_id = batches.id
  {where_cond}
) AS t
ON DUPLICATE KEY UPDATE aggregated_billing_project_resources_by_date.`usage` = aggregated_billing_project_resources_by_date.`usage` + t.usage;
''',
        query_args
    )

    await tx.just_execute(
        f'''
INSERT INTO `aggregated_batch_resources_by_date` (batch_id, start_time, end_time, resource, token, `usage`)
SELECT * FROM (
  SELECT attempts_time_msecs_diff.batch_id,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE)) * 1000,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000,
    resource,
    0,
    msecs_diff * quantity AS `usage`
  FROM attempts_time_msecs_diff
  LEFT JOIN attempt_resources ON attempts_time_msecs_diff.batch_id = attempt_resources.batch_id AND
    attempts_time_msecs_diff.job_id = attempt_resources.job_id AND
    attempts_time_msecs_diff.attempt_id = attempt_resources.attempt_id
  {where_cond}
) AS t
ON DUPLICATE KEY UPDATE aggregated_batch_resources_by_date.`usage` = aggregated_batch_resources_by_date.`usage` + t.usage;
''',
        query_args
    )

    await tx.just_execute(
        f'''
INSERT INTO `aggregated_job_resources_by_date` (batch_id, job_id, start_time, end_time, resource, `usage`)
SELECT * FROM (
  SELECT attempts_time_msecs_diff.batch_id, attempts_time_msecs_diff.job_id,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE)) * 1000,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000,
    resource,
    msecs_diff * quantity AS `usage`
  FROM attempts_time_msecs_diff
  LEFT JOIN attempt_resources ON attempts_time_msecs_diff.batch_id = attempt_resources.batch_id AND
    attempts_time_msecs_diff.job_id = attempt_resources.job_id AND
    attempts_time_msecs_diff.attempt_id = attempt_resources.attempt_id
  {where_cond}
) AS t
ON DUPLICATE KEY UPDATE aggregated_job_resources_by_date.`usage` = aggregated_job_resources_by_date.`usage` + t.usage;
''',
        query_args
    )

    record = await tx.execute_and_fetchone(
        '''
SELECT COUNT(*) AS count FROM attempts_time_msecs_diff;
'''
    )
    return record['count']


async def process_chunk(db, batch_id, job_id, attempt_id, resource, size, last_attempt_diff_counter):
    @transaction(db)
    async def _process(tx):
        nonlocal last_attempt_diff_counter

        await tx.just_execute('SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;')

        if last_attempt_diff_counter is None:
            last_attempt_diff_counter = await tx.execute_and_fetchone('SELECT COUNT(*) AS count FROM attempts_time_msecs_diff;')
            if last_attempt_diff_counter is None:
                last_attempt_diff_counter = 0
            else:
                last_attempt_diff_counter = last_attempt_diff_counter['count']

        await insert_into_agg_billing_project_by_date(tx, batch_id, job_id, attempt_id, resource, size)
        await insert_into_agg_batches_by_date(tx, batch_id, job_id, attempt_id, resource, size)
        await insert_into_agg_jobs_by_date(tx, batch_id, job_id, attempt_id, resource, size)
        new_last_attempt, n_processed = await get_last_attempt_processed(tx, batch_id, job_id, attempt_id, resource, size)

        if new_last_attempt is None:
            return (new_last_attempt, n_processed, last_attempt_diff_counter)

        new_batch_id, new_job_id, new_attempt_id, new_resource = new_last_attempt
        new_last_attempt_diff_counter = await process_attempt_diffs(tx, new_batch_id, new_job_id, new_attempt_id, new_resource, last_attempt_diff_counter)

        return (new_last_attempt, n_processed, new_last_attempt_diff_counter)

    return await _process()


new_attempts_after_update_trigger = '''
CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE job_cores_mcpu INT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT cores_mcpu INTO job_cores_mcpu FROM jobs
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id;

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SET msec_diff = (GREATEST(COALESCE(NEW.end_time - NEW.start_time, 0), 0) -
                   GREATEST(COALESCE(OLD.end_time - OLD.start_time, 0), 0));

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  SELECT billing_project, resource, rand_token, msec_diff * quantity
  FROM attempt_resources
  JOIN batches ON batches.id = attempt_resources.batch_id
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  SELECT batch_id, resource, rand_token, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  SELECT batch_id, job_id, resource, msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_billing_project_resources_by_date (billing_project, start_time, end_time, resource, token, `usage`)
  SELECT billing_project,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE)) * 1000,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000,
    resource,
    rand_token,
    msec_diff * quantity
  FROM attempt_resources
  JOIN batches ON batches.id = attempt_resources.batch_id
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_batch_resources_by_date (batch_id, start_time, end_time, resource, token, `usage`)
  SELECT NEW.batch_id,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE)) * 1000,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000,
    resource,
    rand_token,
    msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

  INSERT INTO aggregated_job_resources_by_date (batch_id, job_id, start_time, end_time, resource, token, `usage`)
  SELECT NEW.batch_id, NEW.job_id,
    UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE)) * 1000,
    UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000,
    resource,
    rand_token,
    msec_diff * quantity
  FROM attempt_resources
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;
END
'''


new_attempt_resources_after_insert_trigger = '''
CREATE TRIGGER attempt_resources_after_insert AFTER INSERT ON attempt_resources
FOR EACH ROW
BEGIN
  DECLARE cur_start_time BIGINT;
  DECLARE cur_end_time BIGINT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE start_time_agg_key BIGINT;
  DECLARE end_time_agg_key BIGINT;

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT start_time, end_time INTO cur_start_time, cur_end_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);

  SET start_time_agg_key = UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(cur_end_time / 1000) AS DATE)) * 1000;
  SET end_time_agg_key = UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(cur_end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000;

  INSERT INTO aggregated_job_resources (batch_id, job_id, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources (batch_id, resource, token, `usage`)
  VALUES (NEW.batch_id, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_billing_project_resources (billing_project, resource, token, `usage`)
  VALUES (cur_billing_project, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_job_resources_by_date (batch_id, job_id, start_time, end_time, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, start_time_agg_key, end_time_agg_key, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources_by_date (batch_id, start_time, end_time, resource, `usage`)
  VALUES (NEW.batch_id, start_time_agg_key, end_time_agg_key, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_billing_project_resources_by_date (billing_project, start_time, end_time, resource, token, `usage`)
  VALUES (cur_billing_project, start_time_agg_key, end_time_agg_key, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;
END
'''


async def audit_changes(db):
    bad_job_records = db.select_and_fetchall(
        '''
SELECT old.batch_id, old.job_id, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT batch_id, job_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_job_resources_by_date
  LEFT JOIN resources ON aggregated_job_resources_by_date.resource = resources.resource
  GROUP BY batch_id, job_id) AS new
LEFT JOIN (
  SELECT batch_id, job_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_job_resources
  LEFT JOIN resources ON aggregated_job_resources.resource = resources.resource
  GROUP BY batch_id, job_id) AS old ON old.batch_id = new.batch_id AND old.job_id = new.job_id
WHERE ABS(new.cost - old.cost) >= 0.000000001;
''')

    async for record in bad_job_records:
        raise Exception(f'found bad job record {record}')

    bad_batch_records = db.select_and_fetchall(
        '''
SELECT old.batch_id, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT batch_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_batch_resources_by_date
  LEFT JOIN resources ON aggregated_batch_resources_by_date.resource = resources.resource
  GROUP BY batch_id) AS new
LEFT JOIN (
  SELECT batch_id, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_batch_resources
  LEFT JOIN resources ON aggregated_batch_resources.resource = resources.resource
  GROUP BY batch_id) AS old ON old.batch_id = new.batch_id
WHERE ABS(new.cost - old.cost) >= 0.000000001;
''')

    async for record in bad_batch_records:
        raise Exception(f'found bad batch record {record}')

    bad_billing_project_records = db.select_and_fetchall(
        '''
SELECT old.billing_project, old.cost, new.cost, ABS(new.cost - old.cost) AS cost_diff
FROM (
  SELECT billing_project, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_resources_by_date
  LEFT JOIN resources ON aggregated_billing_project_resources_by_date.resource = resources.resource
  GROUP BY billing_project) AS new
LEFT JOIN (
  SELECT billing_project, COALESCE(SUM(`usage` * rate), 0) AS cost
  FROM aggregated_billing_project_resources
  LEFT JOIN resources ON aggregated_billing_project_resources.resource = resources.resource
  GROUP BY billing_project) AS old ON old.billing_project = new.billing_project
WHERE ABS(new.cost - old.cost) >= 0.000000001;
''')

    async for record in bad_billing_project_records:
        raise Exception(f'found bad billing project record {record}')


async def main(chunk_size=1000):
    db = Database()
    await db.async_init()

    start_time = time.time()

    try:
        await revert_to_original_state(db)

        last_attempt_diff_counter = None
        last_batch_id = None
        last_job_id = None
        last_attempt_id = None
        last_resource = None
        n_attempt_resources_processed = 0

        while True:
            last_attempt_resource, n_processed, last_attempt_diff_counter = await process_chunk(
                db, last_batch_id, last_job_id, last_attempt_id, last_resource, chunk_size, last_attempt_diff_counter
            )

            if n_processed == 0:
                break

            last_batch_id, last_job_id, last_attempt_id, last_resource = last_attempt_resource

            n_attempt_resources_processed += n_processed
            if n_attempt_resources_processed % 1000 == 0:
                print(f'processed {n_attempt_resources_processed} attempt resources; at ({last_batch_id}, {last_job_id}, {last_attempt_id}, {last_resource})')

        @transaction(db)
        async def finish_processing(tx):
            await tx.just_execute('LOCK TABLES attempts_time_msecs_diff WRITE, attempts WRITE, attempt_resources WRITE, '
                                  'batches WRITE, aggregated_billing_project_resources_by_date WRITE, '
                                  'aggregated_batch_resources_by_date WRITE, aggregated_job_resources_by_date WRITE;')

            await process_attempt_diffs(tx, last_batch_id, last_job_id, last_attempt_id, last_resource, last_attempt_diff_counter)

            await tx.just_execute('DROP TRIGGER IF EXISTS attempts_after_update;')
            await tx.just_execute(new_attempts_after_update_trigger)

            await tx.just_execute('DROP TRIGGER IF EXISTS attempt_resources_after_insert;')
            await tx.just_execute(new_attempt_resources_after_insert_trigger)

            await tx.just_execute('DROP TRIGGER IF EXISTS attempts_after_insert;')

            await tx.just_execute('UNLOCK TABLES;')

        await finish_processing()
        await audit_changes(db)
    except Exception:
        await revert_to_original_state(db)
        raise
    finally:
        print(f'finished migration in {time.time() - start_time}s')
        await db.async_close()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
