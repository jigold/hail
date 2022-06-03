import aiomysql
import asyncio
import time
import datetime

from gear import Database, transaction
from hailtop.utils import secret_alnum_string


def get_resources_fmt_version_less_than_3(cores_mcpu):
    return [
        ('compute/n1-preemptible/1', cores_mcpu),
        ('memory/n1-preemptible/1', 3840 * (cores_mcpu // 1000)),  # standard worker has 3840 mi per core
        ('boot-disk/pd-ssd/1', 100 * 64 * (cores_mcpu // 1000)),
        # worker fraction assumes there are 16 cores and 100 gi of disk
        ('ip-fee/1024/1', 64 * (cores_mcpu // 1000)),
        ('service-fee/1', cores_mcpu),
    ]


def create_tables_sql(token):
    return f'''
CREATE TABLE IF NOT EXISTS `attempt_resources_tmp_{token}` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `attempt_id` VARCHAR(40) NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `quantity` BIGINT NOT NULL,
  PRIMARY KEY (`batch_id`, `job_id`, `attempt_id`, `resource`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(`batch_id`, `job_id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`, `attempt_id`) REFERENCES attempts(`batch_id`, `job_id`, `attempt_id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `aggregated_billing_project_resources_tmp_{token}` (
  `billing_project` VARCHAR(100) NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`billing_project`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`billing_project`) REFERENCES billing_projects(name) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `aggregated_batch_resources_tmp_{token}` (
  `batch_id` BIGINT NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;

CREATE TABLE IF NOT EXISTS `aggregated_job_resources_tmp_{token}` (
  `batch_id` BIGINT NOT NULL,
  `job_id` INT NOT NULL,
  `start_time` BIGINT NOT NULL,
  `end_time` BIGINT NOT NULL,
  `resource` VARCHAR(100) NOT NULL,
  `token` INT NOT NULL DEFAULT 0,
  `usage` BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (`batch_id`, `job_id`, `start_time`, `end_time`, `resource`, `token`),
  FOREIGN KEY (`batch_id`) REFERENCES batches(`id`) ON DELETE CASCADE,
  FOREIGN KEY (`batch_id`, `job_id`) REFERENCES jobs(`batch_id`, `job_id`) ON DELETE CASCADE,
  FOREIGN KEY (`resource`) REFERENCES resources(`resource`) ON DELETE CASCADE
) ENGINE = InnoDB;
'''


# Note these commands must be run in the CLI and not in aiomysql
def create_attempt_resources_tmp_after_insert_trigger(token):
    return f'''
DELIMITER $$

DROP TRIGGER IF EXISTS attempt_resources_tmp_{token}_after_insert $$
CREATE TRIGGER attempt_resources_tmp_{token}_after_insert AFTER INSERT ON attempt_resources_tmp_{token}
FOR EACH ROW
BEGIN
  DECLARE cur_start_time BIGINT;
  DECLARE cur_end_time BIGINT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE start_billing_period BIGINT;
  DECLARE end_billing_period BIGINT;

  SELECT n_tokens INTO cur_n_tokens FROM globals LOCK IN SHARE MODE;
  SET rand_token = FLOOR(RAND() * cur_n_tokens);

  SELECT billing_project INTO cur_billing_project FROM batches WHERE id = NEW.batch_id;

  SELECT start_time, end_time INTO cur_start_time, cur_end_time
  FROM attempts
  WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
  LOCK IN SHARE MODE;

  SET msec_diff = GREATEST(COALESCE(cur_end_time - cur_start_time, 0), 0);

  SET start_billing_period = UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(cur_end_time / 1000) AS DATE)) * 1000;
  SET end_billing_period = UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(cur_end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000;

  INSERT INTO aggregated_job_resources_tmp_{token} (batch_id, job_id, start_time, end_time, resource, `usage`)
  VALUES (NEW.batch_id, NEW.job_id, start_billing_period, end_billing_period, NEW.resource, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_batch_resources_tmp_{token} (batch_id, start_time, end_time, resource, token, `usage`)
  VALUES (NEW.batch_id, start_billing_period, end_billing_period, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;

  INSERT INTO aggregated_billing_project_resources_tmp_{token} (billing_project, start_time, end_time, resource, token, `usage`)
  VALUES (cur_billing_project, start_billing_period, end_billing_period, NEW.resource, rand_token, NEW.quantity * msec_diff)
  ON DUPLICATE KEY UPDATE
    `usage` = `usage` + NEW.quantity * msec_diff;
END $$

DELIMITER ;
'''


# Note these commands must be run in the CLI and not in aiomysql
def create_attempts_after_update_trigger(token):
    return f'''
DELIMITER $$

DROP TRIGGER IF EXISTS attempts_after_update $$
CREATE TRIGGER attempts_after_update AFTER UPDATE ON attempts
FOR EACH ROW
BEGIN
  DECLARE job_cores_mcpu INT;
  DECLARE cur_billing_project VARCHAR(100);
  DECLARE msec_diff BIGINT;
  DECLARE cur_n_tokens INT;
  DECLARE rand_token INT;
  DECLARE start_billing_period BIGINT;
  DECLARE end_billing_period BIGINT;

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

  IF msec_diff > 0 THEN
    SET start_billing_period = UNIX_TIMESTAMP(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE)) * 1000;
    SET end_billing_period = UNIX_TIMESTAMP(ADDDATE(CAST(FROM_UNIXTIME(NEW.end_time / 1000) AS DATE), INTERVAL 1 DAY)) * 1000;

    INSERT INTO aggregated_billing_project_resources_tmp_{token} (billing_project, start_time, end_time, resource, token, `usage`)
    SELECT billing_project, start_billing_period, end_billing_period, resource, rand_token, msec_diff * quantity
    FROM attempt_resources
    JOIN batches ON batches.id = attempt_resources.batch_id
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

    INSERT INTO aggregated_batch_resources_tmp_{token} (batch_id, start_time, end_time, resource, token, `usage`)
    SELECT batch_id, start_billing_period, end_billing_period, resource, rand_token, msec_diff * quantity
    FROM attempt_resources
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;

    INSERT INTO aggregated_job_resources_tmp_{token} (batch_id, job_id, start_time, end_time, resource, `usage`)
    SELECT batch_id, job_id, start_billing_period, end_billing_period, resource, msec_diff * quantity
    FROM attempt_resources
    WHERE batch_id = NEW.batch_id AND job_id = NEW.job_id AND attempt_id = NEW.attempt_id
    ON DUPLICATE KEY UPDATE `usage` = `usage` + msec_diff * quantity;
  END IF;
END $$

DELIMITER ;
'''


def revert_create_tables_sql(token):
    return f'''
DROP TABLE IF EXISTS `attempt_resources_tmp_{token}`;
DROP TABLE IF EXISTS `aggregated_billing_project_resources_tmp_{token}`;
DROP TABLE IF EXISTS `aggregated_batch_resources_tmp_{token}`;
DROP TABLE IF EXISTS `aggregated_job_resources_tmp_{token}`;
'''

def revert_update_attempts_trigger_sql(token):
    



async def write(db, token, attempt_resources_data, agg_billing_project_data, agg_batch_data, agg_job_data):
    @transaction(db)
    async def _write_all(tx):
        await tx.execute_many(
            f'''
INSERT INTO attempt_resources_tmp_{token} (batch_id, job_id, attempt_id, resource, quantity)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE quantity = quantity;
''',
            attempt_resources_data)

        await tx.execute_many(
            f'''
INSERT INTO aggregated_billing_project_resources_tmp_{token} (billing_project, start_time, end_time, resource, token, `usage`)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE `usage` = `usage` + VALUES(`usage`);
''',
            agg_billing_project_data)

        await tx.execute_many(
            f'''
INSERT INTO aggregated_batch_resources_tmp_{token} (batch_id, start_time, end_time, resource, token, `usage`)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE `usage` = `usage` + VALUES(`usage`);
''',
            agg_batch_data)

        await tx.execute_many(
            f'''
INSERT INTO aggregated_job_resources_tmp_{token} (batch_id, job_id, start_time, end_time, resource, `usage`)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE `usage` = `usage` + VALUES(`usage`);
''',
            agg_job_data)

    return await _write_all()


async def main():
    db = Database()
    await db.async_init(config_file='/home/jigold/batch-dump-1-config.json', cursorclass=aiomysql.cursors.SSDictCursor)

    start_time = time.time()
    token = secret_alnum_string(6)

    try:
        attempt_resources = []
        agg_billing_project_resources = []
        agg_batch_resources = []
        agg_job_resources = []

        n_attempts = 0

        async for record in db.select_and_fetchall(
                '''
SELECT attempts.batch_id, attempts.job_id, attempts.attempt_id, cores_mcpu, format_version, resource, quantity, start_time, end_time, billing_project
FROM attempts
LEFT JOIN attempt_resources ON attempts.batch_id = attempt_resources.batch_id
  AND attempts.job_id = attempt_resources.job_id
  AND attempts.attempt_id = attempt_resources.attempt_id
LEFT JOIN batches ON attempts.batch_id = batches.id
LEFT JOIN jobs ON attempts.batch_id = jobs.batch_id AND attempts.job_id = jobs.job_id;
'''):
            if n_attempts % 1000 == 0:
                print(f'processed {n_attempts} attempts: elapsed time {time.time() - start_time}s')
                break

            n_attempts += 1

            if (len(attempt_resources) >= 1000 or
                    len(agg_billing_project_resources) >= 1000 or
                    len(agg_batch_resources) >= 1000 or
                    len(agg_job_resources) >= 1000
            ):
                await write(db, token, attempt_resources, agg_billing_project_resources, agg_batch_resources, agg_job_resources)
                attempt_resources = []
                agg_billing_project_resources = []
                agg_batch_resources = []
                agg_job_resources = []
                break

            if record['format_version'] < 3:
                resources = get_resources_fmt_version_less_than_3(record['cores_mcpu'])
            else:
                resources = [(record['resource'], record['quantity'])]

            for resource, quantity in resources:
                attempt_resources.append((record['batch_id'], record['job_id'], record['attempt_id'], resource, quantity))

            if record['start_time'] is not None and record['end_time'] is not None:
                start_billing_period = datetime.datetime.utcfromtimestamp(record['end_time'] / 1000)
                start_billing_period = datetime.datetime(start_billing_period.year, start_billing_period.month,
                                                         start_billing_period.day, tzinfo=datetime.timezone.utc)
                start_billing_period_utc = start_billing_period.timestamp() * 1000

                end_billing_period = start_billing_period + datetime.timedelta(days=1)
                end_billing_period_utc = end_billing_period.timestamp() * 1000

                duration_msecs = max(0, record['end_time'] - record['start_time'])

                for resource, quantity in resources:
                    usage = duration_msecs * quantity

                    agg_billing_project_resources.append(
                        (record['billing_project'], start_billing_period_utc, end_billing_period_utc, resource, 0, usage)
                    )
                    agg_batch_resources.append(
                        (record['batch_id'], start_billing_period_utc, end_billing_period_utc, resource, 0, usage)
                    )
                    agg_job_resources.append(
                        (record['batch_id'], record['job_id'], start_billing_period_utc, end_billing_period_utc, resource, usage)
                    )

            n_attempts += 1

        await write(db, token, attempt_resources, agg_billing_project_resources, agg_batch_resources, agg_job_resources)
        attempt_resources = []
        agg_billing_project_resources = []
        agg_batch_resources = []
        agg_job_resources = []

        print(f'finished inserting attempts and aggregated resources in {time.time() - start_time}s')
    except Exception:
        # revert trigger and cleanup tables
        raise
    finally:
        await db.async_close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
