import asyncio
import time
import datetime

from gear import Database


async def main():
    db = Database()
    await db.async_init(config_file='/home/jigold/batch-dump-1-config.json')

    start_time = time.time()

    def get_resources_fmt_version_less_than_3(cores_mcpu):
        return [
            ('compute/n1-preemptible/1', cores_mcpu),
            ('memory/n1-preemptible/1', 3840 * (cores_mcpu // 1000)),  # standard worker has 3840 mi per core
            ('boot-disk/pd-ssd/1', 100 * 64 * (cores_mcpu // 1000)),  # worker fraction assumes there are 16 cores and 100 gi of disk
            ('ip-fee/1024/1', 64 * (cores_mcpu // 1000)),
            ('service-fee/1', cores_mcpu),
        ]

    async def write(attempt_resources_data, agg_billing_project_data, agg_batch_data, agg_job_data):
        await db.execute_many(
            '''
INSERT INTO attempt_resources_tmp_1 (batch_id, job_id, attempt_id, resource, quantity)
VALUES (%s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE quantity = quantity;
''',
            attempt_resources_data)

        await db.execute_many(
            '''
INSERT INTO aggregated_billing_project_resources_tmp_1 (billing_project, start_time, end_time, resource, token, `usage`)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE `usage` = `usage` + VALUES(`usage`);
''',
            agg_billing_project_data)

        await db.execute_many(
            '''
INSERT INTO aggregated_batch_resources_tmp_1 (batch_id, start_time, end_time, resource, token, `usage`)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE `usage` = `usage` + VALUES(`usage`);
''',
            agg_batch_data)

        await db.execute_many(
            '''
INSERT INTO aggregated_job_resources_tmp_1 (batch_id, job_id, start_time, end_time, resource, `usage`)
VALUES (%s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE `usage` = `usage` + VALUES(`usage`);
''',
            agg_job_data)

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

            print(len(attempt_resources))
            n_attempts += 1
            break
            if (len(attempt_resources) >= 1000 or
                    len(agg_billing_project_resources) >= 1000 or
                    len(agg_batch_resources) >= 1000 or
                    len(agg_job_resources) >= 1000
            ):
                await write(attempt_resources, agg_billing_project_resources, agg_batch_resources, agg_job_resources)
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

        await write(attempt_resources, agg_billing_project_resources, agg_batch_resources, agg_job_resources)
        attempt_resources = []
        agg_billing_project_resources = []
        agg_batch_resources = []
        agg_job_resources = []

        print(f'finished inserting attempts and aggregated resources in {time.time() - start_time}s')
    finally:
        await db.async_close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
