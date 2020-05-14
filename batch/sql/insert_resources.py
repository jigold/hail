import asyncio

from gear import Database
# from hailtop.utils import (
#     rate_gib_hour_to_mib_msec,
#     rate_gib_month_to_mib_msec,
#     rate_cpu_hour_to_mcpu_msec,
#     rate_instance_hour_to_fraction_msec
# )


def rate_cpu_hour_to_mcpu_msec(rate_cpu_hour):
    rate_cpu_sec = rate_cpu_hour / 3600
    return rate_cpu_sec * 0.001 * 0.001


def rate_gib_hour_to_mib_msec(rate_gib_hour):
    rate_mib_hour = rate_gib_hour / 1024
    rate_mib_sec = rate_mib_hour / 3600
    return rate_mib_sec * 0.001


def rate_gib_month_to_mib_msec(rate_gib_month):
    # average number of days per month = 365.25 / 12 = 30.4375
    avg_n_days_per_month = 30.4375
    rate_gib_hour = rate_gib_month / avg_n_days_per_month / 24
    return rate_gib_hour_to_mib_msec(rate_gib_hour)


def rate_instance_hour_to_fraction_msec(rate_instance_hour, base):
    rate_instance_sec = rate_instance_hour / 3600
    rate_fraction_sec = rate_instance_sec / base
    return rate_fraction_sec * 0.001


async def main():
    # https://cloud.google.com/compute/all-pricing
    rates = [
        ('compute/n1-standard-preemptible/1', rate_cpu_hour_to_mcpu_msec(0.006655)),
        ('memory/n1-standard-preemptible/1', rate_gib_hour_to_mib_msec(0.000892)),
        ('boot-disk/pd-ssd/1', rate_gib_month_to_mib_msec(0.17)),
        ('ip-fee/1024/1', rate_instance_hour_to_fraction_msec(0.004, 1024)),
        ('service-fee/1', rate_cpu_hour_to_mcpu_msec(0.01))
    ]

    db = Database()
    await db.async_init()

    await db.execute_many('''
INSERT INTO `resources` (resource, rate)
VALUES (%s, %s)
''',
                          rates)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())