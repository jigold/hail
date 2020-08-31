import datetime
import calendar
import asyncio
from aiohttp import web
import aiohttp_session
import logging
from collections import defaultdict

from hailtop import aiogoogle
from hailtop.aiogoogle import BigQueryClient
from hailtop.config import get_deploy_config
from hailtop.hail_logging import AccessLogger
from hailtop.tls import get_in_cluster_server_ssl_context
from hailtop.utils import run_if_changed_idempotent, retry_long_running, time_msecs, cost_str
from gear import (Database, setup_aiohttp_session,
                  web_authenticated_developers_only, rest_authenticated_developers_only,
                  transaction)
from web_common import (setup_aiohttp_jinja2, setup_common_static_routes,
                        render_template, set_message)

from .configuration import HAIL_USE_FULL_QUERY

log = logging.getLogger('monitoring')

routes = web.RouteTableDef()

deploy_config = get_deploy_config()


def get_previous_month(dt):
    if dt.month == 1:
        return datetime.datetime(dt.year - 1, 12, 1)
    return datetime.datetime(dt.year, dt.month - 1, 1)


def get_last_day_month(dt):
    _, last_day = calendar.monthrange(dt.year, dt.month)
    return datetime.datetime(dt.year, dt.month, last_day)


def format_data(records):
    cost_by_service = defaultdict(lambda: 0)
    compute_cost_breakdown = defaultdict(lambda: 0)
    cost_by_sku_source = []

    for record in records:
        cost_by_sku_source.append(record)

        cost_by_service[record['service_description']] += record['cost']

        # service.id: service.description -- "6F81-5844-456A": "Compute Engine"
        if record['service_id'] == '6F81-5844-456A':
            assert record['source'] is not None
            compute_cost_breakdown[record['source']] += record['cost']
        else:
            assert record['source'] is None

    cost_by_service = sorted([{'service': k, 'cost': cost_str(v)} for k, v in cost_by_service.items()],
                             key=lambda x: x['cost'],
                             reverse=True)

    compute_cost_breakdown = sorted([{'source': k, 'cost': cost_str(v)} for k, v in compute_cost_breakdown.items()],
                                    key=lambda x: x['cost'],
                                    reverse=True)

    cost_by_sku_source.sort(key=lambda x: x['cost'],
                            reverse=True)
    for record in cost_by_sku_source:
        record['cost'] = cost_str(record['cost'])

    return (cost_by_service, compute_cost_breakdown, cost_by_sku_source)


async def _billing(request):
    app = request.app
    date_format = '%m/%Y'

    now = datetime.datetime.now()
    default_time_period = now.strftime(date_format)

    time_period_query = request.query.get('time_period', default_time_period)

    try:
        time_period = datetime.datetime.strptime(time_period_query, date_format)
    except ValueError:
        msg = f"Invalid value for time_period '{time_period_query}'; must be in the format of MM/YYYY."
        session = await aiohttp_session.get_session(request)
        set_message(session, msg, 'error')
        return ([], [], [], time_period_query)

    db = app['db']
    records = db.execute_and_fetchall('SELECT * FROM monitoring_billing_data WHERE year = %s AND month = %s;',
                                      (time_period.year, time_period.month))
    records = [record async for record in records]

    cost_by_service, compute_cost_breakdown, cost_by_sku_source = format_data(records)

    return (cost_by_service, compute_cost_breakdown, cost_by_sku_source, time_period_query)


@routes.get('/api/v1alpha/billing')
@rest_authenticated_developers_only
async def get_billing(request: web.Request, userdata) -> web.Response:  # pylint: disable=unused-argument
    cost_by_service, compute_cost_breakdown, cost_by_sku_label, time_period_query = await _billing(request)
    resp = {
        'cost_by_service': cost_by_service,
        'compute_cost_breakdown': compute_cost_breakdown,
        'cost_by_sku_label': cost_by_sku_label,
        'time_period_query': time_period_query
    }
    return web.json_response(resp)


@routes.get('/billing')
@web_authenticated_developers_only()
async def billing(request: web.Request, userdata) -> web.Response:  # pylint: disable=unused-argument
    cost_by_service, compute_cost_breakdown, cost_by_sku_label, time_period_query = await _billing(request)
    context = {
        'cost_by_service': cost_by_service,
        'compute_cost_breakdown': compute_cost_breakdown,
        'cost_by_sku_label': cost_by_sku_label,
        'time_period': time_period_query
    }
    return await render_template('monitoring', request, userdata, 'billing.html', context)


async def query_billing_body(app):
    db = app['db']
    bigquery_client = app['bigquery_client']

    async def _query(dt):
        month = dt.month
        year = dt.year

        start = datetime.date(year, month, 1)
        _, last_day_of_month = calendar.monthrange(year, month)

        if HAIL_USE_FULL_QUERY:
            end = datetime.date(year, month, last_day_of_month) + datetime.timedelta(days=7)
        else:
            end = start + datetime.timedelta(days=1)

        date_format = '%Y-%m-%d'
        start_str = datetime.date.strftime(start, date_format)
        end_str = datetime.date.strftime(end, date_format)

        invoice_month = datetime.date.strftime(start, '%Y%m')

        # service.id: service.description -- "6F81-5844-456A": "Compute Engine"
        cmd = f'''
SELECT service.id as service_id, service.description as service_description, sku.id as sku_id, sku.description as sku_description, SUM(cost) as cost,
CASE
  WHEN service.id = "6F81-5844-456A" AND EXISTS(SELECT 1 FROM UNNEST(labels) WHERE key = "namespace" and value = "default") THEN "batch-production"
  WHEN service.id = "6F81-5844-456A" AND EXISTS(SELECT 1 FROM UNNEST(labels) WHERE key = "namespace" and value LIKE '%pr-%') THEN "batch-test"
  WHEN service.id = "6F81-5844-456A" AND EXISTS(SELECT 1 FROM UNNEST(labels) WHERE key = "namespace") THEN "batch-dev"
  WHEN service.id = "6F81-5844-456A" AND EXISTS(SELECT 1 FROM UNNEST(labels) WHERE key = "role" and value LIKE 'vdc') THEN "k8s"
  WHEN service.id = "6F81-5844-456A" THEN "unknown"
  ELSE NULL
END AS source
FROM `broad-ctsa.hail_billing.gcp_billing_export_v1_0055E5_9CA197_B9B894`
WHERE DATE(_PARTITIONTIME) >= "{start_str}" AND DATE(_PARTITIONTIME) <= "{end_str}" AND project.name = "hail-vdc" AND invoice.month = "{invoice_month}"
GROUP BY service_id, service_description, sku_id, sku_description, source;
'''

        log.info(f'querying BigQuery with command: {cmd}')

        records = [(year, month, record['service_id'], record['service_description'], record['sku_id'], record['sku_description'], record['source'], record['cost'])
                   async for record in await bigquery_client.query(cmd)]

        @transaction(db)
        async def insert(tx):
            await tx.just_execute('''
DELETE FROM monitoring_billing_data WHERE year = %s AND month = %s;
''',
                                  (year, month))

            await tx.execute_many('''
INSERT INTO monitoring_billing_data (year, month, service_id, service_description, sku_id, sku_description, source, cost)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
''',
                                  records)

        await insert()  # pylint: disable=no-value-for-parameter

    log.info('updating billing information')
    now = datetime.datetime.now()
    await _query(now)
    last_month = get_previous_month(now)
    end_last_month = get_last_day_month(last_month)
    if now < end_last_month + datetime.timedelta(days=7):
        await _query(last_month)

    now_msecs = time_msecs()
    await db.execute_update('UPDATE monitoring_billing_mark SET mark = %s;', (now_msecs,))

    should_wait = True
    return should_wait


async def polling_loop(app):
    db = app['db']
    while True:
        now = datetime.datetime.now()
        row = await db.select_and_fetchone('SELECT mark FROM monitoring_billing_mark;')
        if not row['mark'] or now > datetime.datetime.fromtimestamp(row['mark'] / 1000) + datetime.timedelta(days=1):
            app['query_billing_event'].set()
        await asyncio.sleep(60)


async def on_startup(app):
    db = Database()
    await db.async_init()
    app['db'] = db

    aiogoogle_credentials = aiogoogle.Credentials.from_file('/billing-monitoring-gsa-key/key.json')
    bigquery_client = BigQueryClient('broad-ctsa', credentials=aiogoogle_credentials)
    app['bigquery_client'] = bigquery_client

    query_billing_event = asyncio.Event()
    app['query_billing_event'] = query_billing_event

    asyncio.ensure_future(retry_long_running(
        'polling_loop',
        polling_loop, app))

    asyncio.ensure_future(retry_long_running(
        'query_billing_loop',
        run_if_changed_idempotent, query_billing_event, query_billing_body, app))


def run():
    app = web.Application()
    setup_aiohttp_session(app)

    setup_aiohttp_jinja2(app, 'monitoring')
    setup_common_static_routes(routes)
    app.add_routes(routes)

    app.on_startup.append(on_startup)

    web.run_app(deploy_config.prefix_application(app, 'monitoring'),
                host='0.0.0.0',
                port=5000,
                access_log_class=AccessLogger,
                ssl_context=get_in_cluster_server_ssl_context())
