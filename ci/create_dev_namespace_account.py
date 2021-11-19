import os
import base64
import json
import argparse
import kubernetes_asyncio as kube
from hailtop.utils import async_to_blocking
from gear import Database, transaction
from gear.cloud_config import get_global_config
from gear.clients import get_identity_client

from auth.driver.driver import create_user

CLOUD = get_global_config()['cloud']
SCOPE = os.environ['HAIL_SCOPE']
SOURCE_NAMESPACE = os.environ['HAIL_SOURCE_NAMESPACE']
TARGET_NAMESPACE = os.environ['HAIL_TARGET_NAMESPACE']


async def insert_user_if_not_exists(app, username, email, is_developer, is_service_account):
    db = app['db']
    k8s_client = app['k8s_client']

    @transaction(db)
    async def insert(tx):
        row = await tx.execute_and_fetchone('SELECT id, state FROM users where username = %s;', (username,))
        if row:
            if row['state'] == 'active':
                return None
            return row['id']

        hail_credentials_secret_name = f'{username}-gsa-key'

        secret = await k8s_client.read_namespaced_secret(hail_credentials_secret_name, SOURCE_NAMESPACE)

        await k8s_client.create_namespaced_secret(
            TARGET_NAMESPACE,
            kube.client.V1Secret(
                metadata=kube.client.V1ObjectMeta(name=hail_credentials_secret_name),
                data=secret.data,
            ))

        credentials_json = base64.b64decode(secret.data['key.json']).decode()
        credentials = json.loads(credentials_json)

        if CLOUD == 'gcp':
            hail_identity = credentials['client_email']
        else:
            assert CLOUD == 'azure'
            hail_identity = credentials['appId']

        namespace_name = None

        return await tx.execute_insertone(
            '''
    INSERT INTO users (state, username, email, is_developer, is_service_account, hail_identity, hail_credentials_secret_name, namespace_name)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    ''',
            (
                'creating',
                username,
                email,
                is_developer,
                is_service_account,
                hail_identity,
                hail_credentials_secret_name,
                namespace_name,
            ),
        )

    return await insert()


async def main():
    parser = argparse.ArgumentParser(description='Create developer user account in a dev namespace.')

    parser.add_argument('username', help='The username of the initial user.')
    parser.add_argument('email', help='The email of the initial user.')

    args = parser.parse_args()

    app = {}

    db = Database()
    await db.async_init(maxsize=50)
    app['db'] = db

    db_instance = Database()
    await db_instance.async_init(maxsize=50, config_file='/database-server-config/sql-config.json')
    app['db_instance'] = db_instance

    # kube.config.load_incluster_config()
    await kube.config.load_kube_config()
    k8s_client = kube.client.CoreV1Api()
    app['k8s_client'] = k8s_client

    app['identity_client'] = get_identity_client(credentials_file='/auth-gsa-key/key.json')

    user_id = await insert_user_if_not_exists(app, args.username, args.email, True, False)

    if user_id is not None:
        db_user = await db.execute_and_fetchone('SELECT * FROM users where id = %s;', (user_id,))
        await create_user(app, db_user, skip_trial_bp=True)


async_to_blocking(main())
