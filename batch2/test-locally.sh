#!/bin/bash
# do not execute this file, use the Makefile

set -ex

PYTEST_ARGS=${PYTEST_ARGS:- -v --failed-first}

cleanup() {
    set +e
    trap "" INT TERM

    for table in ${tables[@]}; do
        python3 -c "from batch.database import Database; db = Database.create_synchronous(\"$CLOUD_SQL_CONFIG_PATH\"); db.drop_table_sync(\"$table\")"
    done

    [[ -z $server_pid ]] || kill -9 $server_pid
    [[ -z $proxy_pid ]] || kill -9 $proxy_pid
}
trap cleanup EXIT
trap "exit 24" INT TERM

if [[ -z $IN_HAIL_CI ]]; then
    export CLOUD_SQL_CONFIG_PATH=`pwd`/batch-secrets/batch-test-cloud-sql-config.json
    export BATCH_GSA_KEY=`pwd`/batch-secrets/batch-test-gsa-key/privateKeyData
    export BATCH_JWT=`pwd`/batch-secrets/batch-test-jwt/jwt

    connection_name=$(jq -r '.connection_name' $CLOUD_SQL_CONFIG_PATH)
    host=$(jq -r '.host' $CLOUD_SQL_CONFIG_PATH)
    port=$(jq -r '.port' $CLOUD_SQL_CONFIG_PATH)
    ./cloud_sql_proxy -instances=$connection_name=tcp:$port &
    proxy_pid=$!
    ../until-with-fuel 30 curl -fL $host:$port
else
    export CLOUD_SQL_CONFIG_PATH=/batch-secrets/batch-test-cloud-sql-config.json
    export BATCH_GSA_KEY=/batch-test-gsa-key/privateKeyData
    export BATCH_JWT=/batch-test-jwt/jwt
fi

export JOBS_TABLE=jobs-$(../generate-uid.sh)
export JOBS_PARENTS_TABLE=jobs-parents-$(../generate-uid.sh)
export BATCH_TABLE=batch-$(../generate-uid.sh)
tables=($JOBS_TABLE $JOBS_PARENTS_TABLE $BATCH_TABLE)

python3 -c 'import batch.server; batch.server.serve(5000)' &
server_pid=$!

../until-with-fuel 30 curl -fL 127.0.0.1:5000/healthcheck

POD_IP='127.0.0.1' BATCH_URL='http://127.0.0.1:5000' python3 -m pytest ${PYTEST_ARGS} test
