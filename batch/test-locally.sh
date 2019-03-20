#!/bin/bash
# do not execute this file, use the Makefile

set -ex

PYTEST_ARGS=${PYTEST_ARGS:- -v --failed-first}

cleanup() {
    set +e
    trap "" INT TERM
    [[ -z $server_pid ]] || kill -9 $server_pid

    python3 -c "from batch.cloud_sql_helpers import *; drop_table(\"$JOBS_TABLE\",\"$JOBS_PARENTS_TABLE\")"    
    [[ -z $proxy_pid ]] || kill -9 $proxy_pid
}
trap cleanup EXIT
trap "exit 24" INT TERM

if [[ $CLOUD_SQL_PROXY -eq 1 ]]; then
    export CLOUD_SQL_CONFIG_PATH=`pwd`/batch-secrets/batch-test-cloud-sql-config.json
    connection_name=$(jq -r '.connection_name' $CLOUD_SQL_CONFIG_PATH)
    host=$(jq -r '.host' $CLOUD_SQL_CONFIG_PATH)
    port=$(jq -r '.port' $CLOUD_SQL_CONFIG_PATH)
    ./cloud_sql_proxy -instances=$connection_name=tcp:$port &
    proxy_pid=$!
    ../until-with-fuel 30 curl -fL $host:$port
else
    export CLOUD_SQL_CONFIG_PATH=/batch-secrets/batch-test-cloud-sql-config.json
fi

export JOBS_TABLE=$(python3 -c 'from batch.cloud_sql_helpers import *; print(get_temp_table("jobs"))')
export JOBS_PARENTS_TABLE=$(python3 -c 'from batch.cloud_sql_helpers import *; print(get_temp_table("jobs-parents"))')

python3 -c 'import batch.server; batch.server.serve(5000)' &
server_pid=$!

../until-with-fuel 30 curl -fL 127.0.0.1:5000/jobs

POD_IP='127.0.0.1' BATCH_URL='http://127.0.0.1:5000' python3 -m pytest ${PYTEST_ARGS} test
