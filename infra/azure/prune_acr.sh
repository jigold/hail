#!/bin/bash

set -ex

REGISTRY=$1
RESOURCE_GROUP=$2

if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Usage: sh prune_acr.sh <REGISTRY> <RESOURCE_GROUP>"
  exit 1
fi

# Environment variable for container command line
# Notice the --dry-run is on
PURGE_CMD="acr purge \
  --filter 'auth:.*' \
  --filter 'ci-intermediate:.*' \
  --filter 'ci-utils:.*' \
  --filter 'base:.*' \
  --filter 'base_spark_3_2:.*' \
  --filter 'batch:.*' \
  --filter 'batch-driver-nginx:.*' \
  --filter 'batch-worker:.*' \
  --filter 'benchmark:.*' \
  --filter 'blog_nginx:.*' \
  --filter 'ci:.*' \
  --filter 'ci-utils:.*' \
  --filter 'create_certs_image:.*' \
  --filter 'echo:.*' \
  --filter 'grafana:.*' \
  --filter 'hail-base:.*' \
  --filter 'hail-build:.*' \
  --filter 'hail-buildkit:.*' \
  --filter 'hail-run:.*' \
  --filter 'hail-run-tests:.*' \
  --filter 'hail-pip-installed-python37:.*' \
  --filter 'hail-pip-installed-python38:.*' \
  --filter 'hail-ubuntu:.*' \
  --filter 'memory:.*' \
  --filter 'monitoring:.*' \
  --filter 'notebook:.*' \
  --filter 'notebook_nginx:.*' \
  --filter 'prometheus:.*' \
  --filter 'service-base:.*' \
  --filter 'service-java-run-base:.*' \
  --filter 'test-ci:.*' \
  --filter 'test-monitoring:.*' \
  --filter 'test-benchmark:.*' \
  --filter 'website:.*' \
  --untagged \
  --ago '7d' \
  --keep 10"


# Run for max 8 hour timeout
# Task will run remotely as a task in Azure's Container Registry
az acr run \
  --timeout 28800 \
  --cmd "$PURGE_CMD" \
  --registry $REGISTRY \
  --resource-group $RESOURCE_GROUP \
  /dev/null
