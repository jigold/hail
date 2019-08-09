#!/bin/bash
set -ex

kubectl -n default get --selector=app=batch deployments -o "jsonpath={.items[*].metadata.labels.hail\.is/sha}"
