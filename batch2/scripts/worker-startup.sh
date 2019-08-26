#!/bin/bash
set -ex

gsutil -m cp gs://hail-common/dev2/batch2/run-worker.sh /

nohup /bin/bash run-worker.sh >run-worker.log 2>&1 &
