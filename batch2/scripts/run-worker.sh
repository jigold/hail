#!/bin/bash
set -x

export CORES=$(nproc)
export DRIVER_BASE_URL=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/driver_base_url")
export BATCH_IMAGE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_image")
export BATCH_INSTANCE=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/batch_instance")
export INST_TOKEN=$(curl -s -H "Metadata-Flavor: Google" "http://metadata.google.internal/computeMetadata/v1/instance/attributes/inst_token")

export HOME=/root
docker run -v /var/run/docker.sock:/var/run/docker.sock -p 5000:5000 -d --entrypoint "/bin/bash" $BATCH_IMAGE -c "python3 -u -m 'batch.agent'"

#gsutil -m cp worker.log run-worker.log $SCRATCH/worker_logs/$INST_TOKEN/

export NAME=$(curl http://metadata.google.internal/computeMetadata/v1/instance/name -H 'Metadata-Flavor: Google')
export ZONE=$(curl http://metadata.google.internal/computeMetadata/v1/instance/zone -H 'Metadata-Flavor: Google')
gcloud -q compute instances delete $NAME --zone=$ZONE
