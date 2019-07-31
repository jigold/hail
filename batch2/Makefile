.PHONY: build-agent push-agent \
  create-agent-instance-template delete-agent-instance-template \
  create-agent-instance-group delete-agent-instance-group \
  clean deploy-agents
# build
#  push push-test \
#  run-docker run \
#  test test-local deploy test-deploy \
#  check clean

PROJECT := $(shell gcloud config get-value project)

BATCH_LATEST = gcr.io/$(PROJECT)/batch:latest
BATCH_IMAGE = gcr.io/$(PROJECT)/batch:$(shell docker images -q --no-trunc batch | sed -e 's,[^:]*:,,')

BATCH_AGENT_LATEST = gcr.io/$(PROJECT)/batch-agent:latest
BATCH_AGENT_IMAGE = gcr.io/$(PROJECT)/batch-agent:$(shell docker images -q --no-trunc batch-agent | sed -e 's,[^:]*:,,')

BATCH_AGENT_TEMPLATE ?= batch2-agent-template-1
BATCH_AGENT_INSTANCE_GROUP ?= batch2-agents

build-agent:
	make -C ../docker build
	-docker pull $(BATCH_AGENT_LATEST)
	python3 ../ci/jinja2_render.py '{"base_image":{"image":"base"}}' Dockerfile.agent Dockerfile.agent.out
	docker build -t batch-agent -f Dockerfile.agent.out --cache-from batch-agent,$(BATCH_AGENT_LATEST),base ..

push-agent: build-agent
	docker tag batch-agent $(BATCH_AGENT_LATEST)
	docker push $(BATCH_AGENT_LATEST)
	docker tag batch-agent $(BATCH_AGENT_IMAGE)
	docker push $(BATCH_AGENT_IMAGE)
	echo $(BATCH_AGENT_IMAGE)
	echo $(BATCH_AGENT_LATEST)

# create-agent-instance-template: # push-agent
# 	gcloud compute instance-templates create-with-container $(BATCH_AGENT_TEMPLATE) \
# 	     --container-image $(BATCH_AGENT_LATEST) \
# 	     --machine-type n1-standard-1 \
# 	     --preemptible \
# 	     --tags batch2-agent \
# 	     --container-privileged \
# 	     --container-mount-host-path=host-path=/var/run/docker.sock,mount-path=/var/run/docker.sock

# create-agent-instance-group: create-agent-instance-template
# 	gcloud compute instance-groups managed create $(BATCH_AGENT_INSTANCE_GROUP) \
# 	    --base-instance-name batch2-agent \
# 	    --size 1 \
# 	    --template $(BATCH_AGENT_TEMPLATE)

# delete-agent-instance-template:
# 	gcloud compute instance-templates delete --quiet $(BATCH_AGENT_TEMPLATE)

# delete-agent-instance-group:
# 	gcloud compute instance-groups managed delete --quiet $(BATCH_AGENT_INSTANCE_GROUP)

# deploy-agents: create-agent-instance-group

deploy-vm:
	gcloud compute --project=hail-vdc instances create batch-agent-2 \
		--zone us-central1-a \
		--machine-type n1-standard-1 \
		--scopes default \
		--image ubuntu-1804-bionic-v20190722a \
		--image-project ubuntu-os-cloud \
		--boot-disk-size 10GB \
		--tags batch2-agent \
		--preemptible \
		--async \
		--metadata-from-file startup-script=scripts/vm-init.sh

deploy-agent:
	gcloud compute --project=hail-vdc instances create batch-agent-3 \
		--zone us-central1-a \
		--machine-type n1-standard-1 \
		--scopes default \
		--image batch-agent-1 \
		--boot-disk-size 10GB \
		--tags batch2-agent \
		--preemptible \
		--async

delete-vm:
	gcloud compute instances delete batch-agent \
		--quiet

# Create managed instance group
# clean: delete-agent-instance-group
# clean: delete-agent-instance-template

clean: delete-agent
