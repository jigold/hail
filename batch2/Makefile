.PHONY: build-agent
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

# build docker images
#build:
#	make -C ../docker build
#	-docker pull $(BATCH_LATEST)
#	python3 ../ci/jinja2_render.py '{"base_image":{"image":"base"}}' Dockerfile Dockerfile.out
#	docker build -t batch -f Dockerfile.out --cache-from batch,$(BATCH_LATEST),base ..

build-agent:
	make -C ../docker build
	-docker pull $(BATCH_AGENT_LATEST)
	python3 ../ci/jinja2_render.py '{"base_image":{"image":"base"}}' Dockerfile.agent Dockerfile.agent.out
	docker build -t batch-agent -f Dockerfile.agent.out ..

push-agent: build-agent
	docker tag batch-agent $(BATCH_AGENT_LATEST)
	docker push $(BATCH_AGENT_LATEST)
	docker tag batch-agent $(BATCH_AGENT_IMAGE)
	docker push $(BATCH_AGENT_IMAGE)

# Create instance template

# Create managed instance group

