.PHONY: build push deploy

PROJECT = $(shell gcloud config get-value project)
GATEWAY_LATEST = gcr.io/$(PROJECT)/gateway:latest
GATEWAY_IMAGE = gcr.io/$(PROJECT)/gateway:$(shell docker images -q --no-trunc gateway | sed -e 's,[^:]*:,,')
DOMAIN ?= hail.is
IP ?= 35.224.105.117
INTERNAL_IP ?= 10.128.0.57
POD_CIDR ?= 10.32.0.0/14

build:
	docker pull ubuntu:18.04
	-docker pull $(GATEWAY_LATEST)
	docker build -t gateway --cache-from gateway,$(GATEWAY_LATEST),ubuntu:18.04 .

push: build
	docker tag gateway $(GATEWAY_LATEST)
	docker push $(GATEWAY_LATEST)
	docker tag gateway $(GATEWAY_IMAGE)
	docker push $(GATEWAY_IMAGE)

deploy: push
	python3 ../ci/jinja2_render.py '{"code":{"sha":"$(shell git rev-parse --short=12 HEAD)"},"deploy":true,"global":{"ip":"$(IP)", "internal_ip":"$(INTERNAL_IP)", "pod_cidr":"$(POD_CIDR)"}}' service.yaml service.yaml.out
	kubectl -n jigold apply -f service.yaml.out
	python3 ../ci/jinja2_render.py '{"code":{"sha":"$(shell git rev-parse --short=12 HEAD)"},"deploy":true,"gateway_image":{"image":"$(GATEWAY_IMAGE)"}}' deployment.yaml deployment.yaml.out
	kubectl -n jigold apply -f deployment.yaml.out
