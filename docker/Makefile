include ../config.mk

.PHONY: build
build: base service-base hail-ubuntu

HAIL_UBUNTU_IMAGE := $(DOCKER_PREFIX)/hail-ubuntu:$(TOKEN)
BASE_IMAGE := $(DOCKER_PREFIX)/base:$(TOKEN)
SERVICE_BASE_IMAGE := $(DOCKER_PREFIX)/service-base:$(TOKEN)
HAIL_PUBLIC_IMAGE := $(DOCKER_PREFIX)/hail-public:$(TOKEN)
GENETICS_PUBLIC_IMAGE := $(DOCKER_PREFIX)/genetics-public:$(TOKEN)
VEP_GRCH37_IMAGE := $(DOCKER_PREFIX)/vep-grch37:$(TOKEN)
VEP_GRCH38_IMAGE := $(DOCKER_PREFIX)/vep-grch38:$(TOKEN)

.PHONY: hail-ubuntu hail-ubuntu-image-ref
hail-ubuntu: hail-ubuntu-image-ref
hail-ubuntu-image-ref: hail-ubuntu/Dockerfile hail-ubuntu/hail-pip-install hail-ubuntu/pip.conf hail-ubuntu/hail-apt-get-install
	python3 ../ci/jinja2_render.py '{"global":{"docker_prefix":"$(DOCKER_PREFIX)"}}' hail-ubuntu/Dockerfile hail-ubuntu/Dockerfile.out
	../docker-build.sh hail-ubuntu Dockerfile.out $(HAIL_UBUNTU_IMAGE)
	echo $(HAIL_UBUNTU_IMAGE) > $@

.PHONY: base base-image-ref
base: base-image-ref
base-image-ref: hail-ubuntu-image-ref Dockerfile.base core-site.xml requirements.txt ../pylintrc ../setup.cfg
	python3 ../ci/jinja2_render.py '{"hail_ubuntu_image":{"image":"'$$(cat hail-ubuntu-image-ref)'"}}' Dockerfile.base Dockerfile.base.out
	../docker-build.sh .. docker/Dockerfile.base.out $(BASE_IMAGE)
	echo $(BASE_IMAGE) > $@

.PHONY: hail_version
hail_version:
	$(MAKE) -C ../hail python/hailtop/hail_version
	cp ../hail/python/hailtop/hail_version ../hail_version

.PHONY: service-base service-base-image-ref
service-base: service-base-image-ref
service-base-image-ref: base hail_version
	python3 ../ci/jinja2_render.py '{"base_image":{"image":"'$$(cat base-image-ref)'"}}' Dockerfile.service-base Dockerfile.service-base.out
	../docker-build.sh .. docker/Dockerfile.service-base.out $(SERVICE_BASE_IMAGE)
	echo $(SERVICE_BASE_IMAGE) > $@

.PHONY: vep-grch37
vep-grch37:
	../docker-build.sh vep Dockerfile.GRCh37 $(VEP_GRCH37_IMAGE) "konradjk/vep85_loftee:1.0.3"

.PHONY: vep-grch38
vep-grch38:
	../docker-build.sh vep Dockerfile.GRCh38 $(VEP_GRCH38_IMAGE) "konradjk/vep95_loftee:0.2"


.PHONY: deploy
deploy: build

.PHONY: generate-pip-lockfile
generate-pip-lockfile:
	# Some service dependencies are linux-specific and the lockfile
	# would differ when generated on MacOS, so we generate the lockfile
	# on a linux image.
	docker run --rm -it \
		-v $(HAIL):/hail \
		python:3.7-slim \
		/bin/bash -c "pip install pip-tools && cd hail/docker && pip-compile --upgrade requirements.txt --output-file=linux-pinned-requirements.txt"

.PHONY: clean
clean:
	rm -f base hail-ubuntu-image-ref base service-base
