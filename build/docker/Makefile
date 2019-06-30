
.NOTPARALLEL:

ROOT=../..

ifeq ($(VERSION),)
VERSION:=$(patsubst v%,%,$(shell git describe --tags `git rev-list --tags --max-count=1`))
endif
ifeq ($(VERSION),)
$(error Cannot determine version. Aborting.)
endif

OSNICK ?= stretch

STEM=redisfab/redistimeseries
BUILD=docker build --rm --squash
PUSH=docker push

define build_x64
docker build $(BUILD_OPT) -t $(STEM)-x64-$(OSNICK):$(VERSION) -f $(ROOT)/Dockerfile $(ROOT)
docker tag $(STEM)-$(1)-$(OSNICK):$(VERSION) $(STEM)-$(1)-$(OSNICK):latest $(ROOT)
endef

define build_arm # (1=arch)
docker build $(BUILD_OPT) -t $(STEM)-$(1)-$(OSNICK):$(VERSION) -f $(ROOT)/Dockerfile.arm --build-arg ARCH=$(1) $(ROOT)
docker tag $(STEM)-$(1)-$(OSNICK):$(VERSION) $(STEM)-$(1)-$(OSNICK):latest $(ROOT)
endef

define push
docker push $(STEM)-$(1)-$(OSNICK):$(VERSION)
docker push $(STEM)-$(1)-$(OSNICK):latest
endef

.PHONY: build public

build:
	$(call build_x64)
	$(call build_arm,arm32v7)
	$(call build_arm,arm64v8)

publish:
	$(call push,x64)
	$(call push,arm32v7)
	$(call push,arm64v8)
	docker manifest create -a $(STEM)-$(OSNICK):$(VERSION) \
		-a $(STEM)-x64-$(OSNICK):$(VERSION) \
		-a $(STEM)-x64-$(OSNICK):$(VERSION) \
		-a $(STEM)-arm32v7-$(OSNICK):$(VERSION)
	docker manifest push -p $(STEM)-$(OSNICK):$(VERSION)
