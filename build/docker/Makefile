
OSNICK ?= stretch

.PHONY: build public

build:
	docker build --rm --squash -t redisfab/arm32v7-xbuild:$(OSNICK) -f Dockerfile.arm32v7-$(OSNICK) .
	docker build --rm --squash -t redisfab/arm64v8-xbuild:$(OSNICK) -f Dockerfile.arm64v8-$(OSNICK) .

publish:
	docker push redisfab/arm32v7-xbuild:$(OSNICK)
	docker push redisfab/arm64v8-xbuild:$(OSNICK)
