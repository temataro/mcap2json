CONTAINER_NAME:=mcap2json
CONTAINER_OWNER:=meriac
CONTAINER_REGISTRY:=ghcr.io
CONTAINER_PREFIX:=$(CONTAINER_REGISTRY)/$(CONTAINER_OWNER)/$(CONTAINER_NAME)

# determine container architecture
CONTAINER_ARCH:=$(shell uname -m)
ifeq ("$(CONTAINER_ARCH)","x86_64")
	CONTAINER_ARCH:=amd64
endif

CONTAINER:=$(CONTAINER_PREFIX):$(CONTAINER_ARCH)
CONTAINER_PLATFORM:=linux/$(CONTAINER_ARCH)

.PHONY: build rebuild run login publish manifest status version clean

build:
	@echo "Building container '$(CONTAINER)'..."
	CONTAINER=$(CONTAINER) podman compose build

rebuild:
	@echo "Rebuilding container '$(CONTAINER)'..."
	CONTAINER=$(CONTAINER) podman compose build --no-cache

run:
	podman compose run --rm $(CONTAINER)

login:
	@echo $(CR_PAT) | podman login $(CONTAINER_REGISTRY) -u DOESNT_MATTER --password-stdin

publish: status
	podman push --compress --compression-level 9 $(CONTAINER)

manifest:
	podman rmi -f $(CONTAINER_PREFIX):latest
	podman manifest create $(CONTAINER_PREFIX):latest $(CONTAINER_PREFIX):amd64 $(CONTAINER_PREFIX):arm64
	podman push --compress --compression-level 9 $(CONTAINER_PREFIX):latest
	podman rmi -f $(CONTAINER_PREFIX):latest

status:
	podman image tree $(CONTAINER)

version:
	@echo $(CONTAINER)

clean:
	podman rmi -f $(CONTAINER)
