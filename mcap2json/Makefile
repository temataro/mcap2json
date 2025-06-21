CONTAINER:=mcap2json

.PHONY: run build clean version

build:
	podman compose build --no-cache

run:
	podman compose run --rm $(CONTAINER)

version:
	@echo $(CONTAINER)

clean:
	podman rmi -f $(CONTAINER)
