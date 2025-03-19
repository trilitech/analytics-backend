.PHONY: default build release image clean
THIS_FILE := $(lastword $(MAKEFILE_LIST))

BUILD_ARTIFACT := analytics-backend
BUILD_TAG ?= main
BUILD_REPO := github.com/trilitech/$(BUILD_ARTIFACT)
BUILD_VERSION ?= $(shell git describe ${BUILD_TAG} --always --tags)
BUILD_COMMIT := $(shell git rev-parse --short ${BUILD_TAG})
BUILD_DATE := $(shell date -u "+%Y-%m-%dT%H:%M:%SZ")
BUILD_ID ?= $(shell uuidgen)

TARGET_IMAGE := $(BUILD_ARTIFACT):$(BUILD_VERSION)

default: image-amd64

image-amd64:
	@echo $@
	@echo "Building $(TARGET_IMAGE)"
	docker buildx build --platform linux/amd64 --ssh default --pull --force-rm --build-arg BUILD_REPO=$(BUILD_REPO) --build-arg BUILD_TAG=$(BUILD_TAG) --build-arg BUILD_ARTIFACT=$(BUILD_ARTIFACT) --build-arg BUILD_DATE=$(BUILD_DATE) --build-arg BUILD_VERSION=$(BUILD_VERSION) --build-arg BUILD_COMMIT=$(BUILD_COMMIT) --build-arg BUILD_ID=$(BUILD_ID) -t $(TARGET_IMAGE) -f Dockerfile .
	@echo
	@echo "Container image complete. Continue with "
	@echo " List:         docker images"
	@echo " Push:         docker push $(TARGET_IMAGE)"
	@echo " Inspect:      docker inspect $(TARGET_IMAGE)"
	@echo " Run:          docker run --rm --name $(BUILD_ARTIFACT) $(TARGET_IMAGE)"
	@echo

release-gcp: image-amd64
	@echo $@
	@echo "Tagging image..."
	docker tag $(TARGET_IMAGE) europe-west2-docker.pkg.dev/tzstats-prod-app-4800/tzstats-backend/$(BUILD_ARTIFACT):$(BUILD_VERSION)
	@echo "Publishing image..."
	docker push europe-west2-docker.pkg.dev/tzstats-prod-app-4800/tzstats-backend/$(BUILD_ARTIFACT):$(BUILD_VERSION)

clean:
	@echo $@
	rm -rf build/*
