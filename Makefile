# init epp version
EPP_VERSION ?= $(shell cat VERSION)
GIT_COMMIT ?= $(shell git rev-parse --short HEAD)

GOBUILD ?= go build
GOTEST  ?= go test

.PHONY: all build test

all: build

build:
	$(GOBUILD) -ldflags "-X main.version=$(EPP_VERSION) -X main.commit=$(GIT_COMMIT)" -o epp ./cmd/epp

test:
	$(GOTEST) ./...
