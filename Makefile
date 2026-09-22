# init epp version
DISTDIR   := $(shell pwd)/dist
HOMEDIR   := $(shell pwd)
EPP_VERSION ?= $(shell cat VERSION | sed 's/^v//')
GIT_COMMIT  ?= $(shell git rev-parse --short HEAD)

GOBUILD ?= go build
GOTEST  ?= go test

.PHONY: all build test prepare release clean

all: build

# 构建本地二进制（当前平台）
build:
	$(GOBUILD) -ldflags "-X main.version=v$(EPP_VERSION) -X main.commit=$(GIT_COMMIT)" \
		-o ./bin/epp ./cmd/epp

test:
	$(GOTEST) ./...

prepare:
	mkdir -p $(DISTDIR)

# 交叉编译 + 打包 dist tarball，供 GitHub Release 上传
release: prepare
	@for arch in amd64 arm64; do \
		STAGE=$(DISTDIR)/tmp-epp_$(EPP_VERSION)_linux_$$arch; \
		rm -rf $$STAGE; \
		mkdir -p $$STAGE; \
		echo "  -> Building linux/$$arch..."; \
		GOOS=linux GOARCH=$$arch CGO_ENABLED=0 \
			$(GOBUILD) -a -installsuffix cgo \
			-ldflags "-X main.version=v$(EPP_VERSION) -X main.commit=$(GIT_COMMIT)" \
			-o $$STAGE/epp ./cmd/epp; \
		[ -f LICENSE ]   && cp LICENSE   $$STAGE/ || true; \
		[ -f README.md ] && cp README.md $$STAGE/ || true; \
		(cd $$STAGE && tar czvf ../epp_$(EPP_VERSION)_linux_$$arch.tar.gz .); \
		rm -rf $$STAGE; \
		echo "  -> dist/epp_$(EPP_VERSION)_linux_$$arch.tar.gz done"; \
	done
	@echo "Release packages built successfully."

clean:
	rm -rf $(DISTDIR) epp
