# Copyright(c) 2024 The Rainway AI Gateway (壬远AI网关) Authors. All rights reserved.
# Copyright (c) 2019 The BFE Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# init epp version
#
# 构建产物布局（约定）：
#   output/bin/epp —— 可执行文件（本地构建）
#   output/conf/   —— 运行配置目录（由仓库 conf/ 拷贝而来，本地文件模式 --local-config-dir 用）
#   dist/          —— release 交叉编译的 tar.gz（包内同样为 bin/epp + conf/）
HOMEDIR   := $(shell pwd)
OUTDIR    := $(HOMEDIR)/output
BINDIR    := $(OUTDIR)/bin
CONFDIR   := $(OUTDIR)/conf
# 仓库跟踪的配置源目录（本地模式样例：epp_data_config.json / cluster_table.json / epp_pool.json）
CONFSRC   := $(HOMEDIR)/conf
DISTDIR   := $(HOMEDIR)/dist
EPP_VERSION ?= $(shell cat VERSION | sed 's/^v//')
GIT_COMMIT  ?= $(shell git rev-parse --short HEAD)

GOBUILD ?= go build
GOTEST  ?= go test


# -----------------------------------------------------------------------------
# Container image build variables (used by docker/docker-push targets)
# -----------------------------------------------------------------------------

# Image name and version
IMAGE_NAME ?= ai-gateway-epp
VERSION ?= $(shell cat VERSION | sed 's/^v//')
VERSION_TAG := v$(VERSION)

# Registry and build settings
REGISTRY ?=
PLATFORMS ?= linux/amd64,linux/arm64
BUILDER_NAME ?= ai-gateway-epp-builder
NO_CACHE ?= false
DOCKER_BUILD_ARGS ?=

# Derived tags
IMAGE_LOCAL := $(IMAGE_NAME):$(VERSION_TAG)
IMAGE_LATEST_LOCAL := $(IMAGE_NAME):latest
IMAGE_REMOTE := $(if $(REGISTRY),$(REGISTRY)/$(IMAGE_NAME):$(VERSION_TAG),)
IMAGE_LATEST_REMOTE := $(if $(REGISTRY),$(REGISTRY)/$(IMAGE_NAME):latest,)
# -----------------------------------------------------------------------------


.PHONY: all build test prepare release clean docker docker-push

all: prepare test build release

# 构建本地二进制（当前平台）→ output/bin/epp，并准备 output/conf
build: prepare
	$(GOBUILD) -ldflags "-X main.version=v$(EPP_VERSION) -X main.commit=$(GIT_COMMIT)" \
		-o $(BINDIR)/epp ./cmd/epp
	@echo "  -> $(BINDIR)/epp done (version v$(EPP_VERSION), commit $(GIT_COMMIT))"
	@echo "  -> run: ./output/bin/epp -epp-group <group> [-local-config-dir ./output/conf]"
	@echo "  -> prod: ./output/bin/epp -epp-group <group> -api-addr http://ai-gateway-api:8181/inner-api/v1"

test:
	$(GOTEST) ./...

# 准备输出目录：output/bin、output/conf（从仓库 conf/ 拷贝，覆盖旧内容，保证与源码一致）与 dist/
prepare:
	mkdir -p $(BINDIR) $(CONFDIR) $(DISTDIR)
	@if [ -d $(CONFSRC) ]; then \
		cp -R $(CONFSRC)/. $(CONFDIR)/; \
		echo "  -> $(CONFDIR) synced from ./conf"; \
	fi

# 交叉编译 + 打包 dist tarball，供 GitHub Release 上传。
# 包内布局与 output/ 一致：bin/epp + conf/
release: prepare
	@for arch in amd64 arm64; do \
		STAGE=$(DISTDIR)/tmp-epp_$(EPP_VERSION)_linux_$$arch; \
		rm -rf $$STAGE; \
		mkdir -p $$STAGE/bin $$STAGE/conf; \
		echo "  -> Building linux/$$arch..."; \
		GOOS=linux GOARCH=$$arch CGO_ENABLED=0 \
			$(GOBUILD) -a -installsuffix cgo \
			-ldflags "-X main.version=v$(EPP_VERSION) -X main.commit=$(GIT_COMMIT)" \
			-o $$STAGE/bin/epp ./cmd/epp; \
		cp -R $(CONFDIR)/. $$STAGE/conf/ 2>/dev/null || true; \
		[ -f LICENSE ]   && cp LICENSE   $$STAGE/ || true; \
		[ -f README.md ] && cp README.md $$STAGE/ || true; \
		(cd $$STAGE && tar czvf ../epp_$(EPP_VERSION)_linux_$$arch.tar.gz .); \
		rm -rf $$STAGE; \
		echo "  -> dist/epp_$(EPP_VERSION)_linux_$$arch.tar.gz done"; \
	done
	@echo "Release packages built successfully."

# 清理构建产物（output/、dist/ 与根目录残留二进制）；仓库 conf/ 为源码配置，保留
clean:
	rm -rf $(OUTDIR) $(DISTDIR)

# -----------------------------------------------------------------------------
# Docker 镜像
#
# 构建方式：镜像内多阶段构建（不依赖本地 output/），Dockerfile 分两段：
#   1) build 阶段（golang:<ver>-alpine）：COPY go.mod/go.sum → go mod download（层缓存）
#      → COPY . . → CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/epp ./cmd/epp
#      （静态二进制、剥离符号，便于放进 alpine 运行时镜像）
#   2) runtime 阶段（alpine）：安装 ca-certificates/tzdata → 建非 root 用户 app(10001)
#      → COPY --from=build /out/epp ./epp → COPY conf ./conf（本地模式样例配置）
#      → 建 log/ 并 chown → USER app → EXPOSE 9002/9003/9090 → ENTRYPOINT ["./epp"]
#
# 镜像创建时需要做的事（检查清单）：
#   - 版本/提交注入：镜像的版本信息当前由二进制内 main.version/commit 决定；如需镜像内可见，
#     应把 EPP_VERSION/GIT_COMMIT 作为 build-arg 传给 Dockerfile 的 go build -ldflags。
#   - 配置：容器默认走 InnerAPI 模式（-api-addr 指向 ai-gateway-api）；本地模式需挂载 conf 并传
#     `-local-config-dir /home/work/epp/conf/epp_data_conf`。EppGroup 必填：启动时必须传 `-epp-group` 或
#     设 `AI_GATEWAY_EPP_GROUP`，否则进程按 fail-fast 拒绝启动。
#   - 时区/证书：runtime 已装 tzdata 与 ca-certificates（TLS 访问 api 需要）。
#   - 端口：9002 ext-proc / 9003 gRPC health / 9090 metrics。
#   - 安全：非 root（app:10001）、工作目录 /home/work/epp、日志目录 log/ 可写。
#   - 多架构：docker-push 用 buildx + PLATFORMS（默认 linux/amd64,linux/arm64）。
#
# 产出物：
#   - docker build  → 本地镜像 tag：$(IMAGE_LOCAL)、$(IMAGE_LATEST_LOCAL)（需本机 docker）
#   - docker-push   → buildx 构建并推送：$(IMAGE_REMOTE)、$(IMAGE_LATEST_REMOTE)（需 REGISTRY=...）
#   - 不产出本地二进制；本地二进制/配置产物由 `make build` 生成到 output/（bin/epp + conf/）
#   - 若需“先本地构建再打镜像”，可用 IMAGE_FROM_LOCAL=1（见下方 target），跳过镜像内编译
# -----------------------------------------------------------------------------

# make docker（镜像内编译；本机无需 Go 环境）
docker:
	docker build $(if $(filter 1 true TRUE True,$(NO_CACHE)),--no-cache,) $(DOCKER_BUILD_ARGS) \
		--build-arg EPP_VERSION=$(EPP_VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		-t $(IMAGE_LOCAL) \
		-t $(IMAGE_LATEST_LOCAL) \
		.

# make docker-push (multi-arch)
docker-push:
	@test -n "$(REGISTRY)" || (echo "REGISTRY is required, e.g. REGISTRY=ghcr.io/your-org" && exit 1)
	@docker buildx inspect $(BUILDER_NAME) >/dev/null 2>&1 || docker buildx create --name $(BUILDER_NAME) --driver docker-container --use
	@docker buildx use $(BUILDER_NAME)
	@docker buildx inspect --bootstrap >/dev/null
	docker buildx build $(if $(filter 1 true TRUE True,$(NO_CACHE)),--no-cache,) $(DOCKER_BUILD_ARGS) \
		--platform $(PLATFORMS) \
		--build-arg EPP_VERSION=$(EPP_VERSION) \
		--build-arg GIT_COMMIT=$(GIT_COMMIT) \
		--push \
		-t $(IMAGE_REMOTE) \
		-t $(IMAGE_LATEST_REMOTE) \
		.
