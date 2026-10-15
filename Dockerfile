# Copyright(c) 2024 The Rainway AI Gateway (壬远AI网关) Authors. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM --platform=${BUILDPLATFORM} golang:1.26-alpine AS build

ARG TARGETARCH
ARG TARGETOS
ARG ALPINE_MIRROR=
# 版本信息注入（由 Makefile 通过 --build-arg 传入；缺省时镜像内二进制无版本号）
ARG EPP_VERSION=
ARG GIT_COMMIT=

WORKDIR /src

RUN set -ex; \
  if [ -n "${ALPINE_MIRROR}" ]; then \
    echo "${ALPINE_MIRROR}/v3.19/main" > /etc/apk/repositories; \
    echo "${ALPINE_MIRROR}/v3.19/community" >> /etc/apk/repositories; \
  fi; \
  apk add --no-cache git ca-certificates curl

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
ENV GOTOOLCHAIN=local
RUN set -ex; \
  GOOS=${TARGETOS:-linux} \
  GOARCH=${TARGETARCH:-$(go env GOARCH)} \
  go build -trimpath \
  -ldflags "-s -w -X main.version=v${EPP_VERSION} -X main.commit=${GIT_COMMIT}" \
  -o /out/epp ./cmd/epp

FROM alpine:3.19

ARG ALPINE_MIRROR=

RUN set -ex; \
  if [ -n "${ALPINE_MIRROR}" ]; then \
    echo "${ALPINE_MIRROR}/v3.19/main" > /etc/apk/repositories; \
    echo "${ALPINE_MIRROR}/v3.19/community" >> /etc/apk/repositories; \
  fi; \
  apk add --no-cache ca-certificates tzdata; \
  addgroup -S app; \
  adduser -S -G app -u 10001 app

WORKDIR /home/work/epp

COPY --from=build /out/epp ./epp
# 本地文件模式样例配置（epp_data_config.json / cluster_table.json / epp_pool.json）
COPY conf ./conf

RUN mkdir -p ./log \
  && chown -R app:app /home/work/epp

USER app

EXPOSE 9002 9003 9090

ENTRYPOINT ["./epp"]
