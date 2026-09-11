// Copyright(c) 2026 The Rainway AI Gateway (壬远AI网关) Authors.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.

/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
)

// healthServer gates readiness on the epp_data sync and cell readiness; the
// liveness probe is always serving. Mirrors the upstream atomic-flag pattern.
type healthServer struct {
	healthpb.UnimplementedHealthServer
	ready   atomic.Bool
	manager *cell.Manager
}

// setReady flips readiness once the epp_data watcher has synced and every
// assigned cell is ready.
func (h *healthServer) setReady(v bool) { h.ready.Store(v) }

func (h *healthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (resp *healthpb.HealthCheckResponse, err error) {
	if req.Service == "liveness" {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
	if h.ready.Load() {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING}, nil
}

func (h *healthServer) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "watch is not supported")
}
