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

package main

import (
	"context"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func TestHealthCheck(t *testing.T) {
	cases := []struct {
		name    string
		ready   bool
		service string
		want    healthpb.HealthCheckResponse_ServingStatus
	}{
		// Liveness must always answer SERVING, even before readiness flips.
		{"liveness while not ready", false, "liveness", healthpb.HealthCheckResponse_SERVING},
		{"liveness while ready", true, "liveness", healthpb.HealthCheckResponse_SERVING},
		// Readiness (and anything that is not the liveness service) follows
		// the ready flag.
		{"readiness while not ready", false, "readiness", healthpb.HealthCheckResponse_NOT_SERVING},
		{"readiness while ready", true, "readiness", healthpb.HealthCheckResponse_SERVING},
		{"empty service while not ready", false, "", healthpb.HealthCheckResponse_NOT_SERVING},
		{"empty service while ready", true, "", healthpb.HealthCheckResponse_SERVING},
		{"other service while not ready", false, "grpc.health.v1.Health", healthpb.HealthCheckResponse_NOT_SERVING},
		{"other service while ready", true, "grpc.health.v1.Health", healthpb.HealthCheckResponse_SERVING},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &healthServer{}
			h.setReady(tc.ready)
			resp, err := h.Check(context.Background(), &healthpb.HealthCheckRequest{Service: tc.service})
			if err != nil {
				t.Fatalf("Check: %v", err)
			}
			if resp.GetStatus() != tc.want {
				t.Errorf("status = %v, want %v", resp.GetStatus(), tc.want)
			}
		})
	}
}

func TestHealthReadyToggle(t *testing.T) {
	h := &healthServer{}
	req := &healthpb.HealthCheckRequest{Service: "readiness"}

	resp, err := h.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("initial status = %v, want NOT_SERVING", resp.GetStatus())
	}

	h.setReady(true)
	resp, err = h.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status after setReady(true) = %v, want SERVING", resp.GetStatus())
	}

	// Readiness can be revoked again (e.g. on later sync failures).
	h.setReady(false)
	resp, err = h.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("status after setReady(false) = %v, want NOT_SERVING", resp.GetStatus())
	}
}

func TestHealthWatchUnsupported(t *testing.T) {
	h := &healthServer{}
	// Watch answers Unimplemented without touching the stream, so a nil
	// stream is a valid probe.
	err := h.Watch(&healthpb.HealthCheckRequest{Service: "liveness"}, nil)
	if err == nil {
		t.Fatal("expected error from Watch")
	}
	if got := status.Code(err); got != codes.Unimplemented {
		t.Errorf("status.Code = %v, want %v", got, codes.Unimplemented)
	}
}

// TestHealthServerConcurrent exercises the atomic readiness flag from many
// goroutines; run with -race to catch regressions in the synchronization.
func TestHealthServerConcurrent(t *testing.T) {
	h := &healthServer{}
	const workers = 8
	const iterations = 200

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				h.setReady((i+j)%2 == 0)
				req := &healthpb.HealthCheckRequest{}
				if j%2 == 0 {
					req.Service = "liveness"
				}
				if _, err := h.Check(context.Background(), req); err != nil {
					t.Errorf("Check: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
