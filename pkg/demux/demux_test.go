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

package demux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
)

func newTestManager(t *testing.T, compile cell.CompileFunc) *cell.Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	m := cell.NewManager(ctx, cell.Options{
		Logger:                   logr.Discard(),
		DrainTimeout:             50 * time.Millisecond,
		DrainWorkers:             1,
		Compile:                  compile,
		AllowExperimentalPlugins: true,
	})
	t.Cleanup(cancel)
	return m
}

func stubCompile(version string, err error) cell.CompileFunc {
	return func(_ context.Context, key cell.Key, raw json.RawMessage, c *cell.Cell) (*cell.Engine, error) {
		if err != nil {
			return nil, err
		}
		return &cell.Engine{Version: version + string(key)}, nil
	}
}

func TestManagerRouterRoute(t *testing.T) {
	m := newTestManager(t, stubCompile("v", nil))
	if _, err := m.Ensure(context.Background(), "cluster-a", cell.RolePrimary); err != nil {
		t.Fatal(err)
	}

	r := ManagerRouter{Manager: m}

	c, err := r.Route("cluster-a")
	if err != nil {
		t.Fatal(err)
	}
	if c == nil || c.Key != "cluster-a" {
		t.Fatalf("unexpected cell: %+v", c)
	}

	if _, err := r.Route("cluster-a"); !errors.Is(err, nil) {
		t.Fatalf("err=%v", err)
	}

	// Unknown pool maps to ErrUnknownPool.
	if _, err := r.Route("ghost"); !errors.Is(err, ErrUnknownPool) {
		t.Fatalf("err=%v", err)
	}
}

func TestToStatus(t *testing.T) {
	other := errors.New("boom")
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"nil", nil, codes.OK},
		{"no-pool", ErrNoPoolMetadata, codes.Internal},
		{"no-pool-wrapped", fmt.Errorf("recv: %w", ErrNoPoolMetadata), codes.Internal},
		{"unknown-pool", ErrUnknownPool, codes.Internal},
		{"unknown-pool-wrapped", fmt.Errorf("route: %w", ErrUnknownPool), codes.Internal},
		{"cell-draining", ErrCellDraining, codes.Unavailable},
		{"cell-draining-wrapped", fmt.Errorf("track: %w", ErrCellDraining), codes.Unavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toStatus(tt.err)
			if code := status.Code(got); code != tt.code {
				t.Fatalf("toStatus(%v) = %v, want code %v", tt.err, got, tt.code)
			}
		})
	}

	// Unrelated errors pass through unchanged.
	if got := toStatus(other); got != other {
		t.Fatalf("toStatus(%v) = %v, want the original error", other, got)
	}
	// The wire message preserves the sentinel text.
	if msg := status.Convert(toStatus(ErrCellDraining)).Message(); msg != ErrCellDraining.Error() {
		t.Fatalf("toStatus(ErrCellDraining) message = %q, want %q", msg, ErrCellDraining.Error())
	}
}
