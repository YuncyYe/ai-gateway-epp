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

// Package demux routes each ext-proc stream to the cell selected by the
// inference-pool metadata injected by BFE, then delegates stream processing
// to that cell's engine's StreamingServer. Failures never block BFE: it falls
// back to local load balancing on any ext-proc error.
package demux

import (
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
)

const (
	// MetadataNamespace is the ext-proc dynamic metadata namespace BFE writes.
	MetadataNamespace = "llm-d.ai"
	// PoolMetadataKey carries the BFE cluster name inside MetadataNamespace.
	PoolMetadataKey = "inference-pool"
)

// Request-path errors. All map to INTERNAL on the wire; BFE logs and falls
// back to local balancing (or retries the standby for ErrCellDraining).
var (
	ErrNoPoolMetadata = errors.New("missing inference-pool metadata")
	ErrUnknownPool    = errors.New("unknown inference pool")
	ErrCellDraining   = errors.New("cell is not serving (draining or not ready)")
)

// Router resolves a pool name to a cell.
type Router interface {
	Route(pool string) (*cell.Cell, error)
}

// ManagerRouter adapts *cell.Manager to Router.
type ManagerRouter struct {
	Manager *cell.Manager
}

// Route implements Router.
func (r ManagerRouter) Route(pool string) (*cell.Cell, error) {
	c, ok := r.Manager.Get(cell.Key(pool))
	if !ok {
		return nil, ErrUnknownPool
	}
	return c, nil
}

// toStatus converts routing errors to gRPC status errors.
func toStatus(err error) error {
	switch {
	case errors.Is(err, ErrNoPoolMetadata):
		return status.Error(codes.Internal, ErrNoPoolMetadata.Error())
	case errors.Is(err, ErrUnknownPool):
		return status.Error(codes.Internal, ErrUnknownPool.Error())
	case errors.Is(err, ErrCellDraining):
		return status.Error(codes.Unavailable, ErrCellDraining.Error())
	default:
		return err
	}
}
