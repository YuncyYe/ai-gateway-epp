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

// Package clustertable implements the cluster-table-discovery endpoint
// discovery plugin: endpoint sets arrive from the ai-gateway-api cluster
// table (pushed into the Hub by the discovery poller) and are diffed against
// each engine generation's applied set, then fed to the llm-d datastore
// through the DiscoveryNotifier in a single goroutine.
package clustertable

import (
	"context"
	"sync"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// Hub holds the desired endpoint set per cluster. The discovery poller calls
// ReplaceAll; discovery plugins read and watch it.
type Hub struct {
	mu      sync.RWMutex
	data    map[string][]fwkdl.EndpointMetadata
	version uint64
	ch      chan struct{}
}

// NewHub creates an empty hub.
func NewHub() *Hub {
	return &Hub{data: map[string][]fwkdl.EndpointMetadata{}, ch: make(chan struct{})}
}

// ReplaceAll atomically replaces the entire desired endpoint table.
func (h *Hub) ReplaceAll(data map[string][]fwkdl.EndpointMetadata) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.data = data
	h.version++
	close(h.ch)
	h.ch = make(chan struct{})
}

// Snapshot returns the desired endpoints for cluster and the current version.
func (h *Hub) Snapshot(cluster string) ([]fwkdl.EndpointMetadata, uint64) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.data[cluster], h.version
}

// WaitChange blocks until the table version differs from since or ctx ends.
func (h *Hub) WaitChange(ctx context.Context, since uint64) (uint64, error) {
	for {
		h.mu.RLock()
		if h.version != since {
			v := h.version
			h.mu.RUnlock()
			return v, nil
		}
		ch := h.ch
		h.mu.RUnlock()

		select {
		case <-ch:
		case <-ctx.Done():
			return since, ctx.Err()
		}
	}
}
