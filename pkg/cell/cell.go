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

package cell

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
)

// Cell is the unit of per-cluster lifecycle: the data plane (datastore,
// metrics collection) is resident for the Cell's lifetime; the policy plane
// (Engine) is hot-swapped by atomically replacing a pointer.
type Cell struct {
	Key Key

	ds datastore.Datastore
	rt *datalayer.Runtime

	ctx    context.Context
	cancel context.CancelFunc

	engine atomic.Pointer[Engine]
	role   atomic.Int32
	state  atomic.Int32

	currentHash    atomic.Value // string; sha256 of the applied raw config
	dataConfigured atomic.Bool

	readyOnce sync.Once
	ready     chan struct{}
	readyAt   atomic.Int64 // unix nanos; 0 until ready
}

func (c *Cell) Datastore() datastore.Datastore { return c.ds }
func (c *Cell) Runtime() *datalayer.Runtime    { return c.rt }

// Engine returns the current engine; nil before the first successful compile.
func (c *Cell) Engine() *Engine { return c.engine.Load() }

func (c *Cell) Role() Role   { return Role(c.role.Load()) }
func (c *Cell) State() State { return State(c.state.Load()) }

func (c *Cell) setRole(r Role) {
	c.role.Store(int32(r))
	cellStateGauge.WithLabelValues(string(c.Key), r.String(), c.State().String()).Set(1)
}

func (c *Cell) setState(s State) {
	c.state.Store(int32(s))
	cellStateGauge.WithLabelValues(string(c.Key), c.Role().String(), s.String()).Set(1)
}

// Ready closes once the Cell has completed its first discovery sync and first
// engine compile.
func (c *Cell) Ready() <-chan struct{} { return c.ready }

// ReadySince is the time the Cell became ready; zero if not yet ready.
func (c *Cell) ReadySince() time.Time {
	t := c.readyAt.Load()
	if t == 0 {
		return time.Time{}
	}
	return time.Unix(0, t)
}

// markReady is idempotent; it performs the CREATING -> STANDBY transition.
func (c *Cell) markReady() {
	c.readyOnce.Do(func() {
		c.readyAt.Store(time.Now().UnixNano())
		close(c.ready)
		if c.State() == StateCreating {
			c.setState(StateStandby)
		}
	})
}

// Track registers an in-flight request on e. It returns false when the engine
// has already been swapped out and the request must be retried on the new
// engine; the returned func must be called exactly once on success.
func (c *Cell) Track(e *Engine) (done func(), ok bool) {
	e.wg.Add(1)
	if c.engine.Load() != e {
		e.wg.Done()
		return nil, false
	}
	return e.wg.Done, true
}

// Close tears down the data plane.
func (c *Cell) Close() {
	c.cancel()
}
