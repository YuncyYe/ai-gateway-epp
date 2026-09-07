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
	"testing"
	"time"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
)

// newTestCell builds a real datastore-backed cell, mirroring
// Manager.defaultNewCell. The cell context is cancelled on test cleanup.
func newTestCell(t *testing.T, key Key) *Cell {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rt := datalayer.NewRuntime(time.Millisecond)
	pool := datalayer.NewEndpointPool("test-pool", string(key))
	ds := datastore.NewDatastore(ctx, rt).WithEndpointPool(pool)
	c := &Cell{
		Key:    key,
		ds:     ds,
		rt:     rt,
		ctx:    ctx,
		cancel: cancel,
		ready:  make(chan struct{}),
	}
	t.Cleanup(c.Close)
	return c
}

func TestCellInitialState(t *testing.T) {
	c := newTestCell(t, "cell-init")

	if c.Role() != RoleNone {
		t.Errorf("Role() = %v, want RoleNone", c.Role())
	}
	if c.State() != StateCreating {
		t.Errorf("State() = %v, want StateCreating", c.State())
	}
	if c.Engine() != nil {
		t.Error("Engine() should be nil before first compile")
	}
	if !c.ReadySince().IsZero() {
		t.Error("ReadySince() should be zero before ready")
	}
	select {
	case <-c.Ready():
		t.Error("Ready() closed before markReady")
	default:
	}
	if c.Datastore() == nil || c.Runtime() == nil {
		t.Error("Datastore()/Runtime() must be non-nil")
	}
}

func TestMarkReady(t *testing.T) {
	t.Run("creating to standby", func(t *testing.T) {
		c := newTestCell(t, "cell-ready-1")
		c.markReady()

		select {
		case <-c.Ready():
		default:
			t.Fatal("Ready() not closed after markReady")
		}
		if c.ReadySince().IsZero() {
			t.Error("ReadySince() still zero after markReady")
		}
		if c.State() != StateStandby {
			t.Errorf("State() = %v, want StateStandby", c.State())
		}
	})

	t.Run("idempotent", func(t *testing.T) {
		c := newTestCell(t, "cell-ready-2")
		c.markReady()
		first := c.ReadySince()
		c.markReady()
		if !c.ReadySince().Equal(first) {
			t.Error("second markReady changed ReadySince")
		}
	})

	t.Run("does not clobber later state", func(t *testing.T) {
		c := newTestCell(t, "cell-ready-3")
		c.setState(StatePrimary)
		c.markReady()
		if c.State() != StatePrimary {
			t.Errorf("State() = %v, want StatePrimary preserved", c.State())
		}
	})
}

func TestCellTrack(t *testing.T) {
	c := newTestCell(t, "cell-track")
	eng := &Engine{}
	c.engine.Store(eng)

	done, ok := c.Track(eng)
	if !ok || done == nil {
		t.Fatalf("Track(current) = done=%t ok=%v, want done func and true", done != nil, ok)
	}
	done()

	// A stale engine must be rejected so the request retries on the new one.
	stale := &Engine{}
	done, ok = c.Track(stale)
	if ok || done != nil {
		t.Fatalf("Track(stale) = done=%t ok=%v, want (nil, false)", done != nil, ok)
	}
}

func TestCellTrackConcurrent(t *testing.T) {
	c := newTestCell(t, "cell-track-race")
	e1 := &Engine{}
	e2 := &Engine{}
	c.engine.Store(e1)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if (i+j)%7 == 0 {
					if c.engine.Load() == e1 {
						c.engine.CompareAndSwap(e1, e2)
					} else {
						c.engine.CompareAndSwap(e2, e1)
					}
				}
				e := c.engine.Load()
				done, ok := c.Track(e)
				if ok {
					done()
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestCellClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rt := datalayer.NewRuntime(time.Millisecond)
	pool := datalayer.NewEndpointPool("test-pool", "cell-close")
	ds := datastore.NewDatastore(ctx, rt).WithEndpointPool(pool)
	c := &Cell{
		Key:    "cell-close",
		ds:     ds,
		rt:     rt,
		ctx:    ctx,
		cancel: cancel,
		ready:  make(chan struct{}),
	}
	c.Close()
	select {
	case <-c.ctx.Done():
	default:
		t.Error("Close() did not cancel the cell context")
	}
}
