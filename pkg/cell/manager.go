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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

// ErrNotAssigned is returned when config or traffic arrives for a cluster
// this instance does not hold.
var ErrNotAssigned = errors.New("cell not assigned to this instance")

// CompileFunc builds a new engine generation for a cell; injectable for tests.
type CompileFunc func(cellCtx context.Context, key Key, raw json.RawMessage, c *Cell) (*Engine, error)

// Options configures a Manager.
type Options struct {
	Logger                   logr.Logger
	MetricsRecorder          fwkplugin.MetricsRecorder
	PoolNamespace            string
	RefreshMetricsInterval   time.Duration
	MaxPoolBufferSize        int
	AllowExperimentalPlugins bool

	// DrainTimeout bounds how long a replaced engine waits for in-flight
	// requests before being dropped.
	DrainTimeout time.Duration
	// DrainWorkers bounds concurrent drains; protects against drain storms
	// during mass failovers.
	DrainWorkers int

	// Compile overrides engine compilation (tests).
	Compile CompileFunc
	// NewCell overrides cell creation (tests).
	NewCell func(ctx context.Context, key Key) (*Cell, error)
}

// Manager owns the Cell registry and lifecycle transitions driven by the
// epp_data watcher (roles) and its per-cell engine hot-swaps.
type Manager struct {
	opts    Options
	logger  logr.Logger
	compile CompileFunc
	newCell func(ctx context.Context, key Key) (*Cell, error)

	cells sync.Map // Key -> *Cell

	drainCh chan drainTask
}

type drainTask struct {
	cell     *Cell
	eng      *Engine
	teardown bool // drop the whole cell after the drain
}

// NewManager creates a Manager and starts its drain workers. ctx bounds the
// workers' lifetime.
func NewManager(ctx context.Context, opts Options) *Manager {
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 60 * time.Second
	}
	if opts.DrainWorkers <= 0 {
		opts.DrainWorkers = 4
	}
	m := &Manager{
		opts:    opts,
		logger:  opts.Logger.WithName("cell-manager"),
		drainCh: make(chan drainTask, 64),
	}
	m.compile = opts.Compile
	if m.compile == nil {
		m.compile = func(cellCtx context.Context, key Key, raw json.RawMessage, c *Cell) (*Engine, error) {
			return Compile(cellCtx, key, raw, c, Dependencies{
				Logger:                   opts.Logger,
				MetricsRecorder:          opts.MetricsRecorder,
				PoolNamespace:            opts.PoolNamespace,
				RefreshMetricsInterval:   opts.RefreshMetricsInterval,
				MaxPoolBufferSize:        opts.MaxPoolBufferSize,
				AllowExperimentalPlugins: opts.AllowExperimentalPlugins,
			})
		}
	}
	m.newCell = opts.NewCell
	if m.newCell == nil {
		m.newCell = m.defaultNewCell
	}
	for i := 0; i < opts.DrainWorkers; i++ {
		go m.drainWorker(ctx)
	}
	return m
}

func (m *Manager) defaultNewCell(ctx context.Context, key Key) (*Cell, error) {
	cellCtx, cancel := context.WithCancel(ctx)
	rt := datalayer.NewRuntime(m.opts.RefreshMetricsInterval)
	pool := datalayer.NewEndpointPool(m.opts.PoolNamespace, string(key))
	ds := datastore.NewDatastore(cellCtx, rt).WithEndpointPool(pool)
	return &Cell{
		Key:    key,
		ds:     ds,
		rt:     rt,
		ctx:    cellCtx,
		cancel: cancel,
		ready:  make(chan struct{}),
	}, nil
}

// Get returns the cell for key.
func (m *Manager) Get(key Key) (*Cell, bool) {
	v, ok := m.cells.Load(key)
	if !ok {
		return nil, false
	}
	return v.(*Cell), true
}

// List returns a snapshot of all cells.
func (m *Manager) List() []*Cell {
	out := []*Cell{}
	m.cells.Range(func(_, v any) bool {
		out = append(out, v.(*Cell))
		return true
	})
	return out
}

// Ensure returns the cell for key, creating it if needed, and applies the
// requested role.
func (m *Manager) Ensure(ctx context.Context, key Key, role Role) (*Cell, error) {
	c, ok := m.Get(key)
	if !ok {
		nc, err := m.newCell(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("create cell %s: %w", key, err)
		}
		nc.setState(StateCreating)
		actual, loaded := m.cells.LoadOrStore(key, nc)
		if loaded {
			nc.Close()
			c = actual.(*Cell)
		} else {
			c = nc
			m.logger.Info("cell created", "cell", string(key))
		}
	}

	switch {
	case c.Role() == role:
		// no-op
	case role == RolePrimary:
		m.Promote(key)
		m.logger.V(2).Info("role transition", "cell", string(key), "role", "primary")
	case role == RoleStandby:
		if c.Role() == RoleNone {
			c.setRole(RoleStandby)
			m.logger.V(2).Info("role transition", "cell", string(key), "role", "standby")
		} else {
			m.Demote(key)
			m.logger.V(2).Info("role transition", "cell", string(key), "role", "standby(demoted)")
		}
	}
	return c, nil
}

// Promote flips a cell to serving.
func (m *Manager) Promote(key Key) {
	c, ok := m.Get(key)
	if !ok || c.State() == StateDraining {
		return
	}
	c.setRole(RolePrimary)
	if c.State() == StateStandby || c.State() == StateCreating {
		c.setState(StatePrimary)
	}
}

// Demote drains the current engine but keeps the data plane hot.
func (m *Manager) Demote(key Key) {
	c, ok := m.Get(key)
	if !ok || c.State() == StateDraining {
		return
	}
	c.setRole(RoleStandby)
	if c.State() == StatePrimary {
		c.setState(StateStandby)
	}
	if eng := c.Engine(); eng != nil {
		m.scheduleDrain(c, eng, false)
	}
}

// Drop drains the engine and releases the data plane.
func (m *Manager) Drop(key Key) {
	c, ok := m.Get(key)
	if !ok {
		return
	}
	m.logger.V(2).Info("dropping cell", "cell", string(key))
	c.setState(StateDraining)
	if eng := c.Engine(); eng != nil {
		m.scheduleDrain(c, eng, true)
		return
	}
	c.Close()
	m.cells.CompareAndDelete(key, c)
}

// ApplyConfig compiles and hot-swaps the engine for key. It returns false
// without error when the config is unchanged. A compile failure leaves the
// old engine serving and is reported as an error.
func (m *Manager) ApplyConfig(ctx context.Context, key Key, raw json.RawMessage) (bool, error) {
	c, ok := m.Get(key)
	if !ok || c.State() == StateDraining {
		return false, ErrNotAssigned
	}

	hash := HashConfig(raw)
	if v := c.currentHash.Load(); v != nil && v.(string) == hash {
		m.logger.V(2).Info("config unchanged, skipping compile", "cell", string(key), "hash", hash)
		return false, nil
	}

	m.logger.V(2).Info("compiling engine", "cell", string(key), "hash", hash)
	eng, err := m.compile(c.ctx, key, raw, c)
	if err != nil {
		engineReloads.WithLabelValues(string(key), "invalid").Inc()
		return false, err
	}

	old := c.engine.Swap(eng)
	c.currentHash.Store(hash)
	engineReloads.WithLabelValues(string(key), "success").Inc()
	engineCurrentVersion.DeletePartialMatch(prometheus.Labels{"cluster": string(key)})
	engineCurrentVersion.WithLabelValues(string(key), eng.Version).Set(1)

	eng.start(c, m.logger)
	if old != nil {
		m.scheduleDrain(c, old, false)
	}
	return true, nil
}

func (m *Manager) scheduleDrain(c *Cell, eng *Engine, teardown bool) {
	select {
	case m.drainCh <- drainTask{cell: c, eng: eng, teardown: teardown}:
	default:
		// Drain queue full: fall back to synchronous drain rather than leaking
		// the engine (bounded by DrainTimeout).
		m.logger.Info("drain queue full, draining synchronously", "cell", string(c.Key))
		m.drain(drainTask{cell: c, eng: eng, teardown: teardown})
	}
}

func (m *Manager) drainWorker(ctx context.Context) {
	for {
		select {
		case t := <-m.drainCh:
			m.drain(t)
		case <-ctx.Done():
			return
		}
	}
}

func (m *Manager) drain(t drainTask) {
	start := time.Now()
	t.eng.drain(m.opts.DrainTimeout)
	drainDuration.WithLabelValues(string(t.cell.Key)).Observe(time.Since(start).Seconds())
	if t.teardown {
		t.cell.Close()
		m.cells.CompareAndDelete(t.cell.Key, t.cell)
		m.logger.Info("cell dropped", "cell", string(t.cell.Key))
	}
}

// WaitReady blocks until every listed cell is ready or the context ends.
// Used by tests and startup probes.
func (m *Manager) WaitReady(ctx context.Context, keys ...Key) error {
	return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		for _, k := range keys {
			c, ok := m.Get(k)
			if !ok {
				return false, nil
			}
			select {
			case <-c.Ready():
			case <-ctx.Done():
				return false, ctx.Err()
			default:
				return false, nil
			}
		}
		return true, nil
	})
}
