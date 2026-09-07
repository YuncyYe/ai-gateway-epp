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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
)

func newTestManager(t *testing.T, compile CompileFunc) (*Manager, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	m := NewManager(ctx, Options{
		Logger:                   logr.Discard(),
		DrainTimeout:             50 * time.Millisecond,
		DrainWorkers:             1,
		Compile:                  compile,
		AllowExperimentalPlugins: true,
	})
	t.Cleanup(cancel)
	return m, ctx, cancel
}

func stubCompile(version string, err error) CompileFunc {
	return func(_ context.Context, key Key, raw json.RawMessage, c *Cell) (*Engine, error) {
		if err != nil {
			return nil, err
		}
		return &Engine{Version: version + string(key)}, nil
	}
}

func TestEnsureAndPromoteDemote(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))

	c, err := m.Ensure(ctx, "cluster-a", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}
	if c.Role() != RoleStandby || c.State() != StateCreating {
		t.Fatalf("role=%v state=%v", c.Role(), c.State())
	}

	m.Promote("cluster-a")
	if c.Role() != RolePrimary || c.State() != StatePrimary {
		t.Fatalf("after promote: role=%v state=%v", c.Role(), c.State())
	}

	m.Demote("cluster-a")
	if c.Role() != RoleStandby || c.State() != StateStandby {
		t.Fatalf("after demote: role=%v state=%v", c.Role(), c.State())
	}

	// Demote on an unknown key is a no-op.
	m.Demote("nope")
}

func TestApplyConfigHotSwap(t *testing.T) {
	var compiles atomic.Int32
	compile := func(_ context.Context, key Key, raw json.RawMessage, c *Cell) (*Engine, error) {
		compiles.Add(1)
		return &Engine{Version: HashConfig(raw)}, nil
	}
	m, ctx, _ := newTestManager(t, compile)

	if _, err := m.Ensure(ctx, "cluster-a", RoleStandby); err != nil {
		t.Fatal(err)
	}

	cfg1 := json.RawMessage(`{"a":1}`)
	swapped, err := m.ApplyConfig(ctx, "cluster-a", cfg1)
	if err != nil || !swapped {
		t.Fatalf("swapped=%v err=%v", swapped, err)
	}
	eng1 := m.List()[0].Engine()
	if eng1 == nil || eng1.Version != HashConfig(cfg1) {
		t.Fatalf("engine version mismatch")
	}

	// Same config: no recompile.
	swapped, err = m.ApplyConfig(ctx, "cluster-a", cfg1)
	if err != nil || swapped || compiles.Load() != 1 {
		t.Fatalf("dedup: swapped=%v compiles=%d", swapped, compiles.Load())
	}

	// New config: swap, old engine drains.
	cfg2 := json.RawMessage(`{"a":2}`)
	swapped, err = m.ApplyConfig(ctx, "cluster-a", cfg2)
	if err != nil || !swapped {
		t.Fatalf("swapped=%v err=%v", swapped, err)
	}
	if m.List()[0].Engine().Version != HashConfig(cfg2) {
		t.Fatal("engine not swapped")
	}
}

func TestApplyConfigCompileFailureKeepsOld(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	if _, err := m.Ensure(ctx, "cluster-a", RoleStandby); err != nil {
		t.Fatal(err)
	}
	good := json.RawMessage(`{"a":1}`)
	if _, err := m.ApplyConfig(ctx, "cluster-a", good); err != nil {
		t.Fatal(err)
	}
	engVersion := m.List()[0].Engine().Version

	// Compile failure: error reported, old engine intact.
	m.compile = stubCompile("v", errors.New("compile boom"))
	bad := json.RawMessage(`{"a":2}`)
	if _, err := m.ApplyConfig(ctx, "cluster-a", bad); err == nil {
		t.Fatal("expected compile error")
	}
	if got := m.List()[0].Engine().Version; got != engVersion {
		t.Fatalf("old engine lost: %v", got)
	}
}

func TestApplyConfigUnassigned(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	if _, err := m.ApplyConfig(ctx, "ghost", json.RawMessage(`{}`)); !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("err=%v", err)
	}
}

func TestDropRemovesCell(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	c, err := m.Ensure(ctx, "cluster-a", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}
	m.Drop("cluster-a")
	if c.State() != StateDraining {
		t.Fatalf("state=%v", c.State())
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.Get("cluster-a"); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cell not dropped after drain")
}

func TestGetAndList(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))

	if _, ok := m.Get("missing"); ok {
		t.Fatal("Get on unknown key returned ok")
	}
	if got := m.List(); len(got) != 0 {
		t.Fatalf("List on empty manager returned %d cells", len(got))
	}

	a, err := m.Ensure(ctx, "a", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}
	b, err := m.Ensure(ctx, "b", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}

	if got, ok := m.Get("a"); !ok || got != a {
		t.Errorf("Get(a) = (%v, %v), want the ensured cell", got, ok)
	}
	cells := m.List()
	if len(cells) != 2 {
		t.Fatalf("List() = %d cells, want 2", len(cells))
	}
	seen := map[*Cell]bool{}
	for _, c := range cells {
		seen[c] = true
	}
	if !seen[a] || !seen[b] {
		t.Errorf("List() missing ensured cells: %v", cells)
	}
}

// newTrackedCell builds a real cell whose cancel records that it was closed.
func newTrackedCell(ctx context.Context, key Key) (*Cell, *atomic.Bool) {
	cellCtx, cancel := context.WithCancel(ctx)
	var closed atomic.Bool
	rt := datalayer.NewRuntime(time.Millisecond)
	pool := datalayer.NewEndpointPool("test-pool", string(key))
	ds := datastore.NewDatastore(cellCtx, rt).WithEndpointPool(pool)
	return &Cell{
		Key: key,
		ds:  ds,
		rt:  rt,
		ctx: cellCtx,
		cancel: func() {
			closed.Store(true)
			cancel()
		},
		ready: make(chan struct{}),
	}, &closed
}

func TestEnsureConcurrentSingleWinner(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	var created atomic.Int32
	var mu sync.Mutex
	var closedFlags []*atomic.Bool
	m := NewManager(ctx, Options{
		Logger:       logr.Discard(),
		DrainTimeout: 50 * time.Millisecond,
		DrainWorkers: 1,
		Compile:      stubCompile("v", nil),
		NewCell: func(ctx context.Context, key Key) (*Cell, error) {
			created.Add(1)
			c, closed := newTrackedCell(ctx, key)
			mu.Lock()
			closedFlags = append(closedFlags, closed)
			mu.Unlock()
			<-release // hold every creation in flight so they race on LoadOrStore
			return c, nil
		},
	})

	const n = 6
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			_, err := m.Ensure(context.Background(), "race-key", RoleStandby)
			errs <- err
		}()
	}

	// Wait for all creations to be in flight, then let them finish.
	deadline := time.Now().Add(5 * time.Second)
	for created.Load() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if created.Load() != n {
		t.Fatalf("only %d creations in flight, want %d", created.Load(), n)
	}
	close(release)
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("Ensure() error = %v", err)
		}
	}

	if got := len(m.List()); got != 1 {
		t.Fatalf("manager holds %d cells, want 1", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(closedFlags) != n {
		t.Fatalf("tracked %d cells, want %d", len(closedFlags), n)
	}
	closed := 0
	for _, f := range closedFlags {
		if f.Load() {
			closed++
		}
	}
	if closed != n-1 {
		t.Errorf("%d losing cells were closed, want %d", closed, n-1)
	}
}

func TestEnsureNewCellError(t *testing.T) {
	boom := errors.New("newcell boom")
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	m.newCell = func(context.Context, Key) (*Cell, error) { return nil, boom }

	_, err := m.Ensure(ctx, "fail-key", RoleStandby)
	if !errors.Is(err, boom) {
		t.Fatalf("Ensure() error = %v, want it to wrap %v", err, boom)
	}
	if got := len(m.List()); got != 0 {
		t.Fatalf("List() = %d cells, want 0", got)
	}
}

func TestPromoteFromCreating(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	c, err := m.Ensure(ctx, "p-key", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}
	if c.State() != StateCreating {
		t.Fatalf("state=%v, want StateCreating before promote", c.State())
	}

	m.Promote("p-key")
	if c.Role() != RolePrimary || c.State() != StatePrimary {
		t.Fatalf("after promote: role=%v state=%v", c.Role(), c.State())
	}

	// Promote is idempotent.
	m.Promote("p-key")
	if c.Role() != RolePrimary || c.State() != StatePrimary {
		t.Fatalf("after second promote: role=%v state=%v", c.Role(), c.State())
	}
}

func TestPromoteDemoteOnDrainingCell(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	c, err := m.Ensure(ctx, "d-key", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}
	c.setState(StateDraining)

	m.Promote("d-key")
	if c.Role() != RoleStandby || c.State() != StateDraining {
		t.Fatalf("promote on draining: role=%v state=%v", c.Role(), c.State())
	}
	m.Demote("d-key")
	if c.Role() != RoleStandby || c.State() != StateDraining {
		t.Fatalf("demote on draining: role=%v state=%v", c.Role(), c.State())
	}

	// Unknown keys are no-ops too.
	m.Promote("nope")
	m.Drop("nope")
}

func TestDemoteDrainsEngine(t *testing.T) {
	engCtx, engCancel := context.WithCancel(context.Background())
	compile := func(_ context.Context, key Key, raw json.RawMessage, c *Cell) (*Engine, error) {
		return &Engine{Version: HashConfig(raw), ctx: engCtx, cancel: engCancel}, nil
	}
	m, ctx, _ := newTestManager(t, compile)
	if _, err := m.Ensure(ctx, "demote-drain", RolePrimary); err != nil {
		t.Fatal(err)
	}
	if _, err := m.ApplyConfig(ctx, "demote-drain", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}

	m.Demote("demote-drain")

	select {
	case <-engCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("engine not drained after demote")
	}
	if c, _ := m.Get("demote-drain"); c.State() != StateStandby {
		t.Fatalf("state=%v, want StateStandby", c.State())
	}
}

func TestDropWithEngineRemovesCell(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	c, err := m.Ensure(ctx, "drop-eng", RolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.ApplyConfig(ctx, "drop-eng", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}

	m.Drop("drop-eng")
	if c.State() != StateDraining {
		t.Fatalf("state=%v, want StateDraining", c.State())
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := m.Get("drop-eng"); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cell with engine not dropped after drain")
}

func TestApplyConfigOnDrainingCell(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	if _, err := m.Ensure(ctx, "drain-cfg", RoleStandby); err != nil {
		t.Fatal(err)
	}
	m.Drop("drain-cfg")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := m.ApplyConfig(ctx, "drain-cfg", json.RawMessage(`{"a":1}`)); !errors.Is(err, ErrNotAssigned) {
			t.Fatalf("ApplyConfig err = %v, want ErrNotAssigned", err)
		}
		if _, ok := m.Get("drain-cfg"); !ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestApplyConfigConcurrentSwap(t *testing.T) {
	var compiles atomic.Int32
	compile := func(_ context.Context, key Key, raw json.RawMessage, c *Cell) (*Engine, error) {
		compiles.Add(1)
		return &Engine{Version: HashConfig(raw)}, nil
	}
	m, ctx, _ := newTestManager(t, compile)
	if _, err := m.Ensure(ctx, "swap-key", RolePrimary); err != nil {
		t.Fatal(err)
	}

	const n = 8
	r := make([]json.RawMessage, n)
	for i := range r {
		r[i] = json.RawMessage(fmt.Sprintf(`{"v":%d}`, i))
	}
	valid := map[string]bool{}
	for _, raw := range r {
		valid[HashConfig(raw)] = true
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := m.ApplyConfig(ctx, "swap-key", r[i]); err != nil {
				t.Errorf("ApplyConfig() error = %v", err)
			}
		}(i)
	}
	wg.Wait()

	if got := int(compiles.Load()); got != n {
		t.Errorf("compiles = %d, want %d (all configs differ)", got, n)
	}
	eng := m.List()[0].Engine()
	if eng == nil || !valid[eng.Version] {
		t.Errorf("final engine version %q not among applied configs", eng.Version)
	}
}

func TestWaitReady(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	c, err := m.Ensure(ctx, "ready-key", RoleStandby)
	if err != nil {
		t.Fatal(err)
	}

	// Not ready yet: must time out.
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer timeoutCancel()
	if err := m.WaitReady(timeoutCtx, "ready-key"); err == nil {
		t.Fatal("WaitReady returned nil before the cell was ready")
	}

	// Unknown key: must time out.
	ghostCtx, ghostCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer ghostCancel()
	if err := m.WaitReady(ghostCtx, "ghost"); err == nil {
		t.Fatal("WaitReady returned nil for an unknown key")
	}

	c.markReady()
	if err := m.WaitReady(ctx, "ready-key"); err != nil {
		t.Fatalf("WaitReady after markReady: %v", err)
	}
	if c.ReadySince().IsZero() {
		t.Error("ReadySince still zero after ready")
	}
}

func TestDrainQueueFullDrainsSynchronously(t *testing.T) {
	m := &Manager{
		opts:    Options{Logger: logr.Discard(), DrainTimeout: time.Second},
		logger:  logr.Discard(),
		drainCh: make(chan drainTask, 2),
	}
	// Fill the queue; no workers consume it, so scheduleDrain must fall back
	// to a synchronous drain.
	for i := 0; i < cap(m.drainCh); i++ {
		m.drainCh <- drainTask{}
	}

	c := newTestCell(t, "qfull")
	engCtx, engCancel := context.WithCancel(context.Background())
	eng := &Engine{ctx: engCtx, cancel: engCancel}

	m.scheduleDrain(c, eng, false)

	select {
	case <-engCtx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("synchronous fallback drain did not run")
	}
}

func TestManagerConcurrentStress(t *testing.T) {
	m, ctx, _ := newTestManager(t, stubCompile("v", nil))
	keys := []Key{"s1", "s2", "s3"}

	var wg sync.WaitGroup
	for i := 0; i < 48; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			k := keys[i%len(keys)]
			cfg := json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))
			switch i % 6 {
			case 0:
				if _, err := m.Ensure(ctx, k, RolePrimary); err != nil {
					t.Errorf("Ensure() error = %v", err)
				}
			case 1:
				if _, err := m.Ensure(ctx, k, RoleStandby); err != nil {
					t.Errorf("Ensure() error = %v", err)
				}
			case 2:
				m.Promote(k)
			case 3:
				m.Demote(k)
			case 4, 5:
				if _, err := m.ApplyConfig(ctx, k, cfg); err != nil && !errors.Is(err, ErrNotAssigned) {
					t.Errorf("ApplyConfig() error = %v", err)
				}
			}
			m.Get(k)
			m.List()
		}(i)
	}
	wg.Wait()
}
