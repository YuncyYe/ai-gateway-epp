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

package poller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

func newTestManager(t *testing.T, compile cell.CompileFunc) *cell.Manager {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return cell.NewManager(ctx, cell.Options{
		Logger:       logr.Discard(),
		DrainTimeout: 50 * time.Millisecond,
		DrainWorkers: 1,
		Compile:      compile,
	})
}

func stubCompileOK(version string) cell.CompileFunc {
	return func(_ context.Context, key cell.Key, raw json.RawMessage, c *cell.Cell) (*cell.Engine, error) {
		return &cell.Engine{Version: version + "-" + string(key)}, nil
	}
}

func sp(s string) *string { return &s }

func TestResolveRole(t *testing.T) {
	tests := []struct {
		name string
		self string
		e    innerapi.AssignmentEntry
		role cell.Role
		mine bool
	}{
		{"primary hit", "epp-1", innerapi.AssignmentEntry{Primary: sp("epp-1"), Standby: sp("epp-2")}, cell.RolePrimary, true},
		{"standby hit", "epp-2", innerapi.AssignmentEntry{Primary: sp("epp-1"), Standby: sp("epp-2")}, cell.RoleStandby, true},
		{"no hit", "epp-3", innerapi.AssignmentEntry{Primary: sp("epp-1"), Standby: sp("epp-2")}, cell.RoleNone, false},
		{"single instance group standby null", "epp-1", innerapi.AssignmentEntry{Primary: sp("epp-1")}, cell.RolePrimary, true},
		{"empty entry", "epp-1", innerapi.AssignmentEntry{}, cell.RoleNone, false},
		{"null primary abnormal standby hit", "epp-2", innerapi.AssignmentEntry{Standby: sp("epp-2")}, cell.RoleStandby, true},
		{"no wildcard", "epp-*", innerapi.AssignmentEntry{Primary: sp("*")}, cell.RoleNone, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			role, mine := resolveRole(tt.self, tt.e)
			if role != tt.role || mine != tt.mine {
				t.Fatalf("resolveRole(%q, %+v) = %v, %v; want %v, %v", tt.self, tt.e, role, mine, tt.role, tt.mine)
			}
		})
	}
}

// observingServer serves the two-section epp_data/config payload and counts
// every request by method; the readiness report must never be posted.
type observingServer struct {
	srv      *httptest.Server
	posts    atomic.Int64
	requests atomic.Int64
}

func newObservingServer(t *testing.T, payload string) *observingServer {
	t.Helper()
	o := &observingServer{}
	o.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.requests.Add(1)
		if r.Method == http.MethodPost {
			o.posts.Add(1)
			t.Errorf("unexpected POST %s (readiness report was removed)", r.URL.Path)
		}
		if r.URL.Path != innerapi.EppDataConfigPath {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Write([]byte(payload))
	}))
	t.Cleanup(o.srv.Close)
	return o
}

func TestEppDataFetchChanged(t *testing.T) {
	o := newObservingServer(t, `{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v4","Config":{`+
		`"epp_config":{"c-a":{"x":1}},"assignment":{"c-a":{"primary":"epp-1"}}}}}`)
	w := NewEppDataWatcher(innerapi.NewClient(o.srv.URL, "", time.Second), "epp-1", nil, logr.Discard())

	changed, ver, cfg, err := w.Fetch(context.Background(), "v3")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "v4" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
	if string(cfg.EppConfig["c-a"]) != `{"x":1}` {
		t.Fatalf("epp_config=%v", cfg.EppConfig)
	}
	if cfg.Assignment["c-a"].Primary == nil || *cfg.Assignment["c-a"].Primary != "epp-1" {
		t.Fatalf("assignment=%+v", cfg.Assignment)
	}
	if cfg.Assignment["c-a"].Standby != nil {
		t.Fatalf("standby=%v, want nil", *cfg.Assignment["c-a"].Standby)
	}
}

func TestEppDataFetchUnchanged(t *testing.T) {
	o := newObservingServer(t, `{"ErrNum":200,"ErrMsg":"ok","Data":null}`)
	w := NewEppDataWatcher(innerapi.NewClient(o.srv.URL, "", time.Second), "epp-1", nil, logr.Discard())

	changed, ver, cfg, err := w.Fetch(context.Background(), "v3")
	if err != nil {
		t.Fatal(err)
	}
	if changed || ver != "v3" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
	if cfg.EppConfig != nil || cfg.Assignment != nil {
		t.Fatalf("cfg=%+v, want zero value on Data:null", cfg)
	}
}

func TestEppDataFetchServerError(t *testing.T) {
	o := newObservingServer(t, `{"ErrNum":500,"ErrMsg":"boom"}`)
	w := NewEppDataWatcher(innerapi.NewClient(o.srv.URL, "", time.Second), "epp-1", nil, logr.Discard())
	if _, _, _, err := w.Fetch(context.Background(), ""); err == nil {
		t.Fatal("expected error")
	}
}

// TestEppDataHandleLifecycle drives the full role/config diff sequence in one
// snapshot stream: ensure, promote, demote, drop, per-cluster compile
// isolation, and zero report posts.
func TestEppDataHandleLifecycle(t *testing.T) {
	o := newObservingServer(t, `{"ErrNum":200,"ErrMsg":"ok","Data":null}`)
	rec := &compileRecorder{versions: map[cell.Key]string{}}
	m := newTestManager(t, rec.compile)
	w := NewEppDataWatcher(innerapi.NewClient(o.srv.URL, "", time.Second), "epp-1", m, logr.Discard())
	ctx := context.Background()

	entry := func(primary, standby string) innerapi.AssignmentEntry {
		e := innerapi.AssignmentEntry{Primary: sp(primary)}
		if standby != "" {
			e.Standby = sp(standby)
		}
		return e
	}

	// Round 1: c-a primary (with peer), c-b standby, c-c belongs to another
	// instance, c-d config present but no role in the assignment.
	cfg := innerapi.EppDataConfig{
		EppConfig: map[string]json.RawMessage{
			"c-a": json.RawMessage(`{"a":1}`),
			"c-b": json.RawMessage(`{"boom":true}`),
			"c-c": json.RawMessage(`{"a":3}`),
			"c-d": json.RawMessage(`{"a":4}`),
		},
		Assignment: map[string]innerapi.AssignmentEntry{
			"c-a": entry("epp-1", "epp-2"),
			"c-b": entry("epp-9", "epp-1"),
			"c-c": entry("epp-9", "epp-8"),
		},
	}
	if err := w.Handle(ctx, cfg); err != nil {
		t.Fatal(err)
	}

	if c, ok := m.Get("c-a"); !ok || c.Role() != cell.RolePrimary {
		t.Fatalf("c-a role=%v", c.Role())
	} else if c.Engine() == nil || c.Engine().Version != "v-c-a" {
		t.Fatalf("c-a engine=%v", c.Engine())
	}
	if _, ok := m.Get("c-c"); ok {
		t.Fatal("c-c created though assigned to another instance")
	}
	if _, ok := m.Get("c-d"); ok {
		t.Fatal("c-d created though it has no assignment role")
	}
	if _, ok := rec.versions["c-c"]; ok {
		t.Fatal("c-c config compiled without a role")
	}
	if _, ok := rec.versions["c-d"]; ok {
		t.Fatal("c-d config compiled without a role")
	}
	// c-b compile failure is isolated; the cell exists but has no engine.
	if c, ok := m.Get("c-b"); !ok || c.Role() != cell.RoleStandby {
		t.Fatalf("c-b role=%v", c.Role())
	} else if c.Engine() != nil {
		t.Fatal("broken cluster c-b got an engine")
	}
	if _, ok := rec.versions["c-b"]; ok {
		t.Fatal("broken cluster c-b reported success")
	}

	// Round 2: c-b demoted further (standby kept), c-a promoted from... c-a is
	// already primary; flip c-a to the other instance and promote c-b.
	cfg = innerapi.EppDataConfig{
		EppConfig: map[string]json.RawMessage{
			"c-b": json.RawMessage(`{"b":1}`),
		},
		Assignment: map[string]innerapi.AssignmentEntry{
			"c-a": entry("epp-2", "epp-1"), // we are standby now
			"c-b": entry("epp-1", ""),      // promoted to primary, single-instance group
		},
	}
	if err := w.Handle(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if c, _ := m.Get("c-a"); c.Role() != cell.RoleStandby {
		t.Fatalf("c-a role=%v, want standby after demote", c.Role())
	}
	if c, _ := m.Get("c-b"); c.Role() != cell.RolePrimary {
		t.Fatalf("c-b role=%v, want primary after promote", c.Role())
	}
	if rec.versions["c-b"] != "v-c-b" {
		t.Fatalf("c-b not compiled after promotion: %v", rec.versions)
	}

	// Round 3: all roles revoked -> every cell dropped and no report posted.
	if err := w.Handle(ctx, innerapi.EppDataConfig{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok := m.Get("c-a"); !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("c-a not dropped after role revoked")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n := o.posts.Load(); n != 0 {
		t.Fatalf("%d report POSTs observed, want 0", n)
	}
}

func TestEppDataHandleEnsureError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := cell.NewManager(ctx, cell.Options{
		Logger:       logr.Discard(),
		DrainTimeout: 50 * time.Millisecond,
		DrainWorkers: 1,
		NewCell:      func(ctx context.Context, key cell.Key) (*cell.Cell, error) { return nil, errors.New("create boom") },
	})
	w := NewEppDataWatcher(nil, "epp-1", m, logr.Discard())

	cfg := innerapi.EppDataConfig{
		Assignment: map[string]innerapi.AssignmentEntry{"c-a": {Primary: sp("epp-1")}},
	}
	err := w.Handle(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), "ensure cell c-a") {
		t.Fatalf("err=%v", err)
	}
}

// TestEppDataHandleNoAssignmentWarns exercises the "instance id matches no
// role" path: no cells are created and the handler reports no error.
func TestEppDataHandleNoAssignmentWarns(t *testing.T) {
	m := newTestManager(t, stubCompileOK("v1"))
	w := NewEppDataWatcher(nil, "epp-ghost", m, logr.Discard())

	cfg := innerapi.EppDataConfig{
		EppConfig: map[string]json.RawMessage{"c-a": json.RawMessage(`{"a":1}`)},
		Assignment: map[string]innerapi.AssignmentEntry{
			"c-a": {Primary: sp("epp-1"), Standby: sp("epp-2")},
		},
	}
	if err := w.Handle(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if len(m.List()) != 0 {
		t.Fatalf("cells=%v, want none for an unmatched instance", m.List())
	}
}

// compileRecorder fails on configs containing "boom" and records every other
// compile by cluster.
type compileRecorder struct {
	versions map[cell.Key]string
}

func (r *compileRecorder) compile(_ context.Context, key cell.Key, raw json.RawMessage, c *cell.Cell) (*cell.Engine, error) {
	if bytes.Contains(raw, []byte("boom")) {
		return nil, fmt.Errorf("compile boom")
	}
	version := "v-" + string(key)
	r.versions[key] = version
	return &cell.Engine{Version: version}, nil
}
