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
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/assignment"
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

func TestAssignmentFetchChanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != AssignmentPath {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("instance"); got != "inst 1" {
			t.Errorf("instance param = %q", got)
		}
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v3","Config":{"c-a":"primary","c-b":"standby"}}}`))
	}))
	defer srv.Close()

	w := NewAssignmentWatcher(innerapi.NewClient(srv.URL, "", time.Second), "inst 1", nil)
	changed, ver, view, err := w.Fetch(context.Background(), "v2")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "v3" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
	if view["c-a"] != assignment.RolePrimary || view["c-b"] != assignment.RoleStandby {
		t.Fatalf("view=%v", view)
	}
}

func TestAssignmentFetchUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":null}`))
	}))
	defer srv.Close()

	w := NewAssignmentWatcher(innerapi.NewClient(srv.URL, "", time.Second), "epp-1", nil)
	changed, ver, view, err := w.Fetch(context.Background(), "v2")
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("changed despite Data=null")
	}
	if ver != "v2" {
		t.Fatalf("ver=%q, want echoed client version", ver)
	}
	if view != nil {
		t.Fatalf("view=%v, want nil", view)
	}
}

func TestAssignmentFetchNilViewBecomesEmpty(t *testing.T) {
	// Config is JSON null but a new Version is present: the watcher must
	// still hand the poller a non-nil (empty) view.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v9","Config":null}}`))
	}))
	defer srv.Close()

	w := NewAssignmentWatcher(innerapi.NewClient(srv.URL, "", time.Second), "epp-1", nil)
	changed, ver, view, err := w.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "v9" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
	if view == nil || len(view) != 0 {
		t.Fatalf("view=%v, want non-nil empty map", view)
	}
}

func TestAssignmentFetchServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":500,"ErrMsg":"boom"}`))
	}))
	defer srv.Close()

	w := NewAssignmentWatcher(innerapi.NewClient(srv.URL, "", time.Second), "epp-1", nil)
	if _, _, _, err := w.Fetch(context.Background(), ""); err == nil {
		t.Fatal("expected error")
	}
}

// reportServer returns a handler that serves the given assignment view on GET
// and captures the posted ReadyReport on POST.
type reportServer struct {
	srv     *httptest.Server
	mu      chan assignment.ReadyReport
	postErr int
}

func newReportServer(t *testing.T, view string) *reportServer {
	t.Helper()
	rs := &reportServer{mu: make(chan assignment.ReadyReport, 8)}
	rs.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == AssignmentPath:
			w.Write([]byte(view))
		case r.Method == http.MethodPost && r.URL.Path == ReportPath:
			if rs.postErr != 0 {
				w.WriteHeader(rs.postErr)
				return
			}
			var rep assignment.ReadyReport
			if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
				t.Errorf("decode report: %v", err)
			}
			rs.mu <- rep
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(rs.srv.Close)
	return rs
}

func (rs *reportServer) reports() *assignment.ReadyReport {
	select {
	case rep := <-rs.mu:
		return &rep
	case <-time.After(2 * time.Second):
		return nil
	}
}

func findCellStatus(rep *assignment.ReadyReport, key string) (assignment.CellStatus, bool) {
	for _, cs := range rep.Cells {
		if cs.Key == key {
			return cs, true
		}
	}
	return assignment.CellStatus{}, false
}

func TestAssignmentHandleAppliesRolesAndReports(t *testing.T) {
	rs := newReportServer(t, `{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v1","Config":{"c-a":"primary"}}}`)
	m := newTestManager(t, stubCompileOK("v1"))
	w := NewAssignmentWatcher(innerapi.NewClient(rs.srv.URL, "", time.Second), "epp-1", m)

	if err := w.Handle(context.Background(), assignment.View{"c-a": assignment.RolePrimary}); err != nil {
		t.Fatal(err)
	}

	c, ok := m.Get("c-a")
	if !ok {
		t.Fatal("cell c-a not created")
	}
	if c.Role() != cell.RolePrimary || c.State() != cell.StatePrimary {
		t.Fatalf("role=%v state=%v", c.Role(), c.State())
	}

	rep := rs.reports()
	if rep == nil {
		t.Fatal("no readiness report posted")
	}
	if rep.Instance != "epp-1" {
		t.Fatalf("instance=%q", rep.Instance)
	}
	cs, ok := findCellStatus(rep, "c-a")
	if !ok {
		t.Fatalf("c-a missing from report %+v", rep.Cells)
	}
	if cs.Role != "primary" || cs.State != "primary" {
		t.Fatalf("status=%+v", cs)
	}
	if cs.EngineVersion != "" || cs.ReadySince != "" {
		t.Fatalf("unexpected fields %+v", cs)
	}

	// Once the engine is compiled, the next report carries its version.
	if _, err := m.ApplyConfig(context.Background(), "c-a", json.RawMessage(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.Handle(context.Background(), assignment.View{"c-a": assignment.RolePrimary}); err != nil {
		t.Fatal(err)
	}
	rep = rs.reports()
	if rep == nil {
		t.Fatal("second report not posted")
	}
	cs, ok = findCellStatus(rep, "c-a")
	if !ok || cs.EngineVersion != "v1-c-a" {
		t.Fatalf("status=%+v", cs)
	}
}

func TestAssignmentHandleStandbyAndDrop(t *testing.T) {
	rs := newReportServer(t, `{"ErrNum":200,"ErrMsg":"ok","Data":null}`)
	m := newTestManager(t, stubCompileOK("v1"))
	w := NewAssignmentWatcher(innerapi.NewClient(rs.srv.URL, "", time.Second), "epp-1", m)
	ctx := context.Background()

	view := assignment.View{"c-a": assignment.RolePrimary, "c-b": assignment.RoleStandby, "c-c": "garbage-role"}
	if err := w.Handle(ctx, view); err != nil {
		t.Fatal(err)
	}
	if c, _ := m.Get("c-b"); c.Role() != cell.RoleStandby {
		t.Fatalf("c-b role=%v", c.Role())
	}
	// Unknown role strings degrade to standby rather than primary.
	if c, _ := m.Get("c-c"); c.Role() != cell.RoleStandby {
		t.Fatalf("c-c role=%v", c.Role())
	}

	// c-b and c-c lose their assignment and must be dropped.
	if err := w.Handle(ctx, assignment.View{"c-a": assignment.RolePrimary}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Get("c-b"); ok {
		t.Fatal("c-b not dropped")
	}
	if _, ok := m.Get("c-c"); ok {
		t.Fatal("c-c not dropped")
	}
	if _, ok := m.Get("c-a"); !ok {
		t.Fatal("c-a dropped unexpectedly")
	}
}

func TestAssignmentHandleReportFailureStillApplies(t *testing.T) {
	rs := newReportServer(t, `{"ErrNum":200,"ErrMsg":"ok","Data":null}`)
	rs.postErr = http.StatusInternalServerError
	m := newTestManager(t, stubCompileOK("v1"))
	w := NewAssignmentWatcher(innerapi.NewClient(rs.srv.URL, "", time.Second), "epp-1", m)

	err := w.Handle(context.Background(), assignment.View{"c-a": assignment.RolePrimary})
	if err == nil || !strings.Contains(err.Error(), "post readiness report") {
		t.Fatalf("err=%v", err)
	}
	// Roles are applied locally even when the report fails.
	if c, ok := m.Get("c-a"); !ok || c.Role() != cell.RolePrimary {
		t.Fatalf("role not applied: %v", c.Role())
	}
}

func TestAssignmentHandleEnsureError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := cell.NewManager(ctx, cell.Options{
		Logger:       logr.Discard(),
		DrainTimeout: 50 * time.Millisecond,
		DrainWorkers: 1,
		NewCell:      func(ctx context.Context, key cell.Key) (*cell.Cell, error) { return nil, errors.New("create boom") },
	})
	w := NewAssignmentWatcher(innerapi.NewClient("http://127.0.0.1:1", "", time.Second), "epp-1", m)

	err := w.Handle(context.Background(), assignment.View{"c-a": assignment.RolePrimary})
	if err == nil || !strings.Contains(err.Error(), "ensure cell c-a") {
		t.Fatalf("err=%v", err)
	}
}
