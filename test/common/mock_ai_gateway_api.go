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

package common

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
)

// MockAPI is an in-process ai-gateway-api InnerAPI mock covering the four
// endpoints ai-gateway-epp consumes, with version increments and a failure
// switch for fail-static scenarios.
type MockAPI struct {
	srv *httptest.Server

	mu sync.Mutex

	assignment  map[string]string
	assignments map[string]map[string]string // per-instance views, keyed by ?instance=
	configs     map[string]json.RawMessage
	table       map[string]any // cluster -> subCluster -> []backend map
	fail        bool

	lastInstance string // instance query param of the current assignment request

	version int64

	reportsMu sync.Mutex
	reports   [][]byte

	// Request observation: per-path request count and last version query
	// param, for incremental-sync assertions.
	reqMu       sync.Mutex
	reqCounts   map[string]int
	lastVersion map[string]string
}

// NewMockAPI creates and starts the mock on a random port.
func NewMockAPI(t fatalT) *MockAPI {
	m := &MockAPI{
		reqCounts:   map[string]int{},
		lastVersion: map[string]string{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/configs/epp_data/assignment", m.handleVersioned(func() (any, int64) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if inst := m.lastInstance; inst != "" && m.assignments != nil {
			if view, ok := m.assignments[inst]; ok {
				return view, m.version
			}
		}
		return m.assignment, m.version
	}))
	mux.HandleFunc("/configs/epp_data/assignment/report", func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		r.Body.Read(body)
		m.reportsMu.Lock()
		m.reports = append(m.reports, body)
		m.reportsMu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/configs/epp_data/picker_config", m.handleVersioned(func() (any, int64) {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.configs, m.version
	}))
	mux.HandleFunc("/configs/gslb_data/cluster_table", m.handleVersioned(func() (any, int64) {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.table, m.version
	}))
	m.srv = httptest.NewServer(mux)
	return m
}

// Addr returns the mock base URL (http://127.0.0.1:port, no trailing path).
func (m *MockAPI) Addr() string { return m.srv.URL }

// Close stops the mock server.
func (m *MockAPI) Close() { m.srv.Close() }

// SetFail makes every versioned endpoint return 500.
func (m *MockAPI) SetFail(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fail = v
}

// SetAssignment replaces the assignment view and bumps the version.
func (m *MockAPI) SetAssignment(v map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignment = v
	m.version++
}

// SetConfigs replaces picker_config and bumps the version.
func (m *MockAPI) SetConfigs(v map[string]json.RawMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.configs = v
	m.version++
}

// SetClusterTable replaces the cluster table and bumps the version.
// Backends are []map[string]any entries: Name/Addr/Port/Weight.
func (m *MockAPI) SetClusterTable(v map[string]map[string][]map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	table := make(map[string]any, len(v))
	for cluster, subs := range v {
		subsOut := make(map[string]any, len(subs))
		for sub, backends := range subs {
			subsOut[sub] = backends
		}
		table[cluster] = subsOut
	}
	m.table = table
	m.version++
}

// Reports returns copies of the readiness reports received so far.
func (m *MockAPI) Reports() [][]byte {
	m.reportsMu.Lock()
	defer m.reportsMu.Unlock()
	out := make([][]byte, len(m.reports))
	copy(out, m.reports)
	return out
}

func (m *MockAPI) handleVersioned(get func() (any, int64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.reqMu.Lock()
		m.reqCounts[r.URL.Path]++
		m.lastVersion[r.URL.Path] = r.URL.Query().Get("version")
		m.reqMu.Unlock()

		m.mu.Lock()
		fail := m.fail
		if r.URL.Path == "/configs/epp_data/assignment" {
			m.lastInstance = r.URL.Query().Get("instance")
		}
		m.mu.Unlock()
		if fail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		cfg, version := get()
		resp := map[string]any{"ErrNum": 200, "ErrMsg": "success", "WorkMode": "ModeNormal"}
		if cfg == nil {
			resp["Data"] = nil
		} else {
			resp["Data"] = map[string]any{"Version": fmt.Sprintf("v%d", version), "Config": cfg}
		}
		raw, _ := json.Marshal(resp)
		w.Write(raw)
	}
}

// RequestCount returns how many versioned requests the mock has served for a
// path since startup.
func (m *MockAPI) RequestCount(path string) int {
	m.reqMu.Lock()
	defer m.reqMu.Unlock()
	return m.reqCounts[path]
}

// LastVersionQuery returns the last version query param a poller sent for a
// path ("" before the first request).
func (m *MockAPI) LastVersionQuery(path string) string {
	m.reqMu.Lock()
	defer m.reqMu.Unlock()
	return m.lastVersion[path]
}

// SetAssignments installs per-instance assignment views (keyed by the
// instance id epp sends as ?instance=) and bumps the version. Per-instance
// views take precedence over the global SetAssignment view.
func (m *MockAPI) SetAssignments(views map[string]map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignments = views
	m.version++
}

// Backend is a cluster_table backend entry.
type Backend struct {
	Name   string
	Addr   string
	Port   int
	Weight int
}

// BackendMap builds the mock's table shape for one cluster/subCluster.
func BackendMap(backends ...Backend) []map[string]any {
	out := make([]map[string]any, 0, len(backends))
	for _, b := range backends {
		out = append(out, map[string]any{
			"Name":   b.Name,
			"Addr":   b.Addr,
			"Port":   b.Port,
			"Weight": b.Weight,
		})
	}
	return out
}
