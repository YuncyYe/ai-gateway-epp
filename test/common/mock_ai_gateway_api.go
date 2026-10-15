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
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

// MockAPI is an in-process ai-gateway-api InnerAPI mock covering the two
// endpoints ai-gateway-epp consumes (epp_data/config with the cluster-to-group
// assignment, gslb_data/cluster_table), with version increments and a failure
// switch for fail-static scenarios. The retired assignment/report endpoint
// is kept as a recorder so tests can assert nothing is posted there.
type MockAPI struct {
	srv *httptest.Server

	mu sync.Mutex

	eppConfig  map[string]json.RawMessage
	assignment map[string]innerapi.AssignmentEntry // cluster -> {group}
	table      map[string]any                      // cluster -> subCluster -> []backend map
	fail       bool

	defaultGroup string // group name SetAssignment builds the view for

	version int64

	reportPosts atomic.Int64

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
	mux.HandleFunc("/configs/epp_data/config", m.handleVersioned(func() (any, int64) {
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.eppConfig == nil && m.assignment == nil {
			return nil, m.version
		}
		return innerapi.EppDataConfig{
			EppConfig:  m.eppConfig,
			Assignment: m.assignment,
		}, m.version
	}))
	mux.HandleFunc("/configs/epp_data/assignment/report", func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		m.reportPosts.Add(1)
		w.WriteHeader(http.StatusOK)
	})
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

// SetDefaultGroup records the group name SetAssignment assigns clusters to.
// NewEnv sets it to the env's group name.
func (m *MockAPI) SetDefaultGroup(group string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.defaultGroup = group
}

// SetAssignment replaces the assignment section: every listed cluster is
// assigned to the default group. Pass an empty map to assign nothing.
func (m *MockAPI) SetAssignment(clusters map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignment = fullView(m.defaultGroup, clusters)
	m.version++
}

// SetAssignmentView installs an assignment section (cluster -> {group})
// directly.
func (m *MockAPI) SetAssignmentView(view map[string]innerapi.AssignmentEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignment = view
	m.version++
}

// fullView expands a cluster set into the assignment section: every listed
// cluster is assigned to the given group.
func fullView(group string, clusters map[string]string) map[string]innerapi.AssignmentEntry {
	out := make(map[string]innerapi.AssignmentEntry, len(clusters))
	for cluster := range clusters {
		out[cluster] = innerapi.AssignmentEntry{Group: group}
	}
	return out
}

// SetEppConfig replaces the epp_config section and bumps the version.
func (m *MockAPI) SetEppConfig(v map[string]json.RawMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.eppConfig = v
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

// ReportCount returns how many POSTs the retired assignment/report endpoint
// has received; the readiness report was removed, so this must stay 0.
func (m *MockAPI) ReportCount() int64 { return m.reportPosts.Load() }

func (m *MockAPI) handleVersioned(get func() (any, int64)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.reqMu.Lock()
		m.reqCounts[r.URL.Path]++
		m.lastVersion[r.URL.Path] = r.URL.Query().Get("version")
		m.reqMu.Unlock()

		m.mu.Lock()
		fail := m.fail
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
