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
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/eppplugin/clustertable"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

func TestUnwrapIPv6(t *testing.T) {
	tests := []struct {
		name string
		addr string
		want string
	}{
		{"bracketed ipv6", "[fd00::1]", "fd00::1"},
		{"plain ipv6", "fd00::1", "fd00::1"},
		{"ipv4", "10.0.0.1", "10.0.0.1"},
		{"hostname", "backend.example.com", "backend.example.com"},
		{"too short to strip", "[]", "[]"},
		{"open bracket only", "[fd00::1", "[fd00::1"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unwrapIPv6(tt.addr); got != tt.want {
				t.Fatalf("unwrapIPv6(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestBackendID(t *testing.T) {
	tests := []struct {
		name string
		b    BackendConf
		want string
	}{
		{"name wins", BackendConf{Name: "b1", Addr: "10.0.0.1", Port: 8000}, "b1"},
		{"fallback to addr:port", BackendConf{Addr: "10.0.0.1", Port: 8000}, "10.0.0.1:8000"},
		{"fallback unwraps ipv6", BackendConf{Addr: "[fd00::1]", Port: 9000}, "[fd00::1]:9000"},
		{"zero value", BackendConf{}, ":0"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := backendID(tt.b); got != tt.want {
				t.Fatalf("backendID(%+v) = %q, want %q", tt.b, got, tt.want)
			}
		})
	}
}

func TestClusterDiscoveryFetchChanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != ClusterTablePath {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Write([]byte(`{
			"ErrNum":200,"ErrMsg":"ok",
			"Data":{"Version":"v7","Config":{
				"c1":{
					"sc1":[
						{"Name":"b1","Addr":"10.0.0.1","Port":8000,"Weight":1},
						{"Name":"drained","Addr":"10.0.0.9","Port":8000,"Weight":0}
					],
					"sc2":[
						{"Name":"","Addr":"[fd00::1]","Port":9000,"Weight":2}
					]
				},
				"c2":{}
			}}
		}`))
	}))
	defer srv.Close()

	d := NewClusterDiscovery(innerapi.NewClient(srv.URL, "", time.Second), nil, nil, nil)
	changed, ver, desired, err := d.Fetch(context.Background(), "v6")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "v7" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}

	eps, ok := desired["c2"]
	if !ok || len(eps) != 0 {
		t.Fatalf("c2 = %v, want present empty slice", desired["c2"])
	}

	eps = desired["c1"]
	if len(eps) != 2 {
		t.Fatalf("c1 endpoints = %v, want 2 (weight-0 drained)", eps)
	}
	byID := map[string]fwkdl.EndpointMetadata{}
	for _, ep := range eps {
		byID[ep.ID.String()] = ep
	}

	b1, ok := byID[k8stypes.NamespacedName{Namespace: "c1", Name: "b1"}.String()]
	if !ok {
		t.Fatalf("b1 missing: %v", byID)
	}
	if b1.Address != "10.0.0.1" || b1.Port != "8000" || b1.MetricsHost != "10.0.0.1:8000" {
		t.Fatalf("b1 = %+v", b1)
	}

	v6, ok := byID[k8stypes.NamespacedName{Namespace: "c1", Name: "[fd00::1]:9000"}.String()]
	if !ok {
		t.Fatalf("ipv6 backend missing: %v", byID)
	}
	if v6.Address != "fd00::1" || v6.Port != "9000" || v6.MetricsHost != "[fd00::1]:9000" {
		t.Fatalf("v6 = %+v", v6)
	}
}

func TestClusterDiscoveryFetchUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":null}`))
	}))
	defer srv.Close()

	d := NewClusterDiscovery(innerapi.NewClient(srv.URL, "", time.Second), nil, nil, nil)
	changed, ver, desired, err := d.Fetch(context.Background(), "v6")
	if err != nil {
		t.Fatal(err)
	}
	if changed || desired != nil || ver != "v6" {
		t.Fatalf("changed=%v ver=%q desired=%v", changed, ver, desired)
	}
}

func TestClusterDiscoveryFetchError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":500,"ErrMsg":"boom"}`))
	}))
	defer srv.Close()

	d := NewClusterDiscovery(innerapi.NewClient(srv.URL, "", time.Second), nil, nil, nil)
	if _, _, _, err := d.Fetch(context.Background(), ""); err == nil {
		t.Fatal("expected error")
	}
}

type fakeCounter struct{ n atomic.Int32 }

func (f *fakeCounter) Inc() { f.n.Add(1) }

func TestClusterDiscoveryHandleFiltersUnassigned(t *testing.T) {
	hub := clustertable.NewHub()
	var skipped fakeCounter
	assigned := func(cluster string) bool { return cluster == "c1" }
	d := NewClusterDiscovery(nil, hub, assigned, &skipped)

	ep := fwkdl.EndpointMetadata{
		ID:      k8stypes.NamespacedName{Namespace: "c1", Name: "b1"},
		Address: "10.0.0.1",
		Port:    "8000",
	}
	desired := map[string][]fwkdl.EndpointMetadata{
		"c1": {ep},
		"c2": {{ID: k8stypes.NamespacedName{Namespace: "c2", Name: "b2"}}},
	}
	if err := d.Handle(context.Background(), desired); err != nil {
		t.Fatal(err)
	}

	got, _ := hub.Snapshot("c1")
	if len(got) != 1 || got[0].ID.Name != "b1" {
		t.Fatalf("c1 = %v", got)
	}
	if got, _ := hub.Snapshot("c2"); got != nil {
		t.Fatalf("c2 = %v, want filtered out", got)
	}
	if skipped.n.Load() != 1 {
		t.Fatalf("skip counter = %d, want 1", skipped.n.Load())
	}
}

func TestClusterDiscoveryHandleNilFilterStoresAll(t *testing.T) {
	hub := clustertable.NewHub()
	d := NewClusterDiscovery(nil, hub, nil, nil)

	// Seed the hub, then push an empty table: missing clusters are cleared.
	desired := map[string][]fwkdl.EndpointMetadata{
		"c1": {{ID: k8stypes.NamespacedName{Namespace: "c1", Name: "b1"}}},
		"c2": {},
	}
	if err := d.Handle(context.Background(), desired); err != nil {
		t.Fatal(err)
	}
	if got, _ := hub.Snapshot("c1"); len(got) != 1 {
		t.Fatalf("c1 = %v", got)
	}
	if got, _ := hub.Snapshot("c2"); got == nil || len(got) != 0 {
		t.Fatalf("c2 = %v, want present empty slice", got)
	}

	if err := d.Handle(context.Background(), map[string][]fwkdl.EndpointMetadata{}); err != nil {
		t.Fatal(err)
	}
	if got, _ := hub.Snapshot("c1"); got != nil {
		t.Fatalf("c1 = %v, want cleared", got)
	}
	if got, _ := hub.Snapshot("c2"); got != nil {
		t.Fatalf("c2 = %v, want cleared", got)
	}
}
