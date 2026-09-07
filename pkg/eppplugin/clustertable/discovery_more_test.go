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

package clustertable

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// syncNotifier is a DiscoveryNotifier safe to read from the test goroutine
// while Start's goroutine writes; every Upsert/Delete also signals its
// channel (buffered, so the notifier never blocks).
type syncNotifier struct {
	mu       sync.Mutex
	upserts  []fwkdl.EndpointMetadata
	deletes  []k8stypes.NamespacedName
	upsertCh chan struct{}
	delCh    chan struct{}
}

func (s *syncNotifier) Upsert(m *fwkdl.EndpointMetadata) {
	s.mu.Lock()
	s.upserts = append(s.upserts, *m)
	s.mu.Unlock()
	s.upsertCh <- struct{}{}
}

func (s *syncNotifier) Delete(id k8stypes.NamespacedName) {
	s.mu.Lock()
	s.deletes = append(s.deletes, id)
	s.mu.Unlock()
	s.delCh <- struct{}{}
}

func (s *syncNotifier) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.deletes)
}

func TestNewFactoryWithClusterName(t *testing.T) {
	factory := NewFactory(NewHub())
	plugin, err := factory("my-name", json.NewDecoder(strings.NewReader(`{"clusterName":"c1"}`)), nil)
	if err != nil {
		t.Fatal(err)
	}
	tn := plugin.TypedName()
	if tn.Type != PluginType {
		t.Fatalf("type=%q, want %q", tn.Type, PluginType)
	}
	if tn.Name != "c1" {
		t.Fatalf("name=%q, want c1", tn.Name)
	}
}

func TestNewFactoryNilParameters(t *testing.T) {
	factory := NewFactory(NewHub())
	plugin, err := factory("my-name", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := plugin.TypedName().Name; got != "default" {
		t.Fatalf("name=%q, want default", got)
	}
}

func TestNewFactoryEmptyClusterName(t *testing.T) {
	factory := NewFactory(NewHub())
	plugin, err := factory("my-name", json.NewDecoder(strings.NewReader(`{}`)), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := plugin.TypedName().Name; got != "default" {
		t.Fatalf("name=%q, want default", got)
	}
}

func TestNewFactoryInvalidJSON(t *testing.T) {
	factory := NewFactory(NewHub())
	if _, err := factory("my-name", json.NewDecoder(strings.NewReader(`{invalid`)), nil); err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestTypedName(t *testing.T) {
	tests := []struct {
		name     string
		cluster  string
		wantName string
	}{
		{"cluster name wins", "c1", "c1"},
		{"empty cluster becomes default", "", "default"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDiscovery(tt.cluster)
			tn := d.TypedName()
			if tn.Type != PluginType || tn.Name != tt.wantName {
				t.Fatalf("TypedName=%+v, want type=%q name=%q", tn, PluginType, tt.wantName)
			}
		})
	}
}

func TestApplyEmptyDesiredDeletesAll(t *testing.T) {
	d := newTestDiscovery("c1")
	n := &recordingNotifier{}

	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000"), ep("c1", "b", "10.0.0.2", "8000")})
	n.upserts, n.deletes = nil, nil

	d.apply(n, nil)
	if len(n.upserts) != 0 {
		t.Fatalf("upserts=%v", n.upserts)
	}
	if len(n.deletes) != 2 {
		t.Fatalf("deletes=%v", n.deletes)
	}
	if len(d.applied) != 0 {
		t.Fatalf("applied=%v", d.applied)
	}
}

func TestApplyAdditions(t *testing.T) {
	d := newTestDiscovery("c1")
	n := &recordingNotifier{}

	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000")})
	n.upserts, n.deletes = nil, nil

	// Add b while keeping a unchanged: only b is upserted.
	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000"), ep("c1", "b", "10.0.0.2", "8000")})
	if len(n.deletes) != 0 {
		t.Fatalf("deletes=%v", n.deletes)
	}
	if len(n.upserts) != 1 || n.upserts[0].ID.Name != "b" {
		t.Fatalf("upserts=%v", n.upserts)
	}
}

func TestApplyMetadataChangeTriggersUpsert(t *testing.T) {
	d := newTestDiscovery("c1")
	n := &recordingNotifier{}

	initial := ep("c1", "a", "10.0.0.1", "8000")
	d.apply(n, []fwkdl.EndpointMetadata{initial})
	n.upserts, n.deletes = nil, nil

	tests := []struct {
		name   string
		mutate func(*fwkdl.EndpointMetadata)
	}{
		{"address", func(m *fwkdl.EndpointMetadata) { m.Address = "10.0.0.9" }},
		{"metrics host", func(m *fwkdl.EndpointMetadata) { m.MetricsHost = "10.0.0.1:9001" }},
		{"node address", func(m *fwkdl.EndpointMetadata) { m.NodeAddress = "192.168.0.1" }},
		{"rank index", func(m *fwkdl.EndpointMetadata) { m.RankIndex = 1 }},
		{"labels", func(m *fwkdl.EndpointMetadata) { m.Labels = map[string]string{"k": "v"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed := initial
			tt.mutate(&changed)
			d.apply(n, []fwkdl.EndpointMetadata{changed})
			if len(n.upserts) != 1 {
				t.Fatalf("upserts=%v", n.upserts)
			}
			// Re-apply the changed version: must be a no-op.
			n.upserts, n.deletes = nil, nil
			d.apply(n, []fwkdl.EndpointMetadata{changed})
			if len(n.upserts) != 0 || len(n.deletes) != 0 {
				t.Fatalf("re-apply: upserts=%v deletes=%v", n.upserts, n.deletes)
			}
			initial = changed
		})
	}
}

func TestApplyDoesNotStoreDesiredSlicePointers(t *testing.T) {
	d := newTestDiscovery("c1")
	n := &recordingNotifier{}

	desired := []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000")}
	d.apply(n, desired)

	// Mutating the caller's slice must not corrupt the applied set.
	desired[0].Address = "1.2.3.4"
	desired[0].Labels = map[string]string{"injected": "true"}

	n.upserts, n.deletes = nil, nil
	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000")})
	if len(n.upserts) != 0 || len(n.deletes) != 0 {
		t.Fatalf("applied set aliased caller's slice: upserts=%v deletes=%v", n.upserts, n.deletes)
	}
}

func TestContainsID(t *testing.T) {
	eps := []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000")}
	if !containsID(eps, k8stypes.NamespacedName{Namespace: "c1", Name: "a"}) {
		t.Fatal("expected id to be found")
	}
	if containsID(eps, k8stypes.NamespacedName{Namespace: "c1", Name: "b"}) {
		t.Fatal("expected id to be absent")
	}
	if containsID(nil, k8stypes.NamespacedName{Namespace: "c1", Name: "a"}) {
		t.Fatal("expected nil slice to contain nothing")
	}
}

func TestStartEmptyHub(t *testing.T) {
	hub := NewHub()
	d := newTestDiscovery("c1")
	d.hub = hub
	n := &recordingNotifier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Start(ctx, n) }()

	// Ready must still close on an empty table.
	select {
	case <-d.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("discovery not ready on empty hub")
	}
	if len(n.upserts) != 0 || len(n.deletes) != 0 {
		t.Fatalf("upserts=%v deletes=%v", n.upserts, n.deletes)
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected ctx error from Start")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestStartPreCanceledContext(t *testing.T) {
	hub := NewHub()
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"c1": {ep("c1", "a", "10.0.0.1", "8000")}})
	d := newTestDiscovery("c1")
	d.hub = hub

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// Start applies the snapshot once, then returns the context error.
	if err := d.Start(ctx, &recordingNotifier{}); err == nil {
		t.Fatal("expected ctx error from Start with canceled context")
	}
}

func TestStartMultipleGenerationsShareHub(t *testing.T) {
	hub := NewHub()
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{
		"c1": {ep("c1", "a", "10.0.0.1", "8000")},
		"c2": {ep("c2", "b", "10.0.0.2", "8000")},
	})
	n1 := &recordingNotifier{}
	n2 := &recordingNotifier{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	d1 := newTestDiscovery("c1")
	d1.hub = hub
	d2 := newTestDiscovery("c2")
	d2.hub = hub
	go func() { done <- d1.Start(ctx, n1) }()
	go func() { done <- d2.Start(ctx, n2) }()

	for _, d := range []*discovery{d1, d2} {
		select {
		case <-d.Ready():
		case <-time.After(2 * time.Second):
			t.Fatal("discovery not ready")
		}
	}
	if len(n1.upserts) != 1 || n1.upserts[0].ID.Name != "a" {
		t.Fatalf("d1 upserts=%v", n1.upserts)
	}
	if len(n2.upserts) != 1 || n2.upserts[0].ID.Name != "b" {
		t.Fatalf("d2 upserts=%v", n2.upserts)
	}

	cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("Start did not return after cancel")
		}
	}
}

// TestStartConcurrentHubWrites runs a discovery against a hub that is being
// updated continuously; run with -race to validate the notifier sequencing.
func TestStartConcurrentHubWrites(t *testing.T) {
	hub := NewHub()
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{
		"c1": {ep("c1", "a", "10.0.0.1", "8000")},
	})
	d := newTestDiscovery("c1")
	d.hub = hub
	n := &syncNotifier{
		upsertCh: make(chan struct{}, 100),
		delCh:    make(chan struct{}, 100),
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Start(ctx, n) }()
	select {
	case <-d.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("discovery not ready")
	}

	// The first apply is guaranteed to observe the pre-populated endpoint
	// (the hub already holds it before Start's initial Snapshot), so waiting
	// for this upsert makes "a is applied" a deterministic precondition
	// instead of racing against the writers below.
	select {
	case <-n.upsertCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for initial upsert")
	}

	// Hammer the hub with concurrent full-table replacements carrying the
	// same endpoint, then drain it. Deletes-before-upserts ordering is
	// validated by waiting for the delete signal.
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{
					"c1": {ep("c1", "a", "10.0.0.1", "8000")},
				})
			}
		}()
	}
	wg.Wait()
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"c1": {}})

	select {
	case <-n.delCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for delete")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Start did not return after cancel")
	}

	// The notifier is only ever called from Start's single goroutine, so the
	// delete count is deterministic once Start has returned.
	if got := n.deleteCount(); got != 1 {
		t.Fatalf("deletes=%d, want 1", got)
	}
}
