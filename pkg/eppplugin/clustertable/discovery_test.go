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
	"testing"
	"time"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

type recordingNotifier struct {
	upserts []fwkdl.EndpointMetadata
	deletes []k8stypes.NamespacedName
}

func (r *recordingNotifier) Upsert(m *fwkdl.EndpointMetadata) { r.upserts = append(r.upserts, *m) }
func (r *recordingNotifier) Delete(id k8stypes.NamespacedName) {
	r.deletes = append(r.deletes, id)
}

func ep(cluster, name, addr, port string) fwkdl.EndpointMetadata {
	return fwkdl.EndpointMetadata{
		ID:      k8stypes.NamespacedName{Namespace: cluster, Name: name},
		Name:    name,
		Address: addr,
		Port:    port,
	}
}

func newTestDiscovery(cluster string) *discovery {
	return &discovery{
		clusterName: cluster,
		applied:     map[k8stypes.NamespacedName]*fwkdl.EndpointMetadata{},
		ready:       make(chan struct{}),
	}
}

func TestApplyDiff(t *testing.T) {
	d := newTestDiscovery("c1")
	n := &recordingNotifier{}

	// Initial apply: two endpoints.
	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000"), ep("c1", "b", "10.0.0.2", "8000")})
	if len(n.upserts) != 2 || len(n.deletes) != 0 {
		t.Fatalf("initial: upserts=%d deletes=%d", len(n.upserts), len(n.deletes))
	}

	// No change: nothing emitted.
	n.upserts, n.deletes = nil, nil
	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000"), ep("c1", "b", "10.0.0.2", "8000")})
	if len(n.upserts) != 0 || len(n.deletes) != 0 {
		t.Fatalf("unchanged: upserts=%d deletes=%d", len(n.upserts), len(n.deletes))
	}

	// Change a port and drop b: delete is emitted before the upsert.
	n.upserts, n.deletes = nil, nil
	d.apply(n, []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "9000")})
	if len(n.deletes) != 1 || n.deletes[0].Name != "b" {
		t.Fatalf("deletes=%v", n.deletes)
	}
	if len(n.upserts) != 1 || n.upserts[0].Port != "9000" {
		t.Fatalf("upserts=%v", n.upserts)
	}
}

func TestStartAppliesHubUpdates(t *testing.T) {
	hub := NewHub()
	d := newTestDiscovery("c1")
	d.hub = hub
	n := &recordingNotifier{}

	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{
		"c1": {ep("c1", "a", "10.0.0.1", "8000")},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- d.Start(ctx, n) }()

	select {
	case <-d.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("discovery not ready")
	}
	if len(n.upserts) != 1 {
		t.Fatalf("upserts=%d", len(n.upserts))
	}

	// Push a new hub version: the drained endpoint must be deleted.
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"c1": {}})
	select {
	case <-done:
		t.Fatal("Start returned before context cancel")
	case <-time.After(100 * time.Millisecond):
	}
	if len(n.deletes) != 1 {
		t.Fatalf("deletes=%v", n.deletes)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

func TestHubWaitChange(t *testing.T) {
	hub := NewHub()
	_, v0 := hub.Snapshot("x")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"x": {}})
	}()
	if _, err := hub.WaitChange(ctx, v0); err != nil {
		t.Fatal(err)
	}
}
