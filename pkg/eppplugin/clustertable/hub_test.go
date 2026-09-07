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
	"sync"
	"testing"
	"time"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

func TestHubNewHubEmpty(t *testing.T) {
	hub := NewHub()
	eps, v := hub.Snapshot("missing")
	if eps != nil {
		t.Fatalf("expected nil endpoints, got %v", eps)
	}
	if v != 0 {
		t.Fatalf("expected version 0, got %d", v)
	}
}

func TestHubReplaceAllIncrementsVersion(t *testing.T) {
	hub := NewHub()
	want := []fwkdl.EndpointMetadata{ep("c1", "a", "10.0.0.1", "8000")}

	for i := 1; i <= 3; i++ {
		hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"c1": want})
		eps, v := hub.Snapshot("c1")
		if v != uint64(i) {
			t.Fatalf("iteration %d: version=%d", i, v)
		}
		if len(eps) != 1 || eps[0].ID.Name != "a" {
			t.Fatalf("iteration %d: eps=%v", i, eps)
		}
	}
}

func TestHubReplaceAllIsolationBetweenClusters(t *testing.T) {
	hub := NewHub()
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{
		"c1": {ep("c1", "a", "10.0.0.1", "8000")},
		"c2": {ep("c2", "b", "10.0.0.2", "8000")},
	})

	if eps, _ := hub.Snapshot("c1"); len(eps) != 1 || eps[0].ID.Name != "a" {
		t.Fatalf("c1: %v", eps)
	}
	if eps, _ := hub.Snapshot("c2"); len(eps) != 1 || eps[0].ID.Name != "b" {
		t.Fatalf("c2: %v", eps)
	}
	if eps, _ := hub.Snapshot("unknown"); eps != nil {
		t.Fatalf("unknown cluster should be nil, got %v", eps)
	}

	// ReplaceAll replaces the ENTIRE table: c2 disappears unless the caller
	// carries it over explicitly.
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"c1": {}})
	if eps, _ := hub.Snapshot("c2"); eps != nil {
		t.Fatalf("c2 should be gone after full-table replace, got %v", eps)
	}
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{
		"c1": {},
		"c2": {ep("c2", "b", "10.0.0.2", "8000")},
	})
	if eps, _ := hub.Snapshot("c2"); len(eps) != 1 {
		t.Fatalf("c2 after carried-over replace: %v", eps)
	}
}

func TestHubReplaceAllEmpty(t *testing.T) {
	hub := NewHub()
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"c1": {ep("c1", "a", "10.0.0.1", "8000")}})
	_, v1 := hub.Snapshot("c1")

	hub.ReplaceAll(nil)
	eps, v2 := hub.Snapshot("c1")
	if eps != nil {
		t.Fatalf("expected nil endpoints after empty replace, got %v", eps)
	}
	if v2 != v1+1 {
		t.Fatalf("version=%d, want %d", v2, v1+1)
	}
}

func TestHubWaitChangeAlreadyChanged(t *testing.T) {
	hub := NewHub()
	_, v0 := hub.Snapshot("x")
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"x": {}})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := hub.WaitChange(ctx, v0)
	if err != nil {
		t.Fatal(err)
	}
	if v == v0 {
		t.Fatalf("version did not advance: %d", v)
	}
}

func TestHubWaitChangeContextTimeout(t *testing.T) {
	hub := NewHub()
	_, v0 := hub.Snapshot("x")

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	v, err := hub.WaitChange(ctx, v0)
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
	if v != v0 {
		t.Fatalf("version=%d, want %d", v, v0)
	}
}

func TestHubWaitChangeCanceledContext(t *testing.T) {
	hub := NewHub()
	_, v0 := hub.Snapshot("x")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := hub.WaitChange(ctx, v0); err == nil {
		t.Fatal("expected error from canceled context")
	}

	// An already-canceled context returns immediately even if the version
	// matches.
	_, v1 := hub.Snapshot("x")
	if v1 != v0 {
		t.Fatalf("version changed without ReplaceAll: %d -> %d", v0, v1)
	}
}

func TestHubWaitChangeMultipleWatchers(t *testing.T) {
	hub := NewHub()
	_, v0 := hub.Snapshot("x")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	const watchers = 5
	var wg sync.WaitGroup
	versions := make([]uint64, watchers)
	for i := 0; i < watchers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, err := hub.WaitChange(ctx, v0)
			if err != nil {
				t.Errorf("watcher %d: %v", i, err)
				return
			}
			versions[i] = v
		}(i)
	}

	// All watchers must observe the first version bump, not a later one.
	hub.ReplaceAll(map[string][]fwkdl.EndpointMetadata{"x": {}})
	wg.Wait()
	for i, v := range versions {
		if v != v0+1 {
			t.Fatalf("watcher %d version=%d, want %d", i, v, v0+1)
		}
	}
}

// TestHubConcurrentAccess exercises ReplaceAll/Snapshot/WaitChange from many
// goroutines; run with -race to validate locking.
func TestHubConcurrentAccess(t *testing.T) {
	hub := NewHub()
	_, v0 := hub.Snapshot("c1")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
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
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				hub.Snapshot("c1")
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if _, err := hub.WaitChange(ctx, v0); err != nil {
					return
				}
			}
		}()
	}
	wg.Wait()
}
