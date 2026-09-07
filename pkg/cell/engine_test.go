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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

func TestHashConfig(t *testing.T) {
	// Known SHA-256 vector (empty input) proves the encoding is plain sha256/hex.
	empty := sha256.Sum256(nil)
	if got, want := HashConfig(nil), hex.EncodeToString(empty[:]); got != want {
		t.Errorf("HashConfig(nil) = %q, want %q", got, want)
	}

	raw := json.RawMessage(`{"a":1}`)
	got := HashConfig(raw)
	if got == HashConfig(json.RawMessage(`{"a":2}`)) {
		t.Error("different configs must hash differently")
	}
	if again := HashConfig(raw); again != got {
		t.Errorf("HashConfig not deterministic: %q vs %q", got, again)
	}
	if len(got) != 64 {
		t.Errorf("HashConfig length = %d, want 64 hex chars", len(got))
	}
}

func TestEngineAccessorsOnZeroValue(t *testing.T) {
	e := &Engine{}
	if e.Streamer() != nil {
		t.Error("Streamer() on zero Engine should be nil")
	}
	if e.FC() != nil {
		t.Error("FC() on zero Engine should be nil")
	}
}

func TestEngineDrainWaitsForInflight(t *testing.T) {
	e := &Engine{}
	e.wg.Add(1)
	released := make(chan struct{})
	go func() {
		time.Sleep(50 * time.Millisecond)
		e.wg.Done()
		close(released)
	}()

	start := time.Now()
	e.drain(2 * time.Second)
	<-released
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("drain returned after %v, before in-flight request finished", elapsed)
	}
}

func TestEngineDrainTimeoutForceDrop(t *testing.T) {
	e := &Engine{}
	e.wg.Add(1) // never released: drain must give up after the timeout

	start := time.Now()
	e.drain(30 * time.Millisecond)
	if elapsed := time.Since(start); elapsed < 30*time.Millisecond {
		t.Errorf("drain returned after %v, want >= timeout", elapsed)
	}
	// Release the waiter goroutine leaked inside drain.
	e.wg.Done()
}

func TestEngineDrainNilCancel(t *testing.T) {
	e := &Engine{} // cancel is nil: drain must not panic
	done := make(chan struct{})
	go func() {
		e.drain(time.Second)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("drain with nil cancel blocked")
	}
}

func TestEngineDrainCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{ctx: ctx, cancel: cancel}
	e.drain(time.Second)
	select {
	case <-ctx.Done():
	default:
		t.Error("drain did not cancel the engine context")
	}
}

// fakeDiscovery is an in-process EndpointDiscovery: Start blocks until the
// context is cancelled, Ready() gates readiness.
type fakeDiscovery struct {
	ready     chan struct{}
	started   atomic.Bool
	notifierC chan fwkdl.DiscoveryNotifier
}

func (f *fakeDiscovery) TypedName() fwkplugin.TypedName {
	return fwkplugin.TypedName{Type: "discovery", Name: "fake"}
}

func (f *fakeDiscovery) Start(ctx context.Context, n fwkdl.DiscoveryNotifier) error {
	f.started.Store(true)
	if f.notifierC != nil {
		select {
		case f.notifierC <- n:
		default:
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeDiscovery) Ready() <-chan struct{} { return f.ready }

func TestEngineStartNoDiscovery(t *testing.T) {
	c := newTestCell(t, "cell-start-none")
	e := &Engine{}

	e.start(c, logr.Discard())

	select {
	case <-c.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("cell not ready with no discovery plugins")
	}
	if c.State() != StateStandby {
		t.Errorf("State() = %v, want StateStandby", c.State())
	}
}

func TestEngineStartWaitsForDiscovery(t *testing.T) {
	c := newTestCell(t, "cell-start-disc")
	ctx, cancel := context.WithCancel(context.Background())
	disc := &fakeDiscovery{ready: make(chan struct{})}
	e := &Engine{ctx: ctx, cancel: cancel, discovery: []fwkdl.EndpointDiscovery{disc}}

	e.start(c, logr.Discard())

	startDeadline := time.Now().Add(2 * time.Second)
	for !disc.started.Load() && time.Now().Before(startDeadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if !disc.started.Load() {
		t.Fatal("discovery plugin was not started")
	}
	select {
	case <-c.Ready():
		t.Fatal("cell ready before discovery sync")
	case <-time.After(100 * time.Millisecond):
	}

	close(disc.ready) // initial sync lands
	select {
	case <-c.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("cell not ready after discovery sync")
	}
	if c.State() != StateStandby {
		t.Errorf("State() = %v, want StateStandby", c.State())
	}
}

func TestEngineStartCancelledBeforeDiscoveryReady(t *testing.T) {
	c := newTestCell(t, "cell-start-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	disc := &fakeDiscovery{ready: make(chan struct{})}
	e := &Engine{ctx: ctx, cancel: cancel, discovery: []fwkdl.EndpointDiscovery{disc}}

	e.start(c, logr.Discard())
	cancel() // engine generation torn down before the initial sync

	select {
	case <-c.Ready():
		t.Fatal("cell became ready after engine context was cancelled")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestMakePodListFunc(t *testing.T) {
	pods := []fwkdl.Endpoint{
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
			ID: k8stypes.NamespacedName{Namespace: "ns-1", Name: "pod-1"},
		}, nil),
		fwkdl.NewEndpoint(&fwkdl.EndpointMetadata{
			ID: k8stypes.NamespacedName{Namespace: "ns-2", Name: "pod-2"},
		}, nil),
	}
	f := &fakeDatastore{pods: pods}

	names := makePodListFunc(f)()
	if len(names) != 2 {
		t.Fatalf("got %d names, want 2", len(names))
	}
	want := map[k8stypes.NamespacedName]bool{
		{Namespace: "ns-1", Name: "pod-1"}: true,
		{Namespace: "ns-2", Name: "pod-2"}: true,
	}
	for _, n := range names {
		if !want[n] {
			t.Errorf("unexpected pod %v", n)
		}
	}

	if got := makePodListFunc(&fakeDatastore{})(); len(got) != 0 {
		t.Errorf("empty datastore: got %d names, want 0", len(got))
	}
}
