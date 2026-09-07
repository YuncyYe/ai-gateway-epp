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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

// scriptedSource is a thread-safe Source whose behavior the test can flip
// while the polling loop is running.
type scriptedSource struct {
	mu      sync.Mutex
	changed bool
	version string
	data    string
	err     error
}

func (s *scriptedSource) Fetch(ctx context.Context, version string) (bool, string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changed, s.version, s.data, s.err
}

func (s *scriptedSource) set(changed bool, version, data string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.changed, s.version, s.data, s.err = changed, version, data, err
}

func TestPollerNoChangeSkipsHandle(t *testing.T) {
	src := &scriptedSource{changed: false, version: "v1"}
	var handled atomic.Int32
	p := New("test-nochange", src, func(ctx context.Context, data string) error {
		handled.Add(1)
		return nil
	}, Options{Interval: 10 * time.Millisecond, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	time.Sleep(150 * time.Millisecond)
	if n := handled.Load(); n != 0 {
		t.Fatalf("handle called %d times despite changed=false", n)
	}
	if p.Version() != "" {
		t.Fatalf("version advanced to %q despite changed=false", p.Version())
	}
	cancel()
	<-done
}

func TestPollerRecoversAfterFetchError(t *testing.T) {
	src := &scriptedSource{err: errors.New("fetch boom")}
	p := New("test-recover", src, func(ctx context.Context, data string) error { return nil },
		Options{Interval: 10 * time.Millisecond, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	// Let at least one failing round happen, then heal the source. The
	// backoff after a failure is 1s, so allow for that.
	time.Sleep(50 * time.Millisecond)
	src.set(true, "v1", "d1", nil)

	select {
	case <-p.FirstSync():
	case <-time.After(3 * time.Second):
		t.Fatal("first sync did not happen after recovery")
	}
	if p.Version() != "v1" {
		t.Fatalf("version=%q", p.Version())
	}
	cancel()
	<-done
}

func TestPollerRetriesFailedHandle(t *testing.T) {
	src := &scriptedSource{changed: true, version: "v1", data: "d1"}
	var failures atomic.Int32
	p := New("test-retry", src, func(ctx context.Context, data string) error {
		if failures.Add(1) <= 2 {
			return errors.New("apply boom")
		}
		return nil
	}, Options{Interval: 5 * time.Millisecond, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	select {
	case <-p.FirstSync():
	case <-time.After(2 * time.Second):
		t.Fatal("first sync did not happen despite handle recovering")
	}
	if p.Version() != "v1" {
		t.Fatalf("version=%q", p.Version())
	}
	cancel()
	<-done
}

func TestPollerStartWithCanceledContext(t *testing.T) {
	src := &scriptedSource{changed: true, version: "v1"}
	p := New("test-canceled", src, func(ctx context.Context, data string) error { return nil },
		Options{Interval: time.Hour, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start did not return on canceled context")
	}
}

func TestPollerConcurrentInstances(t *testing.T) {
	const n = 4
	cancels := make([]context.CancelFunc, 0, n)
	dones := make([]chan error, 0, n)
	for i := 0; i < n; i++ {
		src := &scriptedSource{changed: true, version: "v1"}
		p := New("race", src, func(ctx context.Context, data string) error { return nil },
			Options{Interval: 5 * time.Millisecond, Timeout: time.Second, Logger: logr.Discard()})
		ctx, cancel := context.WithCancel(context.Background())
		cancels = append(cancels, cancel)
		done := make(chan error, 1)
		dones = append(dones, done)
		go func() { done <- p.Start(ctx) }()
		select {
		case <-p.FirstSync():
		case <-time.After(2 * time.Second):
			t.Fatal("first sync did not happen")
		}
	}
	for _, cancel := range cancels {
		cancel()
	}
	for i, done := range dones {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("poller %d did not stop", i)
		}
	}
}
