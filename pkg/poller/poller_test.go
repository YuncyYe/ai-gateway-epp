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
	"testing"
	"time"

	"github.com/go-logr/logr"
)

type fakeSource struct {
	changed bool
	version string
	data    string
	err     error
}

func (f *fakeSource) Fetch(ctx context.Context, version string) (bool, string, string, error) {
	return f.changed, f.version, f.data, f.err
}

func TestPollerAdvancesVersionOnSuccess(t *testing.T) {
	src := &fakeSource{changed: true, version: "v1", data: "d1"}
	p := New("test", src, func(ctx context.Context, data string) error { return nil },
		Options{Interval: 10 * time.Millisecond, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	select {
	case <-p.FirstSync():
	case <-time.After(2 * time.Second):
		t.Fatal("first sync did not happen")
	}
	if p.Version() != "v1" {
		t.Fatalf("version=%q", p.Version())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poller did not stop")
	}
}

func TestPollerHandleErrorKeepsVersion(t *testing.T) {
	src := &fakeSource{changed: true, version: "v1", data: "d1"}
	handleErr := errors.New("apply boom")
	p := New("test", src, func(ctx context.Context, data string) error { return handleErr },
		Options{Interval: 10 * time.Millisecond, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	// Version must not advance even though fetch succeeded.
	time.Sleep(100 * time.Millisecond)
	if p.Version() != "" {
		t.Fatalf("version advanced to %q despite handle error", p.Version())
	}
	// Fail-static: the source keeps getting asked with the same version.
	cancel()
	<-done
}

func TestPollerFetchErrorBackoff(t *testing.T) {
	src := &fakeSource{err: errors.New("fetch boom")}
	p := New("test", src, func(ctx context.Context, data string) error { return nil },
		Options{Interval: time.Hour, Timeout: time.Second, Logger: logr.Discard()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.Start(ctx) }()

	select {
	case <-p.FirstSync():
		t.Fatal("first sync despite fetch errors")
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-done
}
