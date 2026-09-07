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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

func TestConfigFetchChanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != PickerConfigPath {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v4","Config":{"c-a":{"x":1},"c-b":{"y":2}}}}`))
	}))
	defer srv.Close()

	p := NewConfigPoller(innerapi.NewClient(srv.URL, "", time.Second), nil)
	changed, ver, configs, err := p.Fetch(context.Background(), "v3")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "v4" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
	if string(configs["c-a"]) != `{"x":1}` || string(configs["c-b"]) != `{"y":2}` {
		t.Fatalf("configs=%v", configs)
	}
}

func TestConfigFetchUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":null}`))
	}))
	defer srv.Close()

	p := NewConfigPoller(innerapi.NewClient(srv.URL, "", time.Second), nil)
	changed, ver, configs, err := p.Fetch(context.Background(), "v3")
	if err != nil {
		t.Fatal(err)
	}
	if changed || configs != nil || ver != "v3" {
		t.Fatalf("changed=%v ver=%q configs=%v", changed, ver, configs)
	}
}

func TestConfigFetchError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer srv.Close()

	p := NewConfigPoller(innerapi.NewClient(srv.URL, "", time.Second), nil)
	if _, _, _, err := p.Fetch(context.Background(), ""); err == nil {
		t.Fatal("expected error")
	}
}

// compileRecorder fails on configs containing "boom" and records every other
// compile by cluster.
type compileRecorder struct {
	versions map[cell.Key]string
}

func (r *compileRecorder) compile(_ context.Context, key cell.Key, raw json.RawMessage, c *cell.Cell) (*cell.Engine, error) {
	if bytes.Contains(raw, []byte("boom")) {
		return nil, errors.New("compile boom")
	}
	version := "v-" + string(key)
	r.versions[key] = version
	return &cell.Engine{Version: version}, nil
}

func TestConfigHandleAppliesAssignedCells(t *testing.T) {
	rec := &compileRecorder{versions: map[cell.Key]string{}}
	m := newTestManager(t, rec.compile)
	ctx := context.Background()
	if _, err := m.Ensure(ctx, "c-a", cell.RoleStandby); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Ensure(ctx, "c-c", cell.RoleStandby); err != nil {
		t.Fatal(err)
	}

	p := NewConfigPoller(nil, m)
	configs := map[string]json.RawMessage{
		"c-a": json.RawMessage(`{"a":1}`),
		"c-b": json.RawMessage(`{"a":2}`),
		"c-c": json.RawMessage(`{"boom":true}`),
	}
	// Compile failures and unassigned clusters are isolated; Handle reports
	// no error and keeps applying the rest.
	if err := p.Handle(ctx, configs); err != nil {
		t.Fatal(err)
	}

	if got := rec.versions["c-a"]; got != "v-c-a" {
		t.Fatalf("c-a not compiled: %v", rec.versions)
	}
	if _, ok := rec.versions["c-b"]; ok {
		t.Fatal("ghost cluster c-b was compiled")
	}
	if _, ok := rec.versions["c-c"]; ok {
		t.Fatal("broken cluster c-c reported success")
	}
	c, _ := m.Get("c-a")
	if c.Engine() == nil || c.Engine().Version != "v-c-a" {
		t.Fatalf("engine=%v", c.Engine())
	}
	// Compile failure leaves the broken cell without an engine, but the
	// failure did not stop the other clusters from being applied above.
	bad, _ := m.Get("c-c")
	if bad.Engine() != nil {
		t.Fatal("broken cluster got an engine")
	}
}

func TestConfigHandleEmpty(t *testing.T) {
	rec := &compileRecorder{versions: map[cell.Key]string{}}
	m := newTestManager(t, rec.compile)
	p := NewConfigPoller(nil, m)
	if err := p.Handle(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(rec.versions) != 0 {
		t.Fatalf("compiles=%v", rec.versions)
	}
}
