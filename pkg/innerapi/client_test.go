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

package innerapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestGetChanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Token secret" {
			t.Errorf("auth header = %q", got)
		}
		if got := r.URL.Query().Get("version"); got != "v1" {
			t.Errorf("version param = %q", got)
		}
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"success","Data":{"Version":"v2","Config":{"a":1}},"WorkMode":"ModeNormal"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "secret", time.Second)
	var out map[string]int
	changed, ver, err := c.Get(context.Background(), "/x", "v1", &out)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "v2" || out["a"] != 1 {
		t.Fatalf("changed=%v ver=%q out=%v", changed, ver, out)
	}
}

func TestGetUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"success","Data":null,"WorkMode":"ModeNormal"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	changed, ver, err := c.Get(context.Background(), "/x", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if changed || ver != "v1" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
}

func TestGetRawMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"9","Config":{"k":"v"}}}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	var raw json.RawMessage
	changed, ver, err := c.Get(context.Background(), "/x", "", &raw)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || ver != "9" || string(raw) != `{"k":"v"}` {
		t.Fatalf("changed=%v ver=%q raw=%s", changed, ver, raw)
	}
}

func TestGetErrorNum(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":500,"ErrMsg":"boom"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewClientDefaults(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		c := NewClient("http://api-server:8181/", "tok", timeout)
		if c.hc.Timeout != 3*time.Second {
			t.Fatalf("timeout %v: hc.Timeout = %v", timeout, c.hc.Timeout)
		}
	}
	c := NewClient("http://api-server:8181/", "tok", time.Second)
	if c.baseURL != "http://api-server:8181" {
		t.Fatalf("baseURL = %q", c.baseURL)
	}
	if c.token != "tok" {
		t.Fatalf("token = %q", c.token)
	}
}

func TestGetNonOKStatus(t *testing.T) {
	body := strings.Repeat("x", 300)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(body))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("err = %v", err)
	}
	// body is truncated to 200 bytes plus "..."
	if !strings.Contains(err.Error(), strings.Repeat("x", 200)+"...") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetBadEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{not json`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil || !strings.Contains(err.Error(), "innerapi decode /x") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetBadData(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":123}}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil || !strings.Contains(err.Error(), "innerapi decode data /x") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetConfigDecodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v1","Config":"oops"}}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	var out map[string]int
	_, _, err := c.Get(context.Background(), "/x", "", &out)
	if err == nil || !strings.Contains(err.Error(), "innerapi decode config /x") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetNoDataKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","WorkMode":"ModeNormal"}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	changed, ver, err := c.Get(context.Background(), "/x", "v9", nil)
	if err != nil {
		t.Fatal(err)
	}
	if changed || ver != "v9" {
		t.Fatalf("changed=%v ver=%q", changed, ver)
	}
}

func TestGetVersionParamWithQueryInPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("foo"); got != "bar" {
			t.Errorf("foo param = %q", got)
		}
		if got := r.URL.Query().Get("version"); got != "v1" {
			t.Errorf("version param = %q", got)
		}
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":null}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	changed, _, err := c.Get(context.Background(), "/x?foo=bar", "v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("changed should be false")
	}
}

func TestGetReadError(t *testing.T) {
	// Respond with a Content-Length larger than the body actually sent, then
	// drop the connection so the client-side body read fails.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hj, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("response writer does not support hijacking")
		}
		conn, bufrw, err := hj.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		fmt.Fprintf(bufrw, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
		bufrw.Flush()
		conn.Close()
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil || !strings.Contains(err.Error(), "innerapi read /x") {
		t.Fatalf("err = %v", err)
	}
}

func TestGetRequestError(t *testing.T) {
	c := NewClient("http://exa mple", "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetDoError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close() // all requests fail with connection refused

	c := NewClient(srv.URL, "", time.Second)
	_, _, err := c.Get(context.Background(), "/x", "", nil)
	if err == nil || !strings.Contains(err.Error(), "innerapi get /x") {
		t.Fatalf("err = %v", err)
	}
}

func TestTruncate(t *testing.T) {
	if got := truncate("abc", 3); got != "abc" {
		t.Fatalf("truncate(abc, 3) = %q", got)
	}
	if got := truncate("abc", 5); got != "abc" {
		t.Fatalf("truncate(abc, 5) = %q", got)
	}
	if got := truncate("abcdef", 3); got != "abc..." {
		t.Fatalf("truncate(abcdef, 3) = %q", got)
	}
}

func TestConcurrentGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"ErrNum":200,"ErrMsg":"ok","Data":{"Version":"v2","Config":{"a":1}}}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "secret", time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				var out map[string]int
				changed, ver, err := c.Get(context.Background(), "/x", "v1", &out)
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				if !changed || ver != "v2" || out["a"] != 1 {
					t.Errorf("changed=%v ver=%q out=%v", changed, ver, out)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestVersionedUnmarshal(t *testing.T) {
	var v Versioned[map[string]int]
	if err := json.Unmarshal([]byte(`{"Version":"v3","Config":{"b":2}}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.Version != "v3" || v.Config["b"] != 2 {
		t.Fatalf("v = %+v", v)
	}
}
