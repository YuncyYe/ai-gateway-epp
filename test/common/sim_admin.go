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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// SimFakeMetrics selects which fake metrics to set on an inference-sim
// instance via POST /admin/config. Nil fields are left untouched; zero values
// are sent as-is (e.g. KVCacheUsage=0 clears a previously set value).
type SimFakeMetrics struct {
	// KVCacheUsage is vllm:kv_cache_usage_perc (0..1).
	KVCacheUsage *float64
	// WaitingRequests is vllm:num_requests_waiting.
	WaitingRequests *float64
	// RunningRequests is vllm:num_requests_running.
	RunningRequests *float64
}

// SetSimFakeMetrics applies fake metrics to a running sim instance ("ip:port"
// addr), so tests can drive epp scheduling with controlled backend state.
func SetSimFakeMetrics(t fatalT, addr string, m SimFakeMetrics) {
	t.Helper()
	fm := map[string]any{}
	if m.KVCacheUsage != nil {
		fm["kv-cache-usage"] = *m.KVCacheUsage
	}
	if m.WaitingRequests != nil {
		fm["waiting-requests"] = *m.WaitingRequests
	}
	if m.RunningRequests != nil {
		fm["running-requests"] = *m.RunningRequests
	}
	body, err := json.Marshal(map[string]any{"fake-metrics": fm})
	if err != nil {
		t.Fatalf("marshal fake metrics: %v", err)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post("http://"+addr+"/admin/config", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("set fake metrics on %s: %v", addr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("set fake metrics on %s: status %d: %s", addr, resp.StatusCode, b)
	}
}

// SimMetricValue scrapes a sim instance's /metrics and returns the value of
// the named vllm:* gauge (e.g. "vllm:kv_cache_usage_perc"), or 0 when absent.
func SimMetricValue(t fatalT, addr, name string) float64 {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape sim %s: %v", addr, err)
	}
	defer resp.Body.Close()
	text, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read sim metrics: %v", err)
	}
	prefix := name // line is `name{labels...} value` or `name value`
	for _, line := range splitLines(string(text)) {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		rest := line[len(prefix):]
		if strings.HasPrefix(rest, "{") {
			if i := strings.LastIndex(rest, "}"); i >= 0 {
				rest = rest[i+1:]
			} else {
				continue
			}
		}
		rest = strings.TrimSpace(rest)
		var v float64
		if _, err := fmt.Sscanf(rest, "%g", &v); err == nil {
			return v
		}
	}
	return 0
}
