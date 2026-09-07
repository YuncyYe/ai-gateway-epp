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
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"
)

// FetchMetrics GETs the Prometheus text exposition from an epp /metrics port.
func FetchMetrics(t fatalT, metricsAddr string) string {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get("http://" + metricsAddr + "/metrics")
	if err != nil {
		t.Fatalf("fetch metrics: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read metrics: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics status %d: %s", resp.StatusCode, body)
	}
	return string(body)
}

var engineVersionRE = regexp.MustCompile(`ai_epp_engine_current_version\{cluster="([^"]+)",version="([^"]+)"\} 1`)

// EngineVersion returns the active engine config hash for a cluster parsed
// from /metrics text, or "" when the cluster has no compiled engine.
func EngineVersion(metricsText, cluster string) string {
	for _, m := range engineVersionRE.FindAllStringSubmatch(metricsText, -1) {
		if m[1] == cluster {
			return m[2]
		}
	}
	return ""
}

// WaitFor polls fn until it returns true or the timeout expires.
func WaitFor(t fatalT, timeout time.Duration, desc string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if fn() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", desc)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// MetricValue returns the value of an exact metric sample line prefix match,
// e.g. MetricValue(text, `ai_epp_engine_reloads_total{cluster="a",result="invalid"}`).
// Returns 0 when the sample is absent.
func MetricValue(metricsText, samplePrefix string) float64 {
	var v float64
	fmt.Sscanf(metricsLine(metricsText, samplePrefix), "%g", &v)
	return v
}

func metricsLine(text, prefix string) string {
	for _, line := range splitLines(text) {
		if len(line) > len(prefix) && line[:len(prefix)] == prefix && line[len(prefix)] == ' ' {
			return line[len(prefix)+1:]
		}
	}
	return ""
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
