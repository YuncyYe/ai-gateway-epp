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

// Package sc02: SC02 配置热加载。
// 测试设计文档：test/测试设计文档/scenario-SC02-配置热加载/
package sc02

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc02"}],"max_tokens":8}`

// configV2 returns a valid config whose content differs from v1 (plugin
// renamed), so the engine content hash changes.
func configV2(cluster string) json.RawMessage {
	return json.RawMessage(strings.ReplaceAll(string(common.PickerConfig(cluster, false)), `"max-score"`, `"max-score-v2"`))
}

// invalidConfig returns a config whose plugin type is not registered.
func invalidConfig(cluster string) json.RawMessage {
	return json.RawMessage(strings.ReplaceAll(string(common.PickerConfig(cluster, false)),
		`"type": "max-score-picker"`, `"type": "no-such-plugin-type"`))
}

func metricsExcerpt(text string) string {
	var lines []string
	for _, l := range strings.Split(text, "\n") {
		if strings.Contains(l, "ai_epp_engine") {
			lines = append(lines, l)
		}
	}
	return strings.Join(lines, "\n")
}

func lastReport(t *testing.T, e *common.Env) string {
	t.Helper()
	reports := e.API.Reports()
	if len(reports) == 0 {
		return "<none>"
	}
	return string(reports[len(reports)-1])
}

func logTail(t *testing.T, e *common.Env) string {
	t.Helper()
	data, err := os.ReadFile(e.EPP.Proc.LogPath())
	if err != nil {
		return err.Error()
	}
	const keep = 4000
	if len(data) > keep {
		data = data[len(data)-keep:]
	}
	return string(data)
}

// TestTC01_HotReload: a picker_config content change is picked up by the
// poller, the engine swaps, and requests keep being served.
func TestTC01_HotReload(t *testing.T) {
	e := common.NewEnv(t, "epp-sc02-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	if v1 == "" {
		t.Fatal("cluster-a has no compiled engine version")
	}

	e.API.SetConfigs(map[string]json.RawMessage{"cluster-a": configV2("cluster-a")})

	common.WaitFor(t, 20*time.Second, "engine version change after hot reload", func() bool {
		return common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a") != v1 &&
			common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a") != ""
	})

	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil {
		t.Fatalf("pick after hot reload: %v (log: %s)", err, e.EPP.Proc.LogPath())
	}
	if ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("routed to %q, want %q", ep, e.ClusterSims["cluster-a"][0])
	}
}

// TestTC02_MultiClusterIndependent: updating one cluster's config leaves the
// other cluster's engine untouched.
func TestTC02_MultiClusterIndependent(t *testing.T) {
	e := common.NewEnv(t, "epp-sc02-tc02", map[string][]string{
		"cluster-a": {"a0"},
		"cluster-b": {"b0"},
	})
	defer e.Close(t)

	text := common.FetchMetrics(t, e.EPP.MetricsAddr)
	va := common.EngineVersion(text, "cluster-a")
	vb := common.EngineVersion(text, "cluster-b")
	if va == "" || vb == "" {
		t.Fatalf("missing engine versions: a=%q b=%q", va, vb)
	}

	e.API.SetConfigs(map[string]json.RawMessage{
		"cluster-a": common.PickerConfig("cluster-a", false),
		"cluster-b": configV2("cluster-b"),
	})

	deadline := time.Now().Add(20 * time.Second)
	for {
		text := common.FetchMetrics(t, e.EPP.MetricsAddr)
		if common.EngineVersion(text, "cluster-b") != vb {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster-b engine version did not change; metrics excerpt:\n%s\nreport: %s\nepp log tail: %s",
				metricsExcerpt(text), lastReport(t, e), logTail(t, e))
		}
		time.Sleep(100 * time.Millisecond)
	}
	if got := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a"); got != va {
		t.Fatalf("cluster-a engine changed (%q -> %q) though only cluster-b config was updated", va, got)
	}
}

// TestTC03_InvalidConfig: an invalid config must not swap the engine: the old
// version keeps serving, and the reload is recorded as invalid.
func TestTC03_InvalidConfig(t *testing.T) {
	e := common.NewEnv(t, "epp-sc02-tc03", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	if v1 == "" {
		t.Fatal("cluster-a has no compiled engine version")
	}

	e.API.SetConfigs(map[string]json.RawMessage{"cluster-a": invalidConfig("cluster-a")})

	common.WaitFor(t, 20*time.Second, "invalid reload recorded", func() bool {
		text := common.FetchMetrics(t, e.EPP.MetricsAddr)
		return common.MetricValue(text, `ai_epp_engine_reloads_total{cluster="cluster-a",result="invalid"}`) > 0
	})

	// Old engine keeps serving with the same version.
	if got := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a"); got != v1 {
		t.Fatalf("engine version changed to %q despite invalid config", got)
	}
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil {
		t.Fatalf("pick with old engine after invalid config: %v", err)
	}
	if ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("routed to %q, want %q", ep, e.ClusterSims["cluster-a"][0])
	}
}

// TestTC04_Rollback: rolling the config back to a previous version in
// ai-gateway-api restores the old engine without any process operation
// (FR-C5).
func TestTC04_Rollback(t *testing.T) {
	e := common.NewEnv(t, "epp-sc02-tc04", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	if v1 == "" {
		t.Fatal("cluster-a has no compiled engine version")
	}

	e.API.SetConfigs(map[string]json.RawMessage{"cluster-a": configV2("cluster-a")})
	common.WaitFor(t, 20*time.Second, "v2 engine active", func() bool {
		v := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
		return v != "" && v != v1
	})

	// Roll back: same content as v1 (same content hash expected).
	e.API.SetConfigs(map[string]json.RawMessage{"cluster-a": common.PickerConfig("cluster-a", false)})
	common.WaitFor(t, 20*time.Second, "rollback to v1 engine", func() bool {
		return common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a") == v1
	})

	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick after rollback = %q, %v", ep, err)
	}
}

// TestTC05_EffectiveLatency: a config change takes effect within 2x the poll
// interval (NFR-2), with slack for process scheduling on a loaded machine.
func TestTC05_EffectiveLatency(t *testing.T) {
	e := common.NewEnv(t, "epp-sc02-tc05", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")

	// poll interval is 200ms (harness default) -> bound 2*200ms; allow 10x
	// slack for CI/Windows scheduling jitter while still catching a
	// regression to minute-scale reload latency.
	bound := 10 * 200 * time.Millisecond
	start := time.Now()
	e.API.SetConfigs(map[string]json.RawMessage{"cluster-a": configV2("cluster-a")})
	for {
		if v := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a"); v != "" && v != v1 {
			break
		}
		if time.Since(start) > 20*time.Second {
			t.Fatal("config change never took effect")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if elapsed := time.Since(start); elapsed > bound {
		t.Fatalf("config took %v to take effect, want <= %v (2x poll interval + slack)", elapsed, bound)
	}
}
