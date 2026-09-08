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

// Package sc10: SC10 基于 vLLM 状态的差异化调度。
// 测试设计文档：test/测试设计文档/scenario-SC10-基于后端状态的差异化调度/
//
// 通过 inference-sim 的 POST /admin/config fake-metrics 直接控制后端
// 上报的 vLLM 指标（kv-cache-usage / waiting-requests），验证 epp 的
// scorer 消费实时指标并产生差异化调度：空闲端点被偏好，负载恢复后
// 调度回归均衡。
package sc10

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc10"}],"max_tokens":8}`

// singleScorerConfig builds a minimal picker config with exactly one scorer,
// so each TC isolates the metric the scorer consumes.
func singleScorerConfig(cluster, scorerName, scorerType string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": %q}},
    {"name": "scorer", "type": %q, "parameters": {}},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [{"pluginRef": "scorer"}, {"pluginRef": "max-score"}]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`, cluster, scorerType))
}

func f64(v float64) *float64 { return &v }

// allPick hits asserts that every pick in a burst lands on want. Returns true
// when a full burst of n picks all hit want.
func burstAll(t *testing.T, e *common.Env, want string, n int) bool {
	t.Helper()
	for i := 0; i < n; i++ {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
		if err != nil || ep != want {
			return false
		}
	}
	return true
}

// bothHit asserts over repeated bursts that both backends receive traffic.
func bothHit(t *testing.T, e *common.Env, a, b string) bool {
	t.Helper()
	hit := map[string]bool{}
	for i := 0; i < 4 && len(hit) < 2; i++ {
		for j := 0; j < 5; j++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err == nil {
				hit[ep] = true
			}
		}
	}
	return hit[a] && hit[b]
}

// TestTC01_KVCacheUtilizationBias: with only the kv-cache-utilization scorer
// active, a backend reporting high KV-cache usage (0.95) is starved while the
// idle backend serves everything.
func TestTC01_KVCacheUtilizationBias(t *testing.T) {
	e := common.NewEnv(t, "epp-sc10-tc01", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithEppConfigFn(func(c string) json.RawMessage {
		return singleScorerConfig(c, "kv-scorer", "kv-cache-utilization-scorer")
	}), common.WithSimFakeMetrics(map[string]string{"b0": "{}"}))
	defer e.Close(t)

	idle, loaded := e.ClusterSims["cluster-a"][0], e.ClusterSims["cluster-a"][1]

	// Baseline: both backends receive traffic (scorer sees zero usage everywhere).
	common.WaitFor(t, 20*time.Second, "both backends hit at baseline", func() bool {
		return bothHit(t, e, idle, loaded)
	})

	// Load b0: KV-cache usage 0.95, and verify the sim actually reports it.
	common.SetSimFakeMetrics(t, loaded, common.SimFakeMetrics{KVCacheUsage: f64(0.95)})
	common.WaitFor(t, 10*time.Second, "sim reports fake kv-cache usage", func() bool {
		return common.SimMetricValue(t, loaded, "vllm:kv_cache_usage_perc") > 0.9
	})

	common.WaitFor(t, 30*time.Second, "all picks starve the kv-loaded backend", func() bool {
		return burstAll(t, e, idle, 10)
	})
}

// TestTC02_QueueDepthBias: with only the queue scorer active, a backend
// reporting a non-zero waiting queue is starved.
func TestTC02_QueueDepthBias(t *testing.T) {
	e := common.NewEnv(t, "epp-sc10-tc02", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithEppConfigFn(func(c string) json.RawMessage {
		return singleScorerConfig(c, "queue-scorer", "queue-scorer")
	}), common.WithSimFakeMetrics(map[string]string{"b0": "{}"}))
	defer e.Close(t)

	idle, loaded := e.ClusterSims["cluster-a"][0], e.ClusterSims["cluster-a"][1]

	// Baseline: both backends receive traffic.
	common.WaitFor(t, 20*time.Second, "both backends hit at baseline", func() bool {
		return bothHit(t, e, idle, loaded)
	})

	// Load b0: 8 waiting requests, and verify the sim reports it.
	common.SetSimFakeMetrics(t, loaded, common.SimFakeMetrics{WaitingRequests: f64(8)})
	common.WaitFor(t, 10*time.Second, "sim reports fake waiting requests", func() bool {
		return common.SimMetricValue(t, loaded, "vllm:num_requests_waiting") >= 8
	})

	common.WaitFor(t, 30*time.Second, "all picks starve the queue-loaded backend", func() bool {
		return burstAll(t, e, idle, 10)
	})
}

// TestTC03_LoadClearsBackToBalanced: clearing the fake metrics restores
// balanced scheduling across both backends.
func TestTC03_LoadClearsBackToBalanced(t *testing.T) {
	e := common.NewEnv(t, "epp-sc10-tc03", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithEppConfigFn(func(c string) json.RawMessage {
		return singleScorerConfig(c, "kv-scorer", "kv-cache-utilization-scorer")
	}), common.WithSimFakeMetrics(map[string]string{"b0": "{}"}))
	defer e.Close(t)

	idle, loaded := e.ClusterSims["cluster-a"][0], e.ClusterSims["cluster-a"][1]

	common.SetSimFakeMetrics(t, loaded, common.SimFakeMetrics{KVCacheUsage: f64(0.95)})
	common.WaitFor(t, 30*time.Second, "loaded backend starved", func() bool {
		return burstAll(t, e, idle, 8)
	})

	// Load clears: usage back to 0, both backends serve again.
	common.SetSimFakeMetrics(t, loaded, common.SimFakeMetrics{KVCacheUsage: f64(0)})
	common.WaitFor(t, 30*time.Second, "balanced scheduling after load cleared", func() bool {
		return bothHit(t, e, idle, loaded)
	})
}
