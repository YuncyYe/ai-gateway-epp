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

// Package sc13: SC13 UtilizationFilter 调度。
// 测试设计文档：test/测试设计文档/scenario-SC13-UtilizationFilter调度/
//
// 验证 utilization-filter 的硬过滤语义：指标超阈值的端点被移出候选集
// （区别于 scorer 的软偏好）。全部端点被过滤时默认调度失败；
// fallbackOnEmpty=true 时回落到未过滤候选继续服务。
package sc13

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc13"}],"max_tokens":8}`

// utilFilterConfig: discovery + utilization-filter + max-score-picker, no
// scorer — the filter alone decides the candidate set.
func utilFilterConfig(cluster string, maxValue float64, fallback bool) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": %q}},
    {"name": "util-filter", "type": "utilization-filter", "parameters": {
      "conditions": [{"metric": "kv-cache-utilization", "maxValue": %g}],
      "fallbackOnEmpty": %v
    }},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [{"pluginRef": "util-filter"}, {"pluginRef": "max-score"}]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`, cluster, maxValue, fallback))
}

func f64(v float64) *float64 { return &v }

// TestTC01_OverloadedEndpointFiltered: an endpoint above the kv-cache
// utilization cap is removed from the candidate set — every pick lands on
// the idle endpoint; once its load clears it is a candidate again.
func TestTC01_OverloadedEndpointFiltered(t *testing.T) {
	e := common.NewEnv(t, "epp-sc13-tc01", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithEppConfigFn(func(c string) json.RawMessage {
		return utilFilterConfig(c, 0.9, false)
	}), common.WithSimFakeMetrics(map[string]string{"b0": "{}"}))
	defer e.Close(t)

	idle, loaded := e.ClusterSims["cluster-a"][0], e.ClusterSims["cluster-a"][1]

	// Baseline: both backends receive traffic.
	hit := map[string]bool{}
	common.WaitFor(t, 20*time.Second, "both backends hit at baseline", func() bool {
		for i := 0; i < 5 && len(hit) < 2; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err == nil {
				hit[ep] = true
			}
		}
		return len(hit) == 2
	})

	// b0 above the 0.9 cap: filtered out, not merely deprioritized.
	common.SetSimFakeMetrics(t, loaded, common.SimFakeMetrics{KVCacheUsage: f64(0.95)})
	common.WaitFor(t, 30*time.Second, "overloaded backend filtered out", func() bool {
		for i := 0; i < 5; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err != nil || ep != idle {
				return false
			}
		}
		return true
	})

	// Load clears: b0 is a candidate again.
	common.SetSimFakeMetrics(t, loaded, common.SimFakeMetrics{KVCacheUsage: f64(0)})
	hit = map[string]bool{}
	common.WaitFor(t, 30*time.Second, "both backends hit after load cleared", func() bool {
		for i := 0; i < 5 && len(hit) < 2; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err == nil {
				hit[ep] = true
			}
		}
		return len(hit) == 2
	})
}

// TestTC02_AllFilteredFailsClosed: when every endpoint exceeds the cap and
// fallbackOnEmpty is false (default), scheduling fails — the filter is a
// hard guard, not a preference.
func TestTC02_AllFilteredFailsClosed(t *testing.T) {
	e := common.NewEnv(t, "epp-sc13-tc02", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithEppConfigFn(func(c string) json.RawMessage {
		return utilFilterConfig(c, 0.9, false)
	}), common.WithSimFakeMetrics(map[string]string{"a0": "{}", "b0": "{}"}))
	defer e.Close(t)

	a0 := e.ClusterSims["cluster-a"][0]
	b0 := e.ClusterSims["cluster-a"][1]
	common.SetSimFakeMetrics(t, a0, common.SimFakeMetrics{KVCacheUsage: f64(0.95)})
	common.SetSimFakeMetrics(t, b0, common.SimFakeMetrics{KVCacheUsage: f64(0.99)})

	common.WaitFor(t, 30*time.Second, "scheduling fails when all endpoints filtered", func() bool {
		_, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err != nil
	})
	e.EPP.WaitHealth(t, "liveness", 5*time.Second)

	// One endpoint recovers: scheduling works again, pinned to it.
	common.SetSimFakeMetrics(t, a0, common.SimFakeMetrics{KVCacheUsage: f64(0)})
	common.WaitFor(t, 30*time.Second, "scheduling recovers with one healthy endpoint", func() bool {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err == nil && ep == a0
	})
}

// TestTC03_FallbackOnEmpty: with fallbackOnEmpty=true, an all-filtered
// candidate set falls back to the unfiltered list so requests keep flowing.
func TestTC03_FallbackOnEmpty(t *testing.T) {
	e := common.NewEnv(t, "epp-sc13-tc03", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithEppConfigFn(func(c string) json.RawMessage {
		return utilFilterConfig(c, 0.9, true)
	}), common.WithSimFakeMetrics(map[string]string{"a0": "{}", "b0": "{}"}))
	defer e.Close(t)

	a0 := e.ClusterSims["cluster-a"][0]
	b0 := e.ClusterSims["cluster-a"][1]
	common.SetSimFakeMetrics(t, a0, common.SimFakeMetrics{KVCacheUsage: f64(0.95)})
	common.SetSimFakeMetrics(t, b0, common.SimFakeMetrics{KVCacheUsage: f64(0.99)})

	valid := map[string]bool{a0: true, b0: true}
	common.WaitFor(t, 30*time.Second, "fallback keeps routing when all filtered", func() bool {
		for i := 0; i < 5; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
			if err != nil || !valid[ep] {
				return false
			}
		}
		return true
	})
}
