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

// Package sc06: SC06 流控公平排队。
// 测试设计文档：test/测试设计文档/scenario-SC06-流控公平排队/
//
// 范围说明：EPP 的流控在请求调度周期内做准入/排队。集成测试从
// ext-proc 黑盒只能稳定观测到「流控配置生效（引擎编译、请求不被
// 拒绝、并发突发不死锁）」和「非法流控配置 fail-static」；排队时延
// 与公平性属于性能/压力维度，由基准测试覆盖，不在本场景断言。
package sc06

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc06"}],"max_tokens":8}`

// fcConfig is a flow-control-enabled picker config: utilization-detector
// saturation, single priority band with a tight request cap.
func fcConfig(cluster, ttl string, maxReq int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
  "featureGates": ["flowControl"],
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": %q}},
    {"name": "kv-scorer", "type": "kv-cache-utilization-scorer", "parameters": {}},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}},
    {"name": "util-detector", "type": "utilization-detector", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [{"pluginRef": "kv-scorer"}, {"pluginRef": "max-score"}]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "flowControl": {
    "defaultRequestTTL": %q,
    "saturationDetector": {"pluginRef": "util-detector"},
    "priorityBands": [{"priority": 1, "maxRequests": %d}]
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`, cluster, ttl, maxReq))
}

// enableFlowControl swaps the cluster's config for the flow-control config
// and waits for the new engine to compile.
func enableFlowControl(t *testing.T, e *common.Env, cluster string) {
	t.Helper()
	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), cluster)
	if v1 == "" {
		t.Fatal("no compiled engine before flow-control swap")
	}
	e.API.SetConfigs(map[string]json.RawMessage{cluster: fcConfig(cluster, "30s", 1)})
	common.WaitFor(t, 20*time.Second, "flow-control engine compiled", func() bool {
		return common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), cluster) != v1
	})
}

// TestTC01_FlowControlActive: with the flowControl gate enabled and a
// single-request band, a concurrent burst of picks all complete successfully
// (requests queue rather than deadlock or corrupt scheduling).
func TestTC01_FlowControlActive(t *testing.T) {
	e := common.NewEnv(t, "epp-sc06-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)
	enableFlowControl(t, e, "cluster-a")

	const burst = 8
	eps := make([]string, burst)
	errs := make([]error, burst)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < burst; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			eps[i], errs[i] = common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 30*time.Second)
		}(i)
	}
	close(start)
	wg.Wait()

	want := e.ClusterSims["cluster-a"][0]
	for i := 0; i < burst; i++ {
		if errs[i] != nil {
			t.Fatalf("burst pick %d: %v", i, errs[i])
		}
		if eps[i] != want {
			t.Fatalf("burst pick %d routed to %q, want %q", i, eps[i], want)
		}
	}
}

// TestTC02_InvalidFlowControlConfig: a flow-control section referencing a
// missing detector fails compilation; the previous engine keeps serving.
func TestTC02_InvalidFlowControlConfig(t *testing.T) {
	e := common.NewEnv(t, "epp-sc06-tc02", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)
	enableFlowControl(t, e, "cluster-a")

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")

	// Invalid config: the saturationDetector ref does not exist.
	invalid := json.RawMessage(fmt.Sprintf(`{
  "featureGates": ["flowControl"],
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": "cluster-a"}},
    {"name": "kv-scorer", "type": "kv-cache-utilization-scorer", "parameters": {}},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [{"pluginRef": "kv-scorer"}, {"pluginRef": "max-score"}]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "flowControl": {
    "defaultRequestTTL": "30s",
    "saturationDetector": {"pluginRef": "missing-detector"},
    "priorityBands": [{"priority": 1, "maxRequests": 1}]
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`))
	e.API.SetConfigs(map[string]json.RawMessage{"cluster-a": invalid})

	common.WaitFor(t, 20*time.Second, "invalid reload recorded", func() bool {
		text := common.FetchMetrics(t, e.EPP.MetricsAddr)
		return common.MetricValue(text, `ai_epp_engine_reloads_total{cluster="cluster-a",result="invalid"}`) > 0
	})
	if got := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a"); got != v1 {
		t.Fatalf("engine version changed to %q despite invalid flow-control config", got)
	}
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick with old engine = %q, %v", ep, err)
	}
}
