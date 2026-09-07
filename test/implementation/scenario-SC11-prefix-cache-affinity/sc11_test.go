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

// Package sc11: SC11 PrefixCacheAffinity 调度。
// 测试设计文档：test/测试设计文档/scenario-SC11-PrefixCacheAffinity调度/
//
// 验证 prefix-cache-scorer（近似前缀缓存亲和）：请求首次调度后，其
// prompt 前缀哈希被记录到被选后端（approx-prefix-cache producer 的
// PreRequest 自主学习，无需 kv-cache 事件总线）；后续相同前缀的请求
// 被稳定调度到同一后端。配置热加载后索引重建，亲和自动重新学习。
package sc11

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

// prefixConfig: discovery + prefix-cache-scorer + max-score-picker. The
// approx-prefix-cache data producer is a registered default producer, so no
// explicit producer config is needed (upstream sample:
// llm-d-router/deploy/config/epp-estimate-prefix-cache-config.yaml).
func prefixConfig(cluster string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": %q}},
    {"name": "prefix-scorer", "type": "prefix-cache-scorer", "parameters": {}},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [{"pluginRef": "prefix-scorer"}, {"pluginRef": "max-score"}]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`, cluster))
}

// chatBodyWith builds a chat completion body with a long, fixed prompt so the
// prefix spans multiple hash blocks.
func chatBodyWith(content string) string {
	return fmt.Sprintf(`{"model":"sim-model","messages":[{"role":"user","content":%q}],"max_tokens":8}`, content)
}

const longPrompt = "The quick brown fox jumps over the lazy dog. " +
	"Pack my box with five dozen liquor jugs. " +
	"How vexingly quick daft zebras jump. "

// converged runs bursts of identical-prompt picks until one full burst lands
// on a single endpoint, returning that endpoint. The first pick of each
// iteration pins the candidate, so a burst only passes when affinity holds
// for the whole burst.
func converged(t *testing.T, e *common.Env, body string, burst int) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		first, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(body), 5*time.Second)
		if err == nil {
			ok := true
			for i := 1; i < burst; i++ {
				ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(body), 5*time.Second)
				if err != nil || ep != first {
					ok = false
					break
				}
			}
			if ok {
				return first
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("prompt never converged to a single backend")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTC01_AffinityConverges: repeated requests with the same long prompt all
// land on one backend — the prefix learned on the first dispatch drives
// scheduling.
func TestTC01_AffinityConverges(t *testing.T) {
	e := common.NewEnv(t, "epp-sc11-tc01", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithPickerConfigFn(prefixConfig))
	defer e.Close(t)

	body := chatBodyWith(longPrompt)
	want := converged(t, e, body, 8)

	valid := map[string]bool{}
	for _, a := range e.ClusterSims["cluster-a"] {
		valid[a] = true
	}
	if !valid[want] {
		t.Fatalf("converged endpoint %q not in sim set %v", want, e.ClusterSims["cluster-a"])
	}
}

// TestTC02_DistinctPromptsConvergeIndependently: two different long prompts
// each stabilize on a single backend; affinity is keyed by prefix content.
func TestTC02_DistinctPromptsConvergeIndependently(t *testing.T) {
	e := common.NewEnv(t, "epp-sc11-tc02", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithPickerConfigFn(prefixConfig))
	defer e.Close(t)

	bodyA := chatBodyWith(longPrompt)
	bodyB := chatBodyWith("Sphinx of black quartz, judge my vow. " +
		"Waltz, bad nymph, for quick jigs vex. " +
		"Glib jocks quiz nymph to vex dwarf. ")
	if bodyA == bodyB {
		t.Fatal("prompts must differ")
	}

	converged(t, e, bodyA, 6)
	converged(t, e, bodyB, 6)
}

// TestTC03_ReloadRelearns: after a config hot-reload the in-memory prefix
// index resets, and affinity is automatically re-learned — the same prompt
// converges again on a single backend without any process restart.
func TestTC03_ReloadRelearns(t *testing.T) {
	e := common.NewEnv(t, "epp-sc11-tc03", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithPickerConfigFn(prefixConfig))
	defer e.Close(t)

	body := chatBodyWith(longPrompt)
	converged(t, e, body, 6)

	// Hot-reload with a renamed (still valid) plugin: engine swaps, prefix
	// index is rebuilt empty.
	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	e.API.SetConfigs(map[string]json.RawMessage{
		"cluster-a": json.RawMessage(strings.ReplaceAll(string(prefixConfig("cluster-a")), `"prefix-scorer"`, `"prefix-scorer-v2"`)),
	})
	common.WaitFor(t, 20*time.Second, "engine swapped after reload", func() bool {
		v := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
		return v != "" && v != v1
	})

	// Affinity is re-learned on the new engine.
	converged(t, e, body, 8)
}
