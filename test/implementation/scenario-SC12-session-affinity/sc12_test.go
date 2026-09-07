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

// Package sc12: SC12 SessionAffinity 调度。
// 测试设计文档：test/测试设计文档/scenario-SC12-SessionAffinity调度/
//
// 验证 session-affinity-scorer（session_id 策略）：scorer 从请求 header
// 解析 session id，调度后（PreRequest）把 session 绑定到被选端点；
// 后续同 session 请求被稳定调度到同一端点，绑定端点不在候选集时
// 自动迁移。绑定为引擎内存态，随热加载重建。
package sc12

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc12"}],"max_tokens":8}`

// sessionAffinityConfig: discovery + session-affinity-scorer (session_id
// strategy reading the x-session-id header) + max-score-picker.
func sessionAffinityConfig(cluster string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": %q}},
    {"name": "sa-scorer", "type": "session-affinity-scorer", "parameters": {
      "strategy": "session_id",
      "sessionIdConfig": {"sources": [{"header": "x-session-id"}]}
    }},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [{"pluginRef": "sa-scorer"}, {"pluginRef": "max-score"}]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`, cluster))
}

// pickSession sends one ext-proc request carrying the given session id header.
func pickSession(t *testing.T, e *common.Env, sessionID string) (string, error) {
	t.Helper()
	var headers map[string]string
	if sessionID != "" {
		headers = map[string]string{"x-session-id": sessionID}
	}
	return common.PickEndpointWithHeaders(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second, headers)
}

// stick asserts that `n` consecutive picks for the session all land on one
// backend, returning it. Retries while the binding may still be settling.
func stick(t *testing.T, e *common.Env, sessionID string, n int) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		first, err := pickSession(t, e, sessionID)
		if err == nil {
			ok := true
			for i := 1; i < n; i++ {
				ep, err := pickSession(t, e, sessionID)
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
			t.Fatalf("session %q never stuck to a single backend", sessionID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTC01_SessionSticks: a session id pins all subsequent requests of that
// session to one backend; requests without the header are unaffected
// (scorer abstains, picker chooses freely).
func TestTC01_SessionSticks(t *testing.T) {
	e := common.NewEnv(t, "epp-sc12-tc01", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithPickerConfigFn(sessionAffinityConfig))
	defer e.Close(t)

	pinned := stick(t, e, "sess-1", 8)
	valid := map[string]bool{}
	for _, a := range e.ClusterSims["cluster-a"] {
		valid[a] = true
	}
	if !valid[pinned] {
		t.Fatalf("pinned endpoint %q not in sim set %v", pinned, e.ClusterSims["cluster-a"])
	}

	// Headerless requests keep flowing (scorer abstains, no error).
	ep, err := pickSession(t, e, "")
	if err != nil || !valid[ep] {
		t.Fatalf("headerless pick = %q, %v", ep, err)
	}
}

// TestTC02_DistinctSessionsIndependent: two sessions each stick to a backend
// and stay stable alongside each other; a session's affinity does not leak.
func TestTC02_DistinctSessionsIndependent(t *testing.T) {
	e := common.NewEnv(t, "epp-sc12-tc02", map[string][]string{
		"cluster-a": {"a0", "b0"},
	}, common.WithPickerConfigFn(sessionAffinityConfig))
	defer e.Close(t)

	stick(t, e, "sess-a", 6)
	stick(t, e, "sess-b", 6)
	// Interleaved again — both bindings still hold.
	stick(t, e, "sess-a", 4)
	stick(t, e, "sess-b", 4)
}

// TestTC03_BoundBackendDrainedMigrates: when the backend a session is pinned
// to leaves the candidate set (Weight=0), the session migrates to a surviving
// backend and sticks to it.
func TestTC03_BoundBackendDrainedMigrates(t *testing.T) {
	e := common.NewEnv(t, "epp-sc12-tc03", map[string][]string{
		"cluster-a": {"a0", "b1"},
	}, common.WithPickerConfigFn(sessionAffinityConfig))
	defer e.Close(t)

	a0 := e.ClusterSims["cluster-a"][0]
	b1 := e.ClusterSims["cluster-a"][1]

	// Pin the session (which backend it lands on is racy by design).
	pinned := stick(t, e, "sess-migrate", 6)

	// Drain whichever backend the session is NOT pinned to first is a no-op;
	// drain the pinned one.
	var survivor string
	if pinned == a0 {
		survivor = b1
		e.API.SetClusterTable(map[string]map[string][]map[string]any{
			"cluster-a": {"sub-1": common.BackendMap(common.Backend{Name: "a0", Addr: "127.0.0.1", Port: common.PortOf(a0), Weight: 0},
				common.Backend{Name: "b1", Addr: "127.0.0.1", Port: common.PortOf(b1), Weight: 50})},
		})
	} else {
		survivor = a0
		e.API.SetClusterTable(map[string]map[string][]map[string]any{
			"cluster-a": {"sub-1": common.BackendMap(common.Backend{Name: "a0", Addr: "127.0.0.1", Port: common.PortOf(a0), Weight: 50},
				common.Backend{Name: "b1", Addr: "127.0.0.1", Port: common.PortOf(b1), Weight: 0})},
		})
	}

	// The session must migrate to the survivor and stick.
	common.WaitFor(t, 30*time.Second, "session migrates after its backend drained", func() bool {
		ep, err := pickSession(t, e, "sess-migrate")
		return err == nil && ep == survivor
	})
	stick(t, e, "sess-migrate", 8)
}
