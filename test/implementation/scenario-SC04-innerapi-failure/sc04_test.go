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

// Package sc04: SC04 InnerAPI 故障 fail-static。
// 测试设计文档：test/测试设计文档/scenario-SC04-InnerAPI故障fail-static/
package sc04

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc04"}],"max_tokens":8}`

// TestTC01_FailStatic: with the InnerAPI down, scheduling keeps working from
// the last synced state.
func TestTC01_FailStatic(t *testing.T) {
	e := common.NewEnv(t, "epp-sc04-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	e.API.SetFail(true)

	// Poll: a pick succeeds against the cached view. (Pick errors are
	// retried inside WaitFor rather than failing the test immediately.)
	common.WaitFor(t, 10*time.Second, "pick succeeds while InnerAPI down", func() bool {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err == nil && ep == e.ClusterSims["cluster-a"][0]
	})
	e.EPP.WaitHealth(t, "liveness", 5*time.Second)
}

// TestTC02_RecoverySyncsPendingConfig: a config change issued while the
// InnerAPI is down takes effect once it recovers.
func TestTC02_RecoverySyncsPendingConfig(t *testing.T) {
	e := common.NewEnv(t, "epp-sc04-tc02", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	if v1 == "" {
		t.Fatal("cluster-a has no compiled engine version")
	}

	e.API.SetFail(true)
	e.API.SetEppConfig(map[string]json.RawMessage{
		"cluster-a": json.RawMessage(strings.ReplaceAll(string(common.EppConfig("cluster-a", false)), `"max-score"`, `"max-score-v2"`)),
	})
	// Still serving with the old engine while the API is down.
	if got := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a"); got != v1 {
		t.Fatalf("engine version changed to %q while InnerAPI down", got)
	}

	e.API.SetFail(false)

	common.WaitFor(t, 20*time.Second, "pending config applied after recovery", func() bool {
		return common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a") != v1
	})
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick after recovery = %q, %v", ep, err)
	}
}
