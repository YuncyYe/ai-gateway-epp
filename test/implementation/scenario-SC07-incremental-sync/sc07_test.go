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

// Package sc07: SC07 增量同步与拉取健康。
// 测试设计文档：test/测试设计文档/scenario-SC07-增量同步与拉取健康/
package sc07

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc07"}],"max_tokens":8}`

const configPath = "/configs/epp_data/config"

// TestTC01_IncrementalIdle: in steady state pollers keep sending version
// params and idle cheaply (FR-D1): requests continue, Data:null rounds cause
// no engine rebuild; a version bump with byte-identical content does not
// recompile either (hash dedup).
func TestTC01_IncrementalIdle(t *testing.T) {
	e := common.NewEnv(t, "epp-sc07-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	if v1 == "" {
		t.Fatal("no compiled engine")
	}

	// Steady state: several poll intervals with no changes.
	countBefore := e.API.RequestCount(configPath)
	time.Sleep(1 * time.Second) // ~5 poll intervals at 200ms
	countAfter := e.API.RequestCount(configPath)
	if countAfter <= countBefore {
		t.Fatalf("config poller stopped polling (requests %d -> %d)", countBefore, countAfter)
	}
	if got := e.API.LastVersionQuery(configPath); got == "" {
		t.Fatal("poller is not sending the version query param")
	}
	text := common.FetchMetrics(t, e.EPP.MetricsAddr)
	if got := common.EngineVersion(text, "cluster-a"); got != v1 {
		t.Fatalf("engine changed without a config change: %q -> %q", v1, got)
	}
	if n := common.MetricValue(text, `ai_epp_engine_reloads_total{cluster="cluster-a",result="success"}`); n != 1 {
		t.Fatalf("engine recompiled during idle (reloads=%v, want 1)", n)
	}

	// Version bump with identical content: sync advances, no recompile.
	e.API.SetEppConfig(map[string]json.RawMessage{"cluster-a": common.EppConfig("cluster-a", false)})
	time.Sleep(1 * time.Second)
	text = common.FetchMetrics(t, e.EPP.MetricsAddr)
	if got := common.EngineVersion(text, "cluster-a"); got != v1 {
		t.Fatalf("engine recompiled on version bump with identical content: %q -> %q", v1, got)
	}
	if n := common.MetricValue(text, `ai_epp_engine_reloads_total{cluster="cluster-a",result="success"}`); n != 1 {
		t.Fatalf("reloads=%v after identical-content bump, want 1", n)
	}

	// Service unaffected throughout.
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick = %q, %v", ep, err)
	}
}

// TestTC02_PollerHealthMetrics: sync health is observable per poller
// (FR-O2): last-sync timestamps exist; failures increment counters and raise
// the backoff gauge while the API is down; recovery clears backoff.
func TestTC02_PollerHealthMetrics(t *testing.T) {
	e := common.NewEnv(t, "epp-sc07-tc02", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	text := common.FetchMetrics(t, e.EPP.MetricsAddr)
	for _, poller := range []string{"discovery", "epp_data"} {
		if common.MetricValue(text, `ai_epp_poller_last_sync_timestamp{poller="`+poller+`"}`) == 0 {
			t.Fatalf("missing last sync timestamp for poller %q", poller)
		}
	}

	e.API.SetFail(true)
	common.WaitFor(t, 15*time.Second, "failure counters increment while API down", func() bool {
		text := common.FetchMetrics(t, e.EPP.MetricsAddr)
		return common.MetricValue(text, `ai_epp_poller_failures_total{poller="epp_data"}`) > 0 &&
			common.MetricValue(text, `ai_epp_poller_backoff_state{poller="epp_data"}`) == 1
	})
	common.WaitFor(t, 15*time.Second, "discovery poller in backoff", func() bool {
		return common.MetricValue(common.FetchMetrics(t, e.EPP.MetricsAddr), `ai_epp_poller_backoff_state{poller="discovery"}`) == 1
	})

	e.API.SetFail(false)
	common.WaitFor(t, 15*time.Second, "backoff cleared after recovery", func() bool {
		text := common.FetchMetrics(t, e.EPP.MetricsAddr)
		return common.MetricValue(text, `ai_epp_poller_backoff_state{poller="epp_data"}`) == 0 &&
			common.MetricValue(text, `ai_epp_poller_last_sync_timestamp{poller="epp_data"}`) > 0
	})
}
