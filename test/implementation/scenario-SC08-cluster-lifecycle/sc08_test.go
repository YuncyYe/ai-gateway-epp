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

// Package sc08: SC08 cluster 数据驱动生命周期。
// 测试设计文档：test/测试设计文档/scenario-SC08-cluster数据驱动生命周期/
package sc08

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc08"}],"max_tokens":8}`

// TestTC01_NewClusterAdopted: a cluster that appears later in all three
// InnerAPI views is adopted automatically — new cell, discovery, scheduling —
// without restarting epp (FR-D4, FR-M3).
func TestTC01_NewClusterAdopted(t *testing.T) {
	e := common.NewEnv(t, "epp-sc08-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	// Start a new backend and publish cluster-b in assignment, picker_config
	// and cluster_table.
	logDir := t.TempDir()
	proc, addr := common.StartSim(t, logDir, "sim-b0", "sim-model")
	e.Sims = append(e.Sims, proc)
	e.ClusterSims["cluster-b"] = []string{addr}

	backends := func(cluster string, addrs []string) []map[string]any {
		bs := []common.Backend{}
		for _, a := range addrs {
			bs = append(bs, common.Backend{
				Name:   cluster + "-" + strings.TrimPrefix(a, "127.0.0.1:"),
				Addr:   "127.0.0.1",
				Port:   common.PortOf(a),
				Weight: 50,
			})
		}
		return common.BackendMap(bs...)
	}
	e.API.SetAssignment(map[string]string{"cluster-a": "primary", "cluster-b": "primary"})
	e.API.SetConfigs(map[string]json.RawMessage{
		"cluster-a": common.PickerConfig("cluster-a", false),
		"cluster-b": common.PickerConfig("cluster-b", false),
	})
	e.API.SetClusterTable(map[string]map[string][]map[string]any{
		"cluster-a": {"sub-1": backends("cluster-a", e.ClusterSims["cluster-a"])},
		"cluster-b": {"sub-1": backends("cluster-b", e.ClusterSims["cluster-b"])},
	})

	// The new cluster becomes schedulable without any epp restart.
	common.WaitFor(t, 30*time.Second, "new cluster-a pick fails, cluster-b serves", func() bool {
		epB, errB := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-b", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		epA, errA := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return errB == nil && epB == addr && errA == nil && epA == e.ClusterSims["cluster-a"][0]
	})

	// The new cell reports itself.
	common.WaitFor(t, 20*time.Second, "report carries cluster-b", func() bool {
		reports := e.API.Reports()
		if len(reports) == 0 {
			return false
		}
		return strings.Contains(string(reports[len(reports)-1]), `"key":"cluster-b"`)
	})
}

// TestTC02_UnassignedConfigIgnored: a picker_config entry for a cluster this
// instance is not assigned must not create a cell and must not disturb
// assigned clusters (FR-C2 isolation).
func TestTC02_UnassignedConfigIgnored(t *testing.T) {
	e := common.NewEnv(t, "epp-sc08-tc02", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	e.API.SetConfigs(map[string]json.RawMessage{
		"cluster-a":     common.PickerConfig("cluster-a", false),
		"cluster-ghost": common.PickerConfig("cluster-ghost", false),
	})

	// Let several poll intervals pass.
	time.Sleep(1 * time.Second)

	text := common.FetchMetrics(t, e.EPP.MetricsAddr)
	if strings.Contains(text, `"cluster-ghost"`) && common.EngineVersion(text, "cluster-ghost") != "" {
		t.Fatal("engine compiled for unassigned cluster-ghost")
	}
	reports := e.API.Reports()
	if len(reports) == 0 {
		t.Fatal("no report received")
	}
	if strings.Contains(string(reports[len(reports)-1]), "cluster-ghost") {
		t.Fatal("report carries unassigned cluster-ghost")
	}

	// The assigned cluster keeps serving.
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("cluster-a pick = %q, %v", ep, err)
	}
}
