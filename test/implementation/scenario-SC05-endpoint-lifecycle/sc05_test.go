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

// Package sc05: SC05 端点生命周期。
// 测试设计文档：test/测试设计文档/scenario-SC05-端点生命周期/
package sc05

import (
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc05"}],"max_tokens":8}`

// tableFor builds the mock cluster table for cluster-a from named sims.
// names must match entries in e.ClusterSims order; weights parallel.
func tableFor(e *common.Env, names []string, weights []int) map[string]map[string][]map[string]any {
	backends := []common.Backend{}
	for i, n := range names {
		addr := e.ClusterSims["cluster-a"][i]
		backends = append(backends, common.Backend{
			Name:   "cluster-a-" + n,
			Addr:   "127.0.0.1",
			Port:   common.PortOf(addr),
			Weight: weights[i],
		})
	}
	return map[string]map[string][]map[string]any{"cluster-a": {"sub-1": common.BackendMap(backends...)}}
}

// TestTC01_ScaleOut: adding a backend to the cluster table makes it a
// scheduling target without restarting epp.
func TestTC01_ScaleOut(t *testing.T) {
	e := common.NewEnv(t, "epp-sc05-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	// Add sim a1 and publish a table containing both backends.
	logDir := t.TempDir()
	proc, addr := common.StartSim(t, logDir, "sim-a1", "sim-model")
	e.Sims = append(e.Sims, proc)
	e.ClusterSims["cluster-a"] = append(e.ClusterSims["cluster-a"], addr)
	e.API.SetClusterTable(tableFor(e, []string{"a0", "a1"}, []int{50, 50}))

	want := map[string]bool{}
	for _, a := range e.ClusterSims["cluster-a"] {
		want[a] = true
	}
	hit := map[string]bool{}
	common.WaitFor(t, 30*time.Second, "picks hit the new backend", func() bool {
		for i := 0; i < 5; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err == nil && want[ep] {
				hit[ep] = true
			}
		}
		return len(hit) == 2
	})
}

// TestTC02_DrainByWeightZero: setting Weight=0 removes the backend from the
// scheduling set while traffic keeps flowing to the remaining backend.
func TestTC02_DrainByWeightZero(t *testing.T) {
	e := common.NewEnv(t, "epp-sc05-tc02", map[string][]string{
		"cluster-a": {"a0", "a1"},
	})
	defer e.Close(t)

	e.API.SetClusterTable(tableFor(e, []string{"a0", "a1"}, []int{50, 0}))

	alive := e.ClusterSims["cluster-a"][0]
	drained := e.ClusterSims["cluster-a"][1]
	// The discovery poller applies the Weight=0 removal asynchronously; wait
	// until picks consistently avoid the drained backend.
	deadline := time.Now().Add(20 * time.Second)
	for {
		allAlive := true
		for i := 0; i < 5; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err != nil {
				t.Fatalf("pick: %v", err)
			}
			if ep == drained {
				allAlive = false
			} else if ep != alive {
				t.Fatalf("routed to %q, want %q", ep, alive)
			}
		}
		if allAlive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("picks still route to drained backend %q", drained)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// TestTC03_DrainAll: with every backend at Weight=0, scheduling fails and the
// process stays healthy; scaling back up restores service.
func TestTC03_DrainAll(t *testing.T) {
	e := common.NewEnv(t, "epp-sc05-tc03", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	e.API.SetClusterTable(tableFor(e, []string{"a0"}, []int{0}))

	common.WaitFor(t, 30*time.Second, "pick fails with no available endpoints", func() bool {
		_, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err != nil
	})
	e.EPP.WaitHealth(t, "liveness", 5*time.Second)

	// Restore. The rediscovery propagates asynchronously; wait for service to
	// come back rather than asserting on a single immediate pick.
	e.API.SetClusterTable(tableFor(e, []string{"a0"}, []int{50}))
	common.WaitFor(t, 20*time.Second, "service restored after restore", func() bool {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err == nil && ep == e.ClusterSims["cluster-a"][0]
	})
}

// TestTC04_WeightIsNotRatio: a non-zero Weight is only a registration switch,
// not a traffic ratio (需求分析 §3.1.1). With weights 90/10 both backends
// keep receiving traffic; if Weight were a ratio, the 10-weight backend would
// vanish from the hit set.
func TestTC04_WeightIsNotRatio(t *testing.T) {
	e := common.NewEnv(t, "epp-sc05-tc04", map[string][]string{
		"cluster-a": {"a0", "a1"},
	})
	defer e.Close(t)

	e.API.SetClusterTable(tableFor(e, []string{"a0", "a1"}, []int{90, 10}))

	hit := map[string]bool{}
	common.WaitFor(t, 30*time.Second, "both backends receive traffic with 90/10 weights", func() bool {
		for i := 0; i < 5; i++ {
			ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second)
			if err == nil {
				hit[ep] = true
			}
		}
		return len(hit) == 2
	})
}
