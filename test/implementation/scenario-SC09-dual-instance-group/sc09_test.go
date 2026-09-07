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

// Package sc09: SC09 双实例互备组。
// 测试设计文档：test/测试设计文档/scenario-SC09-双实例互备组/
package sc09

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc09"}],"max_tokens":8}`

// pairEnv is a two-epp-instance environment sharing one mock API and one sim
// per cluster, per the milestone-3 "固定 2 实例互备组" deployment model.
type pairEnv struct {
	API   *common.MockAPI
	Sims  []*common.Process
	Addrs map[string]string // cluster -> sim addr
	A, B  *common.EppEnv
}

func (p *pairEnv) Close(t *testing.T) {
	t.Helper()
	if p.A != nil {
		p.A.Proc.Stop(t)
	}
	if p.B != nil {
		p.B.Proc.Stop(t)
	}
	for _, s := range p.Sims {
		s.Stop(t)
	}
	if p.API != nil {
		p.API.Close()
	}
}

// newPairEnv starts mock + sim per cluster + epp-A/epp-B against the mock.
// Per-instance assignments must be installed by the caller before WaitHealthy.
func newPairEnv(t *testing.T, clusters ...string) *pairEnv {
	t.Helper()
	logDir := t.TempDir()
	p := &pairEnv{API: common.NewMockAPI(t), Addrs: map[string]string{}}

	table := map[string]map[string][]map[string]any{}
	configs := map[string]json.RawMessage{}
	for _, c := range clusters {
		proc, addr := common.StartSim(t, logDir, "sim-"+c, "sim-model")
		p.Sims = append(p.Sims, proc)
		p.Addrs[c] = addr
		table[c] = map[string][]map[string]any{"sub-1": common.BackendMap(common.Backend{
			Name: c + "-0", Addr: "127.0.0.1", Port: common.PortOf(addr), Weight: 50,
		})}
		configs[c] = common.PickerConfig(c, false)
	}
	p.API.SetConfigs(configs)
	p.API.SetClusterTable(table)

	p.A = common.StartEPP(t, logDir, p.API.Addr(), "epp-A")
	p.B = common.StartEPP(t, logDir, p.API.Addr(), "epp-B")
	return p
}

// TestTC01_GroupSharding: in a 2-instance group each cluster is served by its
// own primary; the peer (standby) rejects traffic, ready to take over (FR-H1).
func TestTC01_GroupSharding(t *testing.T) {
	p := newPairEnv(t, "cluster-a", "cluster-b")
	defer p.Close(t)

	p.API.SetAssignments(map[string]map[string]string{
		"epp-A": {"cluster-a": "primary", "cluster-b": "standby"},
		"epp-B": {"cluster-a": "standby", "cluster-b": "primary"},
	})
	p.A.WaitHealth(t, "", 30*time.Second)
	p.B.WaitHealth(t, "", 30*time.Second)

	// A serves cluster-a only.
	ep, err := common.PickEndpoint(p.A.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != p.Addrs["cluster-a"] {
		t.Fatalf("A/cluster-a = %q, %v", ep, err)
	}
	if _, err := common.PickEndpoint(p.A.GRPCAddr, "cluster-b", "/v1/chat/completions", []byte(chatBody), 5*time.Second); err == nil ||
		!strings.Contains(err.Error(), "not serving") {
		t.Fatalf("A/cluster-b err = %v, want cell-not-serving", err)
	}

	// B serves cluster-b only.
	ep, err = common.PickEndpoint(p.B.GRPCAddr, "cluster-b", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != p.Addrs["cluster-b"] {
		t.Fatalf("B/cluster-b = %q, %v", ep, err)
	}
	if _, err := common.PickEndpoint(p.B.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second); err == nil ||
		!strings.Contains(err.Error(), "not serving") {
		t.Fatalf("B/cluster-a err = %v, want cell-not-serving", err)
	}
}

// TestTC02_DualActiveAccepted: when ai-gateway-api (mis)assigns the same
// cluster primary to both group members, both serve it — split-brain is
// tolerated by design (FR-H4) — and each instance reports its active state,
// which is the data source for the dual-active alert.
func TestTC02_DualActiveAccepted(t *testing.T) {
	p := newPairEnv(t, "cluster-a")
	defer p.Close(t)

	p.API.SetAssignments(map[string]map[string]string{
		"epp-A": {"cluster-a": "primary"},
		"epp-B": {"cluster-a": "primary"},
	})
	p.A.WaitHealth(t, "", 30*time.Second)
	p.B.WaitHealth(t, "", 30*time.Second)

	// Both instances serve the cluster.
	epA, errA := common.PickEndpoint(p.A.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	epB, errB := common.PickEndpoint(p.B.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if errA != nil || epA != p.Addrs["cluster-a"] {
		t.Fatalf("A pick = %q, %v", epA, errA)
	}
	if errB != nil || epB != p.Addrs["cluster-a"] {
		t.Fatalf("B pick = %q, %v", epB, errB)
	}

	// Both report primary/active state — the dual-active alert data source.
	common.WaitFor(t, 20*time.Second, "both instances report cluster-a primary", func() bool {
		seen := map[string]bool{}
		for _, raw := range p.API.Reports() {
			var r struct {
				Instance string `json:"instance"`
				Cells    []struct {
					Key  string `json:"key"`
					Role string `json:"role"`
				} `json:"cells"`
			}
			if err := json.Unmarshal(raw, &r); err != nil {
				continue
			}
			for _, c := range r.Cells {
				if c.Key == "cluster-a" && c.Role == "primary" {
					seen[r.Instance] = true
				}
			}
		}
		return seen["epp-A"] && seen["epp-B"]
	})
}
