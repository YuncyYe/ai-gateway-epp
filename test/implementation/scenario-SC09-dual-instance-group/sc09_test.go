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

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc09"}],"max_tokens":8}`

// groupView builds the full assignment view for a {primary, standby} pair:
// A holds the listed clusters as primary, B as standby.
func groupView(a, b string, clusters ...string) map[string]innerapi.AssignmentEntry {
	view := make(map[string]innerapi.AssignmentEntry, len(clusters))
	for _, c := range clusters {
		view[c] = innerapi.AssignmentEntry{Primary: sp(a), Standby: sp(b)}
	}
	return view
}

func sp(s string) *string { return &s }

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
// The assignment view must be installed by the caller before WaitHealthy.
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
		configs[c] = common.EppConfig(c, false)
	}
	p.API.SetEppConfig(configs)
	p.API.SetClusterTable(table)

	p.A = common.StartEPP(t, logDir, p.API.Addr(), "epp-A")
	p.B = common.StartEPP(t, logDir, p.API.Addr(), "epp-B")
	return p
}

func notServing(err error) bool {
	return err != nil && strings.Contains(err.Error(), "not serving")
}

// TestTC01_GroupSharding: in a 2-instance group each cluster is served by its
// own primary; the peer (standby) rejects traffic, ready to take over (FR-H1).
func TestTC01_GroupSharding(t *testing.T) {
	p := newPairEnv(t, "cluster-a", "cluster-b")
	defer p.Close(t)

	p.API.SetAssignmentView(map[string]innerapi.AssignmentEntry{
		"cluster-a": {Primary: sp("epp-A"), Standby: sp("epp-B")},
		"cluster-b": {Primary: sp("epp-B"), Standby: sp("epp-A")},
	})
	p.A.WaitHealth(t, "", 30*time.Second)
	p.B.WaitHealth(t, "", 30*time.Second)

	// A serves cluster-a only.
	ep, err := common.PickEndpoint(p.A.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != p.Addrs["cluster-a"] {
		t.Fatalf("A/cluster-a = %q, %v", ep, err)
	}
	if _, err := common.PickEndpoint(p.A.GRPCAddr, "cluster-b", "/v1/chat/completions", []byte(chatBody), 5*time.Second); !notServing(err) {
		t.Fatalf("A/cluster-b err = %v, want cell-not-serving", err)
	}

	// B serves cluster-b only.
	ep, err = common.PickEndpoint(p.B.GRPCAddr, "cluster-b", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != p.Addrs["cluster-b"] {
		t.Fatalf("B/cluster-b = %q, %v", ep, err)
	}
	if _, err := common.PickEndpoint(p.B.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second); !notServing(err) {
		t.Fatalf("B/cluster-a err = %v, want cell-not-serving", err)
	}

	// The full-view design has no readiness report channel at all.
	if n := p.API.ReportCount(); n != 0 {
		t.Fatalf("readiness report posted %d times, want 0", n)
	}
}

// TestTC02_FailoverFlip: flipping the full assignment view (primary A ->
// primary B) makes the standby serve and the former primary reject — the
// hot-standby takeover path, driven purely by the view each instance matches
// locally. Dual-primary is no longer expressible: the full view assigns one
// primary per cluster by construction.
func TestTC02_FailoverFlip(t *testing.T) {
	p := newPairEnv(t, "cluster-a")
	defer p.Close(t)

	p.API.SetAssignmentView(groupView("epp-A", "epp-B", "cluster-a"))
	p.A.WaitHealth(t, "", 30*time.Second)
	p.B.WaitHealth(t, "", 30*time.Second)

	ep, err := common.PickEndpoint(p.A.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != p.Addrs["cluster-a"] {
		t.Fatalf("A pick = %q, %v", ep, err)
	}
	if _, err := common.PickEndpoint(p.B.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 5*time.Second); !notServing(err) {
		t.Fatalf("B (standby) err = %v, want cell-not-serving", err)
	}

	// Failover: B becomes primary, A falls back to standby.
	p.API.SetAssignmentView(groupView("epp-B", "epp-A", "cluster-a"))
	common.WaitFor(t, 20*time.Second, "B serves after failover", func() bool {
		ep, err := common.PickEndpoint(p.B.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err == nil && ep == p.Addrs["cluster-a"]
	})
	common.WaitFor(t, 20*time.Second, "A rejects after failover", func() bool {
		_, err := common.PickEndpoint(p.A.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return notServing(err)
	})
}
