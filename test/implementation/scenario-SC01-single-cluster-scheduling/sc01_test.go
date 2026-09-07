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

// Package sc01: SC01 单 cluster 调度闭环。
// 测试设计文档：test/测试设计文档/scenario-SC01-单cluster调度闭环/
package sc01

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc01"}],"max_tokens":8}`

func chatCompletion(t *testing.T, endpoint, body string) (int, string) {
	t.Helper()
	resp, err := http.Post("http://"+endpoint+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post to %s: %v", endpoint, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestTC01_SchedulingClosedLoop: repeated picks land on live sims and the
// real OpenAI request succeeds.
func TestTC01_SchedulingClosedLoop(t *testing.T) {
	e := common.NewEnv(t, "epp-sc01-tc01", map[string][]string{
		"cluster-a": {"a0", "a1"},
	})
	defer e.Close(t)

	valid := map[string]bool{}
	for _, a := range e.ClusterSims["cluster-a"] {
		valid[a] = true
	}
	for i := 0; i < 10; i++ {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
		if err != nil {
			t.Fatalf("pick %d: %v (log: %s)", i, err, e.EPP.Proc.LogPath())
		}
		if !valid[ep] {
			t.Fatalf("pick %d: endpoint %q not in sim set %v", i, ep, e.ClusterSims["cluster-a"])
		}
		code, body := chatCompletion(t, ep, chatBody)
		if code != http.StatusOK {
			t.Fatalf("chat completion via %s: status %d: %s", ep, code, body)
		}
	}
}

// TestTC02_DemuxRouting: two clusters route to disjoint backend sets.
func TestTC02_DemuxRouting(t *testing.T) {
	e := common.NewEnv(t, "epp-sc01-tc02", map[string][]string{
		"cluster-a": {"a0"},
		"cluster-b": {"b0"},
	})
	defer e.Close(t)

	for cluster, want := range e.ClusterSims {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, cluster, "/v1/chat/completions", []byte(chatBody), 10*time.Second)
		if err != nil {
			t.Fatalf("pick %s: %v (log: %s)", cluster, err, e.EPP.Proc.LogPath())
		}
		if ep != want[0] {
			t.Fatalf("cluster %s routed to %q, want %q", cluster, ep, want[0])
		}
	}
}

// TestTC03_NoPoolMetadata: no metadata and no default pool -> error, process survives.
func TestTC03_NoPoolMetadata(t *testing.T) {
	e := common.NewEnv(t, "epp-sc01-tc03", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	_, err := common.PickEndpoint(e.EPP.GRPCAddr, "", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err == nil {
		t.Fatal("expected error for missing pool metadata")
	}
	// Process still healthy and serving the valid pool.
	e.EPP.WaitHealth(t, "", 5*time.Second)
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil {
		t.Fatalf("valid pick after error: %v", err)
	}
	if len(ep) == 0 {
		t.Fatal("empty endpoint")
	}
}

// TestTC04_UnknownPool: unknown pool -> error; assigned clusters unaffected.
func TestTC04_UnknownPool(t *testing.T) {
	e := common.NewEnv(t, "epp-sc01-tc04", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	_, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-ghost", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err == nil {
		t.Fatal("expected error for unknown pool")
	}
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil {
		t.Fatalf("valid pick after unknown-pool error: %v", err)
	}
	if len(ep) == 0 {
		t.Fatal("empty endpoint")
	}
}
