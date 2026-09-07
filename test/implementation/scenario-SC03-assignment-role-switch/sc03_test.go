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

// Package sc03: SC03 主备角色切换。
// 测试设计文档：test/测试设计文档/scenario-SC03-主备角色切换/
package sc03

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc03"}],"max_tokens":8}`

// report is a decoded assignment report body (pkg/assignment/types.go).
type report struct {
	Instance string `json:"instance"`
	Cells    []struct {
		Key    string `json:"key"`
		Role   string `json:"role"`
		State  string `json:"state"`
		Engine string `json:"engine_version"`
	} `json:"cells"`
}

func lastReport(t *testing.T, e *common.Env) report {
	t.Helper()
	reports := e.API.Reports()
	if len(reports) == 0 {
		t.Fatal("no assignment report received")
	}
	var r report
	if err := json.Unmarshal(reports[len(reports)-1], &r); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	return r
}

func cellOf(r report, cluster string) (reportCell, bool) {
	for _, c := range r.Cells {
		if c.Key == cluster {
			return c, true
		}
	}
	return reportCell{}, false
}

type reportCell = struct {
	Key    string `json:"key"`
	Role   string `json:"role"`
	State  string `json:"state"`
	Engine string `json:"engine_version"`
}

// TestTC01_DemoteToStandby: demoting the cluster to standby makes ext-proc
// reject requests for it (Unavailable), while the instance stays healthy.
func TestTC01_DemoteToStandby(t *testing.T) {
	e := common.NewEnv(t, "epp-sc03-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	e.API.SetAssignment(map[string]string{"cluster-a": "standby"})

	common.WaitFor(t, 20*time.Second, "standby pick rejection", func() bool {
		_, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err != nil && strings.Contains(err.Error(), "not serving")
	})
	e.EPP.WaitHealth(t, "liveness", 5*time.Second)
}

// TestTC02_PromoteToPrimary: promoting back to primary restores service and
// the report reflects the primary role/state.
func TestTC02_PromoteToPrimary(t *testing.T) {
	e := common.NewEnv(t, "epp-sc03-tc02", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	e.API.SetAssignment(map[string]string{"cluster-a": "standby"})
	common.WaitFor(t, 20*time.Second, "standby rejection", func() bool {
		_, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err != nil
	})

	e.API.SetAssignment(map[string]string{"cluster-a": "primary"})

	common.WaitFor(t, 20*time.Second, "service restored after promotion", func() bool {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err == nil && ep == e.ClusterSims["cluster-a"][0]
	})

	common.WaitFor(t, 20*time.Second, "report shows primary", func() bool {
		c, ok := cellOf(lastReport(t, e), "cluster-a")
		return ok && c.Role == "primary" && c.State == "primary"
	})
}

// TestTC03_Revoke: removing the cluster from the assignment drops the cell;
// requests fail and the report no longer carries the cluster.
func TestTC03_Revoke(t *testing.T) {
	e := common.NewEnv(t, "epp-sc03-tc03", map[string][]string{
		"cluster-a": {"a0"},
		"cluster-b": {"b0"},
	})
	defer e.Close(t)

	e.API.SetAssignment(map[string]string{"cluster-b": "primary"})

	common.WaitFor(t, 20*time.Second, "revoked cluster pick failure", func() bool {
		_, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err != nil
	})
	// The surviving cluster is unaffected.
	ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-b", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil || ep != e.ClusterSims["cluster-b"][0] {
		t.Fatalf("cluster-b pick = %q, %v", ep, err)
	}

	common.WaitFor(t, 20*time.Second, "report drops revoked cluster", func() bool {
		_, ok := cellOf(lastReport(t, e), "cluster-a")
		return !ok
	})
}
