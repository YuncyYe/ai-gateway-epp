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

// Package sc15: SC15 本地配置文件加载。
// 测试设计文档：test/测试设计文档/scenario-SC15-本地配置文件加载/
//
// 范围限定：只验证配置被正确加载并生效，不访问推理服务。因此不启动
// inference-sim，cluster_table 端点为保留回环端口；仅通过 metrics /
// gRPC health / ext-proc 选点观测加载结果，不向后端发起推理请求。
package sc15

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

// chatBody is only used to drive the ext-proc pick (metadata + a parseable
// body); it is never sent to a backend in this scenario.
const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc15"}],"max_tokens":8}`

const configPath = "/configs/epp_data/config"
const tablePath = "/configs/gslb_data/cluster_table"

// pick drives one ext-proc scheduling round through epp and returns the
// chosen endpoint from the cluster_table snapshot (no backend is contacted).
func pick(t *testing.T, e *common.LocalEnv, pool string) (string, error) {
	t.Helper()
	return common.PickEndpoint(e.EPP.GRPCAddr, pool, "/v1/chat/completions", []byte(chatBody), 10*time.Second)
}

// assertLoaded checks the core "config loaded" signals for a primary cluster:
// compiled engine, primary cell, assignment matched, both pollers synced.
func assertLoaded(t *testing.T, e *common.LocalEnv, cluster string) {
	t.Helper()
	text := common.FetchMetrics(t, e.EPP.MetricsAddr)
	if v := common.EngineVersion(text, cluster); v == "" {
		t.Fatalf("%s has no compiled engine (epp_data_config not applied)", cluster)
	}
	if v := common.MetricValue(text, fmt.Sprintf(`ai_epp_cell_state{cluster=%q,role="primary",state="primary"}`, cluster)); v != 1 {
		t.Fatalf("%s cell_state primary/primary = %v, want 1", cluster, v)
	}
	if v := common.MetricValue(text, `ai_epp_assignment_no_match`); v != 0 {
		t.Fatalf("assignment_no_match = %v, want 0", v)
	}
	for _, poller := range []string{"discovery", "epp_data"} {
		if common.MetricValue(text, `ai_epp_poller_last_sync_timestamp{poller="`+poller+`"}`) <= 0 {
			t.Fatalf("poller %q has no last sync timestamp", poller)
		}
		if common.MetricValue(text, `ai_epp_poller_backoff_state{poller="`+poller+`"}`) != 0 {
			t.Fatalf("poller %q is in backoff", poller)
		}
		if common.MetricValue(text, `ai_epp_poller_failures_total{poller="`+poller+`"}`) != 0 {
			t.Fatalf("poller %q recorded failures", poller)
		}
	}
}

// reservedAddr returns a loopback "ip:port" placeholder that is never dialed.
func reservedAddr(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("127.0.0.1:%d", common.FindFreePort(t))
}

// configV2 renames the max-score plugin so the compiled engine content hash
// changes while remaining valid.
func configV2(cluster string) json.RawMessage {
	return json.RawMessage(strings.ReplaceAll(string(common.EppConfig(cluster, false)), `"max-score"`, `"max-score-v2"`))
}

// TestTC01_LocalConfigStartup: --local-config-dir alone loads both consumed
// files: readiness SERVING, engine compiled, primary cell, assignment matched,
// both pollers synced, and the cluster_table endpoint is picked by ext-proc.
func TestTC01_LocalConfigStartup(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc01", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	assertLoaded(t, e, "cluster-a")

	if n := common.MetricValue(common.FetchMetrics(t, e.EPP.MetricsAddr), `ai_epp_engine_reloads_total{cluster="cluster-a",result="success"}`); n < 1 {
		t.Fatalf("engine_reloads_total success = %v, want >= 1", n)
	}

	ep, err := pick(t, e, "cluster-a")
	if err != nil {
		t.Fatalf("pick cluster-a: %v (log: %s)", err, e.EPP.Proc.LogPath())
	}
	if want := e.ClusterSims["cluster-a"][0]; ep != want {
		t.Fatalf("pick = %q, want %q", ep, want)
	}
}

// TestTC02_LocalConfigEnvVar: AI_GATEWAY_EPP_LOCAL_CONFIG_DIR is equivalent to
// the --local-config-dir flag.
func TestTC02_LocalConfigEnvVar(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc02", map[string][]string{
		"cluster-a": {"a0"},
	}, common.WithLocalConfigEnvVar())
	defer e.Close(t)

	assertLoaded(t, e, "cluster-a")

	ep, err := pick(t, e, "cluster-a")
	if err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick = %q, %v", ep, err)
	}
}

// TestTC03_MultiCluster: multiple clusters load independently from one local
// dir; each cluster compiles its own engine and routes to its own endpoints.
func TestTC03_MultiCluster(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc03", map[string][]string{
		"cluster-a": {"a0"},
		"cluster-b": {"b0"},
	})
	defer e.Close(t)

	for _, cluster := range []string{"cluster-a", "cluster-b"} {
		assertLoaded(t, e, cluster)
	}
	for _, cluster := range []string{"cluster-a", "cluster-b"} {
		ep, err := pick(t, e, cluster)
		if err != nil {
			t.Fatalf("pick %s: %v (log: %s)", cluster, err, e.EPP.Proc.LogPath())
		}
		if want := e.ClusterSims[cluster][0]; ep != want {
			t.Fatalf("cluster %s pick = %q, want %q", cluster, ep, want)
		}
	}
}

// TestTC04_StandbyRole: a local assignment that holds this instance as standby
// builds a hot-standby cell (engine still compiled, readiness SERVING) but
// ext-proc refuses to serve the cluster.
func TestTC04_StandbyRole(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc04", map[string][]string{
		"cluster-a": {"a0"},
	}, common.WithLocalRoles(map[string]string{"cluster-a": "standby"}))
	defer e.Close(t)

	text := common.FetchMetrics(t, e.EPP.MetricsAddr)
	if v := common.EngineVersion(text, "cluster-a"); v == "" {
		t.Fatal("standby cell has no compiled engine (data plane should stay hot)")
	}
	if v := common.MetricValue(text, `ai_epp_cell_state{cluster="cluster-a",role="standby",state="standby"}`); v != 1 {
		t.Fatalf("cell_state standby/standby = %v, want 1", v)
	}
	if v := common.MetricValue(text, `ai_epp_assignment_no_match`); v != 0 {
		t.Fatalf("assignment_no_match = %v, want 0 (standby is a self-match)", v)
	}

	if _, err := pick(t, e, "cluster-a"); err == nil || !strings.Contains(err.Error(), "not serving") {
		t.Fatalf("standby pick err = %v, want cell-not-serving", err)
	}
	e.EPP.WaitHealth(t, "liveness", 5*time.Second)
}

// TestTC05_PollReload: LocalFileSource reports changed=true on every fetch, so
// editing a consumed file takes effect on the next poll without restarting
// epp (no inotify): an epp_config edit changes the engine hash, a
// cluster_table edit changes the picked endpoint.
func TestTC05_PollReload(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc05", map[string][]string{
		"cluster-a": {"a0"},
	})
	defer e.Close(t)

	v1 := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
	if v1 == "" {
		t.Fatal("cluster-a has no compiled engine")
	}

	// Edit epp_data_config.json: rename a plugin -> content hash changes.
	common.WriteEppDataConfigFile(t, e.ConfigDir,
		common.EppDataConfigFor(e.InstanceID, []string{"cluster-a"}, nil, configV2))
	common.WaitFor(t, 20*time.Second, "engine version changes after file edit", func() bool {
		v := common.EngineVersion(common.FetchMetrics(t, e.EPP.MetricsAddr), "cluster-a")
		return v != "" && v != v1
	})

	// Edit cluster_table.json: point cluster-a at a new reserved endpoint.
	newAddr := reservedAddr(t)
	common.WriteClusterTableFile(t, e.ConfigDir,
		common.LocalBackendTable(map[string][]string{"cluster-a": {newAddr}}))
	common.WaitFor(t, 20*time.Second, "pick hits the new cluster_table endpoint", func() bool {
		ep, err := common.PickEndpoint(e.EPP.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second)
		return err == nil && ep == newAddr
	})

	// No restart: the process is still healthy and consistent.
	e.EPP.WaitHealth(t, "", 5*time.Second)
}

// TestTC06_MissingEppData: a missing epp_data_config.json keeps epp alive but
// not ready (liveness SERVING, readiness NOT_SERVING), with the epp_data
// poller failing and backing off and no cell/engine; writing a valid file
// heals it automatically.
func TestTC06_MissingEppData(t *testing.T) {
	logDir := t.TempDir()
	dir := t.TempDir()
	addr := reservedAddr(t)
	common.WriteClusterTableFile(t, dir, common.LocalBackendTable(map[string][]string{"cluster-a": {addr}}))
	common.WriteEppPoolFile(t, dir, "epp-sc15-tc06")

	env := common.StartLocalEPP(t, logDir, dir, "epp-sc15-tc06", common.LocalEPPOptions{})
	defer env.Proc.Stop(t)

	env.WaitHealth(t, "liveness", 5*time.Second)
	if st, err := common.CheckHealth(env.HealthAddr, ""); err != nil || st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness = %v err=%v, want NOT_SERVING (log: %s)", st, err, env.Proc.LogPath())
	}

	common.WaitFor(t, 15*time.Second, "epp_data poller failure and backoff", func() bool {
		text := common.FetchMetrics(t, env.MetricsAddr)
		return common.MetricValue(text, `ai_epp_poller_failures_total{poller="epp_data"}`) > 0 &&
			common.MetricValue(text, `ai_epp_poller_backoff_state{poller="epp_data"}`) == 1
	})
	text := common.FetchMetrics(t, env.MetricsAddr)
	if v := common.EngineVersion(text, "cluster-a"); v != "" {
		t.Fatalf("engine compiled (%q) despite missing epp_data_config.json", v)
	}
	if v := common.MetricValue(text, `ai_epp_cell_state{cluster="cluster-a",role="primary",state="primary"}`); v != 0 {
		t.Fatalf("cell created despite missing epp_data_config.json (gauge=%v)", v)
	}

	// Heal.
	common.WriteEppDataConfigFile(t, dir,
		common.EppDataConfigFor("epp-sc15-tc06", []string{"cluster-a"}, nil, common.LocalEppConfig))
	env.WaitHealth(t, "", 45*time.Second)
	common.WaitFor(t, 15*time.Second, "epp_data backoff cleared after heal", func() bool {
		return common.MetricValue(common.FetchMetrics(t, env.MetricsAddr), `ai_epp_poller_backoff_state{poller="epp_data"}`) == 0
	})
	if ep, err := common.PickEndpoint(env.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second); err != nil || ep != addr {
		t.Fatalf("pick after heal = %q, %v", ep, err)
	}
}

// TestTC07_BadJSON: corrupt JSON (file exists) fails the same way as a missing
// file - decode error, fail-static, backoff retry - and heals once valid.
func TestTC07_BadJSON(t *testing.T) {
	logDir := t.TempDir()
	dir := t.TempDir()
	addr := reservedAddr(t)
	common.WriteClusterTableFile(t, dir, common.LocalBackendTable(map[string][]string{"cluster-a": {addr}}))
	common.WriteEppPoolFile(t, dir, "epp-sc15-tc07")
	if err := os.WriteFile(filepath.Join(dir, "epp_data_config.json"), []byte(`{ this is not json }`), 0644); err != nil {
		t.Fatalf("write bad json: %v", err)
	}

	env := common.StartLocalEPP(t, logDir, dir, "epp-sc15-tc07", common.LocalEPPOptions{})
	defer env.Proc.Stop(t)

	env.WaitHealth(t, "liveness", 5*time.Second)
	if st, err := common.CheckHealth(env.HealthAddr, ""); err != nil || st != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness = %v err=%v, want NOT_SERVING", st, err)
	}
	common.WaitFor(t, 15*time.Second, "epp_data poller failure and backoff", func() bool {
		text := common.FetchMetrics(t, env.MetricsAddr)
		return common.MetricValue(text, `ai_epp_poller_failures_total{poller="epp_data"}`) > 0 &&
			common.MetricValue(text, `ai_epp_poller_backoff_state{poller="epp_data"}`) == 1
	})
	text := common.FetchMetrics(t, env.MetricsAddr)
	if v := common.EngineVersion(text, "cluster-a"); v != "" {
		t.Fatalf("engine compiled (%q) despite corrupt epp_data_config.json", v)
	}
	if v := common.MetricValue(text, `ai_epp_cell_state{cluster="cluster-a",role="primary",state="primary"}`); v != 0 {
		t.Fatalf("cell created despite corrupt epp_data_config.json (gauge=%v)", v)
	}

	// Heal.
	common.WriteEppDataConfigFile(t, dir,
		common.EppDataConfigFor("epp-sc15-tc07", []string{"cluster-a"}, nil, common.LocalEppConfig))
	env.WaitHealth(t, "", 45*time.Second)
	if ep, err := common.PickEndpoint(env.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second); err != nil || ep != addr {
		t.Fatalf("pick after heal = %q, %v", ep, err)
	}
}

// TestTC08_NoInnerAPITraffic: even with a reachable InnerAPI mock on
// -api-addr, the local mode never contacts it: both consumed endpoints and the
// retired report endpoint stay at zero requests.
func TestTC08_NoInnerAPITraffic(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc08", map[string][]string{
		"cluster-a": {"a0"},
	}, common.WithLocalObservedAPI())
	defer e.Close(t)

	if e.API == nil {
		t.Fatal("observed mock API not started")
	}
	if _, err := pick(t, e, "cluster-a"); err != nil {
		t.Fatalf("pick cluster-a: %v (log: %s)", err, e.EPP.Proc.LogPath())
	}
	// Serve for several poll intervals so any (unexpected) InnerAPI traffic
	// would have happened.
	time.Sleep(2 * time.Second)

	if n := e.API.RequestCount(configPath); n != 0 {
		t.Fatalf("%s requests = %d, want 0 in local mode", configPath, n)
	}
	if n := e.API.RequestCount(tablePath); n != 0 {
		t.Fatalf("%s requests = %d, want 0 in local mode", tablePath, n)
	}
	if n := e.API.ReportCount(); n != 0 {
		t.Fatalf("assignment/report posts = %d, want 0", n)
	}
}

// TestTC09_PoolFileNotConsumed: epp_pool.json is reference-only. Omitting it
// does not block startup, and adding a pool that does not contain this
// instance id has no runtime effect.
func TestTC09_PoolFileNotConsumed(t *testing.T) {
	e := common.NewLocalEnv(t, "epp-sc15-tc09", map[string][]string{
		"cluster-a": {"a0"},
	}, common.WithoutLocalPoolFile())
	defer e.Close(t)

	assertLoaded(t, e, "cluster-a")
	if ep, err := pick(t, e, "cluster-a"); err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick without pool file = %q, %v", ep, err)
	}

	// Write a pool that does not contain this instance id: no runtime
	// validation should occur, so behavior is unchanged.
	common.WriteEppPoolFile(t, e.ConfigDir, "some-other-instance")
	time.Sleep(2 * time.Second)

	e.EPP.WaitHealth(t, "", 5*time.Second)
	assertLoaded(t, e, "cluster-a")
	if ep, err := pick(t, e, "cluster-a"); err != nil || ep != e.ClusterSims["cluster-a"][0] {
		t.Fatalf("pick after pool change = %q, %v", ep, err)
	}
}
