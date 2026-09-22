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

package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

// LocalEnv is an integration environment where epp loads all cluster-level
// config from JSON files via --local-config-dir instead of a mock InnerAPI.
// No inference-sim process is started: the cluster_table endpoints are
// reserved loopback ports that are never dialed, because local-config tests
// only verify that the config is loaded (cells, engine, readiness, endpoint
// registration), not that inference traffic is served.
type LocalEnv struct {
	EPP         *EppEnv
	InstanceID  string
	ConfigDir   string              // directory passed to --local-config-dir
	Clusters    []string            // cluster names in declaration order
	ClusterSims map[string][]string // cluster -> reserved "127.0.0.1:port" endpoints
	// API is non-nil only when the env was built WithLocalObservedAPI; it
	// records InnerAPI traffic so tests can assert the local mode never
	// contacts ai-gateway-api.
	API *MockAPI
}

// Close stops epp and (when started) the observed mock API.
func (e *LocalEnv) Close(t *testing.T) {
	t.Helper()
	if e.EPP != nil {
		e.EPP.Proc.Stop(t)
	}
	if e.API != nil {
		e.API.Close()
	}
}

// LocalEPPOptions tunes StartLocalEPP.
type LocalEPPOptions struct {
	// APIAddr, when non-empty, is passed as -api-addr.
	APIAddr string
	// EnvVar passes the config dir via AI_GATEWAY_EPP_LOCAL_CONFIG_DIR
	// instead of the --local-config-dir flag.
	EnvVar bool
	// ExtraArgs are appended after the standard local-mode flags.
	ExtraArgs []string
}

// StartLocalEPP starts epp in local-config-dir mode and waits for the gRPC
// port to accept connections (process up), without waiting for readiness:
// fail-static tests expect a running but not-ready process and still need the
// handle to scrape /metrics and heal the config.
func StartLocalEPP(t fatalT, logDir, configDir, instanceID string, opts LocalEPPOptions) *EppEnv {
	t.Helper()
	bin := EppBinary(t)
	grpcPort := FindFreePort(t)
	healthPort := FindFreePort(t)
	metricsPort := FindFreePort(t)
	args := []string{
		bin,
		"-instance-id", instanceID,
		"-poll-interval", "200ms",
		"-poll-timeout", "1s",
		"-grpc-port", fmt.Sprint(grpcPort),
		"-health-port", fmt.Sprint(healthPort),
		"-metrics-port", fmt.Sprint(metricsPort),
		"-bind-address", "127.0.0.1",
	}
	if !opts.EnvVar {
		args = append(args, "-local-config-dir", configDir)
	}
	if opts.APIAddr != "" {
		args = append(args, "-api-addr", opts.APIAddr)
	}
	args = append(args, opts.ExtraArgs...)

	var env []string
	if opts.EnvVar {
		env = append(env, "AI_GATEWAY_EPP_LOCAL_CONFIG_DIR="+configDir)
	}
	proc := StartProcessWithEnv(t, logDir, "epp", env, args...)
	e := &EppEnv{
		Proc:        proc,
		GRPCAddr:    fmt.Sprintf("127.0.0.1:%d", grpcPort),
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", healthPort),
		MetricsAddr: fmt.Sprintf("127.0.0.1:%d", metricsPort),
		MetricsPort: metricsPort,
	}
	if err := WaitForTCP(e.GRPCAddr, 60*time.Second); err != nil {
		proc.Stop(t)
		t.Fatalf("epp not ready: %v (log: %s)", err, proc.LogPath())
	}
	return e
}

type localEnvConfig struct {
	eppArgs     []string
	eppConfigFn func(cluster string) json.RawMessage
	roles       map[string]string // cluster -> "primary" (default) / "standby"
	useEnvVar   bool
	observedAPI bool
	skipPool    bool
}

// LocalEnvOption customizes NewLocalEnv.
type LocalEnvOption func(*localEnvConfig)

// WithLocalEppArgs appends extra command-line flags for the epp process.
func WithLocalEppArgs(args ...string) LocalEnvOption {
	return func(c *localEnvConfig) { c.eppArgs = append(c.eppArgs, args...) }
}

// WithLocalEppConfigFn overrides the per-cluster epp_config generator
// (default: LocalEppConfig).
func WithLocalEppConfigFn(fn func(cluster string) json.RawMessage) LocalEnvOption {
	return func(c *localEnvConfig) { c.eppConfigFn = fn }
}

// WithLocalRoles sets the role this instance holds per cluster ("primary" or
// "standby"); clusters not listed are primary.
func WithLocalRoles(roles map[string]string) LocalEnvOption {
	return func(c *localEnvConfig) { c.roles = roles }
}

// WithLocalConfigEnvVar passes the config dir via
// AI_GATEWAY_EPP_LOCAL_CONFIG_DIR instead of --local-config-dir.
func WithLocalConfigEnvVar() LocalEnvOption {
	return func(c *localEnvConfig) { c.useEnvVar = true }
}

// WithLocalObservedAPI starts an in-process mock InnerAPI and points epp at
// it via -api-addr, so tests can assert the local mode never contacts it.
func WithLocalObservedAPI() LocalEnvOption {
	return func(c *localEnvConfig) { c.observedAPI = true }
}

// WithoutLocalPoolFile omits epp_pool.json from the config dir (the file is
// reference-only for epp).
func WithoutLocalPoolFile() LocalEnvOption {
	return func(c *localEnvConfig) { c.skipPool = true }
}

// NewLocalEnv starts epp in local-config-dir mode. clusters maps a cluster
// name to logical backend names; each logical name gets one reserved loopback
// endpoint (no listener). Two of the three config files are consumed by epp
// (cluster_table.json, epp_data_config.json); epp_pool.json is written for
// parity with the documented layout unless WithoutLocalPoolFile is given.
func NewLocalEnv(t *testing.T, instanceID string, clusters map[string][]string, opts ...LocalEnvOption) *LocalEnv {
	t.Helper()
	cfg := &localEnvConfig{eppConfigFn: LocalEppConfig}
	for _, o := range opts {
		o(cfg)
	}

	logDir := t.TempDir()
	configDir := t.TempDir()

	// Reserve one loopback endpoint per logical backend name. FindFreePort
	// closes the listener before returning, so the port stays unbound: it is
	// a pure placeholder address that epp registers from cluster_table.json.
	reserved := map[string]string{}
	clusterSims := map[string][]string{}
	table := map[string]map[string][]map[string]any{}
	clusterList := make([]string, 0, len(clusters))
	for cluster, names := range clusters {
		clusterList = append(clusterList, cluster)
		backends := make([]map[string]any, 0, len(names))
		for _, name := range names {
			addr, ok := reserved[name]
			if !ok {
				addr = fmt.Sprintf("127.0.0.1:%d", FindFreePort(t))
				reserved[name] = addr
			}
			clusterSims[cluster] = append(clusterSims[cluster], addr)
			backends = append(backends, map[string]any{
				"Name":   cluster + "-" + name,
				"Addr":   "127.0.0.1",
				"Port":   PortOf(addr),
				"Weight": 50,
			})
		}
		table[cluster] = map[string][]map[string]any{"sub-1": backends}
	}

	e := &LocalEnv{
		InstanceID:  instanceID,
		ConfigDir:   configDir,
		Clusters:    clusterList,
		ClusterSims: clusterSims,
	}
	WriteClusterTableFile(t, configDir, table)
	WriteEppDataConfigFile(t, configDir, EppDataConfigFor(instanceID, clusterList, cfg.roles, cfg.eppConfigFn))
	if !cfg.skipPool {
		WriteEppPoolFile(t, configDir, instanceID)
	}

	epOpts := LocalEPPOptions{EnvVar: cfg.useEnvVar, ExtraArgs: cfg.eppArgs}
	if cfg.observedAPI {
		e.API = NewMockAPI(t)
		epOpts.APIAddr = e.API.Addr()
	}
	e.EPP = StartLocalEPP(t, logDir, configDir, instanceID, epOpts)
	e.EPP.WaitHealth(t, "", 30*time.Second)
	return e
}

// LocalEppConfig is the default minimal epp_config generator for local-file
// scenarios (same shape as EppConfig):
func LocalEppConfig(cluster string) json.RawMessage { return EppConfig(cluster, false) }

// EppDataConfigFor builds the epp_data snapshot: every cluster gets a config
// from configFn and an assignment entry giving instanceID a role from roles
// ("standby" leaves a peer as primary; anything else makes instanceID
// primary).
func EppDataConfigFor(instanceID string, clusters []string, roles map[string]string, configFn func(cluster string) json.RawMessage) innerapi.EppDataConfig {
	cfg := innerapi.EppDataConfig{
		EppConfig:  make(map[string]json.RawMessage, len(clusters)),
		Assignment: make(map[string]innerapi.AssignmentEntry, len(clusters)),
	}
	for _, c := range clusters {
		if configFn != nil {
			cfg.EppConfig[c] = configFn(c)
		}
		if roles[c] == "standby" {
			cfg.Assignment[c] = innerapi.AssignmentEntry{Primary: sp("peer-" + instanceID), Standby: sp(instanceID)}
		} else {
			cfg.Assignment[c] = innerapi.AssignmentEntry{Primary: sp(instanceID)}
		}
	}
	return cfg
}

// PoolGroup / PoolInstance mirror the OpenAPI epp-pool Data layer.
type PoolGroup struct {
	Name      string         `json:"name"`
	Instances []PoolInstance `json:"instances"`
}

// PoolInstance is one EPP instance entry.
type PoolInstance struct {
	ID   string `json:"id"`
	Host string `json:"host"`
	Port int    `json:"port"`
}

// WriteClusterTableFile writes cluster_table.json in the InnerAPI
// ClusterTableConfig shape (cluster -> subCluster -> []BackendConf).
func WriteClusterTableFile(t fatalT, dir string, table map[string]map[string][]map[string]any) {
	t.Helper()
	writeJSONFile(t, filepath.Join(dir, "cluster_table.json"), table)
}

// WriteEppDataConfigFile writes epp_data_config.json (innerapi.EppDataConfig:
// epp_config + assignment).
func WriteEppDataConfigFile(t fatalT, dir string, cfg innerapi.EppDataConfig) {
	t.Helper()
	writeJSONFile(t, filepath.Join(dir, "epp_data_config.json"), cfg)
}

// WriteEppPoolFile writes epp_pool.json containing the given instance ids
// (reference-only for epp).
func WriteEppPoolFile(t fatalT, dir string, ids ...string) {
	t.Helper()
	insts := make([]PoolInstance, 0, len(ids))
	for _, id := range ids {
		insts = append(insts, PoolInstance{ID: id, Host: "127.0.0.1", Port: 9002})
	}
	writeJSONFile(t, filepath.Join(dir, "epp_pool.json"), map[string]any{
		"name":   "EPP.pool",
		"groups": []PoolGroup{{Name: "epp-local", Instances: insts}},
	})
}

// LocalBackendTable builds the cluster_table shape for clusters backed by the
// given reserved endpoint addresses ("127.0.0.1:port").
func LocalBackendTable(clusterSims map[string][]string) map[string]map[string][]map[string]any {
	table := make(map[string]map[string][]map[string]any, len(clusterSims))
	for cluster, addrs := range clusterSims {
		backends := make([]map[string]any, 0, len(addrs))
		for i, addr := range addrs {
			backends = append(backends, map[string]any{
				"Name":   fmt.Sprintf("%s-%d", cluster, i),
				"Addr":   "127.0.0.1",
				"Port":   PortOf(addr),
				"Weight": 50,
			})
		}
		table[cluster] = map[string][]map[string]any{"sub-1": backends}
	}
	return table
}

func writeJSONFile(t fatalT, path string, v any) {
	t.Helper()
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal %s: %v", path, err)
	}
	if err := os.WriteFile(path, raw, 0644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
