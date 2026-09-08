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
	"testing"
	"time"
)

// Env is a full integration-test environment: in-process mock ai-gateway-api,
// one real inference-sim per logical backend name, and one real ai-gateway-epp
// process. Modeled after bfe/tests/integration/common.
type Env struct {
	API         *MockAPI
	Sims        []*Process
	ClusterSims map[string][]string // cluster -> sim addrs ("127.0.0.1:port")
	EPP         *EppEnv
}

// Close stops epp, all sims, and the mock API.
func (e *Env) Close(t *testing.T) {
	t.Helper()
	if e.EPP != nil {
		e.EPP.Proc.Stop(t)
	}
	for _, s := range e.Sims {
		s.Stop(t)
	}
	if e.API != nil {
		e.API.Close()
	}
}

type envConfig struct {
	eppArgs      []string
	eppConfigFn  func(cluster string) json.RawMessage
	simFake      map[string]string // logical sim name -> initial --fake-metrics JSON
}

// EnvOption customizes NewEnv.
type EnvOption func(*envConfig)

// WithEppArgs appends extra command-line flags for the epp process.
func WithEppArgs(args ...string) EnvOption {
	return func(c *envConfig) { c.eppArgs = append(c.eppArgs, args...) }
}

// WithEppConfigFn overrides the per-cluster epp_config generator
// (default: EppConfig).
func WithEppConfigFn(fn func(cluster string) json.RawMessage) EnvOption {
	return func(c *envConfig) { c.eppConfigFn = fn }
}

// WithSimFakeMetrics starts the named logical sims with fake metrics enabled
// (initial JSON per name, e.g. "{}"). Required for /admin/config fake-metrics
// updates to be accepted later.
func WithSimFakeMetrics(fake map[string]string) EnvOption {
	return func(c *envConfig) { c.simFake = fake }
}

// NewEnv starts mock API + one sim per logical name + epp. clusters maps a
// cluster name to the logical sim names backing it; a logical name appearing
// in multiple clusters shares one sim process. Every cluster is assigned
// "primary" to this instance and gets a minimal working epp_config.
func NewEnv(t *testing.T, instanceID string, clusters map[string][]string, opts ...EnvOption) *Env {
	t.Helper()
	cfg := &envConfig{eppConfigFn: func(cluster string) json.RawMessage { return EppConfig(cluster, false) }}
	for _, o := range opts {
		o(cfg)
	}

	logDir := t.TempDir()
	e := &Env{ClusterSims: map[string][]string{}}

	e.API = NewMockAPI(t)

	simAddr := map[string]string{}
	startSim := func(name string) {
		if _, ok := simAddr[name]; ok {
			return
		}
		var proc *Process
		var addr string
		if fake, ok := cfg.simFake[name]; ok {
			proc, addr = StartSimWithFakeMetrics(t, logDir, "sim-"+name, "sim-model", fake)
		} else {
			proc, addr = StartSim(t, logDir, "sim-"+name, "sim-model")
		}
		simAddr[name] = addr
		e.Sims = append(e.Sims, proc)
	}

	table := map[string]map[string][]map[string]any{}
	assign := map[string]string{}
	configs := map[string]json.RawMessage{}
	for cluster, names := range clusters {
		backends := []Backend{}
		for _, name := range names {
			startSim(name)
			addr := simAddr[name]
			e.ClusterSims[cluster] = append(e.ClusterSims[cluster], addr)
			backends = append(backends, Backend{
				Name:   cluster + "-" + name,
				Addr:   "127.0.0.1",
				Port:   PortOf(addr),
				Weight: 50,
			})
		}
		table[cluster] = map[string][]map[string]any{"sub-1": BackendMap(backends...)}
		assign[cluster] = "primary"
		configs[cluster] = cfg.eppConfigFn(cluster)
	}
	e.API.SetDefaultInstance(instanceID)
	e.API.SetAssignment(assign)
	e.API.SetEppConfig(configs)
	e.API.SetClusterTable(table)

	e.EPP = StartEPP(t, logDir, e.API.Addr(), instanceID, cfg.eppArgs...)
	e.EPP.WaitHealth(t, "", 30*time.Second)
	return e
}

// PortOf extracts the port from a "127.0.0.1:port" address.
func PortOf(addr string) int {
	var p int
	fmt.Sscanf(addr, "127.0.0.1:%d", &p)
	return p
}
