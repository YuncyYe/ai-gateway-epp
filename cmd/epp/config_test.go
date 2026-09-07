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

package main

import (
	"flag"
	"os"
	"testing"
	"time"
)

// parseConfig mutates the global flag.CommandLine and reads process env, so
// each invocation runs against a fresh FlagSet and the originals are restored
// on cleanup. Tests here must not use t.Parallel.
func parseConfigForTest(t *testing.T, args ...string) Config {
	t.Helper()
	oldCommandLine := flag.CommandLine
	oldArgs := os.Args
	t.Cleanup(func() {
		flag.CommandLine = oldCommandLine
		os.Args = oldArgs
	})
	flag.CommandLine = flag.NewFlagSet("epp-test", flag.ContinueOnError)
	os.Args = append([]string{"epp"}, args...)
	return parseConfig()
}

// clearConfigEnv blanks every env var parseConfig consults so a test starts
// from a deterministic baseline. An empty value behaves as unset for every
// lookup parseConfig performs (getenv treats "" as unset; plain os.Getenv
// yields "").
func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"AI_GATEWAY_API_ADDR",
		"AI_GATEWAY_API_TOKEN",
		"AI_GATEWAY_EPP_INSTANCE_ID",
		"AI_GATEWAY_EPP_TLS_CERT",
		"AI_GATEWAY_EPP_TLS_KEY",
		"NAMESPACE",
	} {
		t.Setenv(key, "")
	}
}

func TestGetenv(t *testing.T) {
	const key = "AI_GATEWAY_EPP_TEST_GETENV"
	orig, existed := os.LookupEnv(key)
	t.Cleanup(func() {
		if existed {
			os.Setenv(key, orig)
		} else {
			os.Unsetenv(key)
		}
	})

	if got := getenv(key, "fallback"); got != "fallback" {
		t.Errorf("unset: getenv = %q, want %q", got, "fallback")
	}
	os.Setenv(key, "")
	if got := getenv(key, "fallback"); got != "fallback" {
		t.Errorf("empty: getenv = %q, want %q", got, "fallback")
	}
	os.Setenv(key, "value")
	if got := getenv(key, "fallback"); got != "value" {
		t.Errorf("set: getenv = %q, want %q", got, "value")
	}
}

func TestParseConfigDefaults(t *testing.T) {
	clearConfigEnv(t)
	cfg := parseConfigForTest(t)

	if cfg.APIAddr != "http://127.0.0.1:8181/inner-api/v1" {
		t.Errorf("APIAddr = %q, want default", cfg.APIAddr)
	}
	if cfg.APIToken != "" {
		t.Errorf("APIToken = %q, want empty", cfg.APIToken)
	}
	if cfg.PollInterval != 5*time.Second {
		t.Errorf("PollInterval = %v, want 5s", cfg.PollInterval)
	}
	if cfg.PollTimeout != 3*time.Second {
		t.Errorf("PollTimeout = %v, want 3s", cfg.PollTimeout)
	}
	if cfg.GRPCPort != 9002 {
		t.Errorf("GRPCPort = %d, want 9002", cfg.GRPCPort)
	}
	if cfg.HealthPort != 9003 {
		t.Errorf("HealthPort = %d, want 9003", cfg.HealthPort)
	}
	if cfg.MetricsPort != 9090 {
		t.Errorf("MetricsPort = %d, want 9090", cfg.MetricsPort)
	}
	if cfg.EnablePprof {
		t.Error("EnablePprof = true, want false")
	}
	if cfg.GRPCTLSCertFile != "" || cfg.GRPCTLSKeyFile != "" {
		t.Errorf("TLS files = %q/%q, want empty", cfg.GRPCTLSCertFile, cfg.GRPCTLSKeyFile)
	}
	if cfg.BindAddress != "" {
		t.Errorf("BindAddress = %q, want empty", cfg.BindAddress)
	}
	if cfg.DefaultPool != "" {
		t.Errorf("DefaultPool = %q, want empty", cfg.DefaultPool)
	}
	if cfg.EngineDrainTimeout != 60*time.Second {
		t.Errorf("EngineDrainTimeout = %v, want 60s", cfg.EngineDrainTimeout)
	}
	if cfg.PoolNamespace != "ai-gateway" {
		t.Errorf("PoolNamespace = %q, want ai-gateway", cfg.PoolNamespace)
	}
	if cfg.RefreshMetricsInterval != 50*time.Millisecond {
		t.Errorf("RefreshMetricsInterval = %v, want 50ms", cfg.RefreshMetricsInterval)
	}
	if cfg.AllowExperimentalPlugins {
		t.Error("AllowExperimentalPlugins = true, want false")
	}

	// InstanceID falls back to the hostname when neither env nor flag set it.
	host, err := os.Hostname()
	if err != nil {
		t.Skipf("hostname unavailable: %v", err)
	}
	if cfg.InstanceID != host {
		t.Errorf("InstanceID = %q, want hostname %q", cfg.InstanceID, host)
	}
}

func TestParseConfigEnvOverrides(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("AI_GATEWAY_API_ADDR", "http://api.example.com/inner-api/v1")
	t.Setenv("AI_GATEWAY_API_TOKEN", "env-token")
	t.Setenv("AI_GATEWAY_EPP_INSTANCE_ID", "epp-env-instance")
	t.Setenv("AI_GATEWAY_EPP_TLS_CERT", "/env/cert.pem")
	t.Setenv("AI_GATEWAY_EPP_TLS_KEY", "/env/key.pem")
	t.Setenv("NAMESPACE", "env-namespace")

	cfg := parseConfigForTest(t)

	if cfg.APIAddr != "http://api.example.com/inner-api/v1" {
		t.Errorf("APIAddr = %q, want env value", cfg.APIAddr)
	}
	if cfg.APIToken != "env-token" {
		t.Errorf("APIToken = %q, want env value", cfg.APIToken)
	}
	if cfg.InstanceID != "epp-env-instance" {
		t.Errorf("InstanceID = %q, want env value", cfg.InstanceID)
	}
	if cfg.GRPCTLSCertFile != "/env/cert.pem" || cfg.GRPCTLSKeyFile != "/env/key.pem" {
		t.Errorf("TLS files = %q/%q, want env values", cfg.GRPCTLSCertFile, cfg.GRPCTLSKeyFile)
	}
	if cfg.PoolNamespace != "env-namespace" {
		t.Errorf("PoolNamespace = %q, want env value", cfg.PoolNamespace)
	}
}

func TestParseConfigFlagsOverrideEnv(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("AI_GATEWAY_API_ADDR", "http://env.example.com")
	t.Setenv("AI_GATEWAY_API_TOKEN", "env-token")
	t.Setenv("AI_GATEWAY_EPP_INSTANCE_ID", "env-instance")
	t.Setenv("NAMESPACE", "env-namespace")

	cfg := parseConfigForTest(t,
		"-api-addr", "http://flag.example.com",
		"-api-token", "flag-token",
		"-instance-id", "flag-instance",
		"-pool-namespace", "flag-namespace",
	)

	if cfg.APIAddr != "http://flag.example.com" {
		t.Errorf("APIAddr = %q, want flag value", cfg.APIAddr)
	}
	if cfg.APIToken != "flag-token" {
		t.Errorf("APIToken = %q, want flag value", cfg.APIToken)
	}
	if cfg.InstanceID != "flag-instance" {
		t.Errorf("InstanceID = %q, want flag value", cfg.InstanceID)
	}
	if cfg.PoolNamespace != "flag-namespace" {
		t.Errorf("PoolNamespace = %q, want flag value", cfg.PoolNamespace)
	}
}

func TestParseConfigFlags(t *testing.T) {
	clearConfigEnv(t)
	cfg := parseConfigForTest(t,
		"-poll-interval", "10s",
		"-poll-timeout", "1500ms",
		"-grpc-port", "19002",
		"-health-port", "19003",
		"-metrics-port", "19090",
		"-enable-pprof",
		"-bind-address", "127.0.0.1",
		"-default-pool", "fallback-pool",
		"-engine-drain-timeout", "30s",
		"-refresh-metrics-interval", "1s",
		"-allow-experimental-plugins",
	)

	if cfg.PollInterval != 10*time.Second {
		t.Errorf("PollInterval = %v, want 10s", cfg.PollInterval)
	}
	if cfg.PollTimeout != 1500*time.Millisecond {
		t.Errorf("PollTimeout = %v, want 1500ms", cfg.PollTimeout)
	}
	if cfg.GRPCPort != 19002 {
		t.Errorf("GRPCPort = %d, want 19002", cfg.GRPCPort)
	}
	if cfg.HealthPort != 19003 {
		t.Errorf("HealthPort = %d, want 19003", cfg.HealthPort)
	}
	if cfg.MetricsPort != 19090 {
		t.Errorf("MetricsPort = %d, want 19090", cfg.MetricsPort)
	}
	if !cfg.EnablePprof {
		t.Error("EnablePprof = false, want true")
	}
	if cfg.BindAddress != "127.0.0.1" {
		t.Errorf("BindAddress = %q, want 127.0.0.1", cfg.BindAddress)
	}
	if cfg.DefaultPool != "fallback-pool" {
		t.Errorf("DefaultPool = %q, want fallback-pool", cfg.DefaultPool)
	}
	if cfg.EngineDrainTimeout != 30*time.Second {
		t.Errorf("EngineDrainTimeout = %v, want 30s", cfg.EngineDrainTimeout)
	}
	if cfg.RefreshMetricsInterval != time.Second {
		t.Errorf("RefreshMetricsInterval = %v, want 1s", cfg.RefreshMetricsInterval)
	}
	if !cfg.AllowExperimentalPlugins {
		t.Error("AllowExperimentalPlugins = false, want true")
	}
}
