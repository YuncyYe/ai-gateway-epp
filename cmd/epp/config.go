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

// Command epp is the ai-gateway endpoint picker: a multi-cluster,
// config-driven recomposition of the llm-d EPP that consumes ai-gateway-api
// InnerAPI instead of Kubernetes CRDs.
package main

import (
	"flag"
	"os"
	"time"
)

var (
	showVersion = flag.Bool("v", false, "to show version of epp")
	showVerbose = flag.Bool("V", false, "to show verbose information about epp")
)

// Config is the process-level configuration; everything cluster-level comes
// from ai-gateway-api at runtime.
type Config struct {
	InstanceID   string
	APIAddr      string
	APIToken     string
	PollInterval time.Duration
	PollTimeout  time.Duration

	GRPCPort    int
	HealthPort  int
	MetricsPort int
	EnablePprof bool
	// GRPCTLSCertFile / GRPCTLSKeyFile enable server-side TLS on the ext-proc
	// and health gRPC servers when both are set (empty = plaintext).
	GRPCTLSCertFile string
	GRPCTLSKeyFile  string
	// BindAddress is the listen address for gRPC/health/metrics servers
	// ("" = all interfaces, "127.0.0.1" = loopback only).
	BindAddress string

	DefaultPool        string
	EngineDrainTimeout time.Duration
	PoolNamespace      string

	RefreshMetricsInterval   time.Duration
	AllowExperimentalPlugins bool
}

func parseConfig() Config {
	cfg := Config{}
	flag.StringVar(&cfg.APIAddr, "api-addr", getenv("AI_GATEWAY_API_ADDR", "http://127.0.0.1:8181/inner-api/v1"), "ai-gateway-api InnerAPI base URL")
	flag.StringVar(&cfg.APIToken, "api-token", os.Getenv("AI_GATEWAY_API_TOKEN"), "InnerAPI auth token (env AI_GATEWAY_API_TOKEN takes precedence)")
	flag.StringVar(&cfg.InstanceID, "instance-id", os.Getenv("AI_GATEWAY_EPP_INSTANCE_ID"), "instance identifier for assignment (default: hostname)")
	flag.DurationVar(&cfg.PollInterval, "poll-interval", 5*time.Second, "poll interval for InnerAPI sync")
	flag.DurationVar(&cfg.PollTimeout, "poll-timeout", 3*time.Second, "per-request timeout for InnerAPI sync")
	flag.IntVar(&cfg.GRPCPort, "grpc-port", 9002, "ext-proc gRPC port")
	flag.IntVar(&cfg.HealthPort, "health-port", 9003, "gRPC health port")
	flag.IntVar(&cfg.MetricsPort, "metrics-port", 9090, "HTTP metrics port")
	flag.BoolVar(&cfg.EnablePprof, "enable-pprof", false, "expose pprof on the metrics port")
	flag.StringVar(&cfg.GRPCTLSCertFile, "grpc-tls-cert", os.Getenv("AI_GATEWAY_EPP_TLS_CERT"), "TLS certificate file for the ext-proc/health gRPC servers (env AI_GATEWAY_EPP_TLS_CERT takes precedence; empty = plaintext)")
	flag.StringVar(&cfg.GRPCTLSKeyFile, "grpc-tls-key", os.Getenv("AI_GATEWAY_EPP_TLS_KEY"), "TLS private key file for the ext-proc/health gRPC servers (env AI_GATEWAY_EPP_TLS_KEY takes precedence)")
	flag.StringVar(&cfg.BindAddress, "bind-address", "", "listen address for gRPC/health/metrics (empty = all interfaces)")
	flag.StringVar(&cfg.DefaultPool, "default-pool", "", "fallback cluster when a request has no pool metadata (empty = reject)")
	flag.DurationVar(&cfg.EngineDrainTimeout, "engine-drain-timeout", 60*time.Second, "max wait for in-flight requests when draining an engine")
	flag.StringVar(&cfg.PoolNamespace, "pool-namespace", getenv("NAMESPACE", "ai-gateway"), "endpoint pool namespace label for metrics")
	flag.DurationVar(&cfg.RefreshMetricsInterval, "refresh-metrics-interval", 50*time.Millisecond, "datalayer metrics polling interval")
	flag.BoolVar(&cfg.AllowExperimentalPlugins, "allow-experimental-plugins", false, "allow Alpha-stability plugins")
	flag.Parse()

	if cfg.InstanceID == "" {
		if h, err := os.Hostname(); err == nil {
			cfg.InstanceID = h
		} else {
			cfg.InstanceID = "epp"
		}
	}
	return cfg
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
