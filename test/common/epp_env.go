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
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// EppEnv holds the addresses of a started epp process.
type EppEnv struct {
	Proc        *Process
	GRPCAddr    string // ext-proc
	HealthAddr  string
	MetricsAddr string // http /metrics
	MetricsPort int
}

// StartEPP builds (if needed) and starts the epp binary against the given
// mock API, waiting for the gRPC port to accept connections.
func StartEPP(t fatalT, logDir, apiAddr, instanceID string, extraArgs ...string) *EppEnv {
	t.Helper()
	bin := EppBinary(t)
	grpcPort := FindFreePort(t)
	healthPort := FindFreePort(t)
	metricsPort := FindFreePort(t)
	args := []string{
		bin,
		"-api-addr", apiAddr,
		"-instance-id", instanceID,
		"-poll-interval", "200ms",
		"-poll-timeout", "1s",
		"-grpc-port", fmt.Sprint(grpcPort),
		"-health-port", fmt.Sprint(healthPort),
		"-metrics-port", fmt.Sprint(metricsPort),
		"-bind-address", "127.0.0.1",
	}
	args = append(args, extraArgs...)
	proc := StartProcess(t, logDir, "epp", args...)
	env := &EppEnv{
		Proc:        proc,
		GRPCAddr:    fmt.Sprintf("127.0.0.1:%d", grpcPort),
		HealthAddr:  fmt.Sprintf("127.0.0.1:%d", healthPort),
		MetricsAddr: fmt.Sprintf("127.0.0.1:%d", metricsPort),
		MetricsPort: metricsPort,
	}
	// Generous startup window: under parallel package runs (go test -p N),
	// every test binary links its own epp copy and CPU contention can delay
	// process startup well beyond the time a single run needs.
	if err := WaitForTCP(env.GRPCAddr, 60*time.Second); err != nil {
		proc.Stop(t)
		t.Fatalf("epp not ready: %v (log: %s)", err, proc.LogPath())
	}
	return env
}

// WaitHealth polls the gRPC health endpoint until SERVING or timeout. service
// "" checks overall readiness; "liveness" always reports SERVING.
func (e *EppEnv) WaitHealth(t fatalT, service string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		status, err := e.checkHealth(service)
		if err == nil && status == healthpb.HealthCheckResponse_SERVING {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health %q not SERVING: last=%v err=%v (log: %s)", service, status, err, e.Proc.LogPath())
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (e *EppEnv) checkHealth(service string) (healthpb.HealthCheckResponse_ServingStatus, error) {
	return CheckHealth(e.HealthAddr, service)
}

// CheckHealth performs a single plaintext gRPC health check against an
// arbitrary address. service "" checks overall readiness; "liveness" always
// reports SERVING.
func CheckHealth(addr, service string) (healthpb.HealthCheckResponse_ServingStatus, error) {
	return checkHealthWithCreds(addr, service, insecure.NewCredentials())
}

// CheckHealthTLS is CheckHealth over TLS, verifying the server against pool.
func CheckHealthTLS(addr, service string, pool *x509.CertPool) (healthpb.HealthCheckResponse_ServingStatus, error) {
	return checkHealthWithCreds(addr, service, credentials.NewClientTLSFromCert(pool, ""))
}

func checkHealthWithCreds(addr, service string, creds credentials.TransportCredentials) (healthpb.HealthCheckResponse_ServingStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return healthpb.HealthCheckResponse_SERVICE_UNKNOWN, err
	}
	defer conn.Close()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{Service: service})
	if err != nil {
		return healthpb.HealthCheckResponse_SERVICE_UNKNOWN, err
	}
	return resp.Status, nil
}

// PickerConfig returns a minimal working picker config for a cluster. The
// cluster-table-discovery plugin is pinned to the given cluster name.
func PickerConfig(cluster string, flowControl bool) json.RawMessage {
	fc := ""
	if flowControl {
		fc = `
  "featureGates": ["flowControl"],`
	}
	return json.RawMessage(fmt.Sprintf(`{%s
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": %q}},
    {"name": "kv-scorer", "type": "kv-cache-utilization-scorer", "parameters": {}},
    {"name": "queue-scorer", "type": "queue-scorer", "parameters": {}},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [
      {"pluginRef": "kv-scorer"},
      {"pluginRef": "queue-scorer"},
      {"pluginRef": "max-score"}
    ]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`, fc, cluster))
}
