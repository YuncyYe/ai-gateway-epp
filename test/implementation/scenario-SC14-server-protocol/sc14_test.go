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

// Package sc14: SC14 服务端协议契约（主端口 health + gRPC TLS）。
// 测试设计文档：test/测试设计文档/scenario-SC14-服务端协议契约/
package sc14

import (
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/rainway-ai-gateway/ai-gateway-epp/test/common"
)

const chatBody = `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc14"}],"max_tokens":8}`

func installCluster(api *common.MockAPI, simAddr string) {
	api.SetClusterTable(map[string]map[string][]map[string]any{
		"cluster-a": {"sub-1": common.BackendMap(common.Backend{
			Name: "cluster-a-a0", Addr: "127.0.0.1", Port: common.PortOf(simAddr), Weight: 50,
		})},
	})
	api.SetAssignment(map[string]string{"cluster-a": "primary"})
	api.SetConfigs(map[string]json.RawMessage{"cluster-a": common.PickerConfig("cluster-a", false)})
}

func waitHealthTLS(t *testing.T, addr string, pool *x509.CertPool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		st, err := common.CheckHealthTLS(addr, "", pool)
		if err == nil && st == healthpb.HealthCheckResponse_SERVING {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("health %q not SERVING over TLS: last=%v err=%v", addr, st, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestTC01_HealthOnExtProcPort: gRPC health is served on the ext-proc (data)
// port too, gating on the same readiness as the dedicated health port — the
// contract BFE's failover health check relies on.
func TestTC01_HealthOnExtProcPort(t *testing.T) {
	api := common.NewMockAPI(t)
	defer api.Close()
	api.SetFail(true) // hold readiness until we install the cluster config

	logDir := t.TempDir()
	epp := common.StartEPP(t, logDir, api.Addr(), "epp-sc14-tc01")
	defer epp.Proc.Stop(t)

	// While the inner API is failing, readiness is NOT_SERVING on BOTH ports.
	if st, err := common.CheckHealth(epp.GRPCAddr, ""); err != nil || st == healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("ext-proc port health = %v, %v; want NOT_SERVING before first sync", st, err)
	}
	if st, err := common.CheckHealth(epp.HealthAddr, ""); err != nil || st == healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health port health = %v, %v; want NOT_SERVING before first sync", st, err)
	}

	sim, simAddr := common.StartSim(t, logDir, "sim-a0", "sim-model")
	defer sim.Stop(t)
	installCluster(api, simAddr)
	api.SetFail(false)

	epp.WaitHealth(t, "", 30*time.Second)
	if st, err := common.CheckHealth(epp.GRPCAddr, ""); err != nil || st != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("ext-proc port health = %v, %v; want SERVING after ready", st, err)
	}
	if st, err := common.CheckHealth(epp.GRPCAddr, "liveness"); err != nil || st != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("ext-proc port liveness = %v, %v; want SERVING", st, err)
	}
	// ext-proc data path unaffected by the health registration.
	ep, err := common.PickEndpoint(epp.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second)
	if err != nil {
		t.Fatalf("pick: %v (log: %s)", err, epp.Proc.LogPath())
	}
	if ep != simAddr {
		t.Fatalf("pick = %q, want %q", ep, simAddr)
	}
}

// TestTC02_GrpcTLS: with -grpc-tls-cert/-grpc-tls-key both gRPC servers speak
// TLS: plaintext dials are rejected, TLS picks and health checks succeed.
func TestTC02_GrpcTLS(t *testing.T) {
	certFile, keyFile, pool := common.WriteSelfSignedCert(t, t.TempDir())

	api := common.NewMockAPI(t)
	defer api.Close()

	logDir := t.TempDir()
	sim, simAddr := common.StartSim(t, logDir, "sim-a0", "sim-model")
	defer sim.Stop(t)
	installCluster(api, simAddr)

	epp := common.StartEPP(t, logDir, api.Addr(), "epp-sc14-tc02",
		"-grpc-tls-cert", certFile, "-grpc-tls-key", keyFile)
	defer epp.Proc.Stop(t)

	// NewEnv's plaintext WaitHealth cannot be used against a TLS server, so
	// readiness is awaited over TLS here.
	waitHealthTLS(t, epp.HealthAddr, pool, 30*time.Second)
	waitHealthTLS(t, epp.GRPCAddr, pool, 5*time.Second)

	// Plaintext ext-proc dial must fail against a TLS server.
	if _, err := common.PickEndpoint(epp.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 3*time.Second); err == nil {
		t.Fatal("expected plaintext pick to TLS server to fail")
	}
	// Plaintext health check must fail too.
	if _, err := common.CheckHealth(epp.GRPCAddr, ""); err == nil {
		t.Fatal("expected plaintext health check to TLS server to fail")
	}

	ep, err := common.PickEndpointTLS(epp.GRPCAddr, "cluster-a", "/v1/chat/completions", []byte(chatBody), 10*time.Second, pool)
	if err != nil {
		t.Fatalf("TLS pick: %v (log: %s)", err, epp.Proc.LogPath())
	}
	if ep != simAddr {
		t.Fatalf("TLS pick = %q, want %q", ep, simAddr)
	}
}

// TestTC03_TLSConfigFailFast: setting only -grpc-tls-cert makes the process
// exit immediately with a clear error.
func TestTC03_TLSConfigFailFast(t *testing.T) {
	api := common.NewMockAPI(t)
	defer api.Close()

	logDir := t.TempDir()
	certFile, _, _ := common.WriteSelfSignedCert(t, logDir)

	proc := common.StartProcess(t, logDir, "epp",
		common.EppBinary(t),
		"-api-addr", api.Addr(),
		"-instance-id", "epp-sc14-tc03",
		"-grpc-tls-cert", certFile, // key deliberately missing
		"-grpc-port", fmt.Sprint(common.FindFreePort(t)),
		"-health-port", fmt.Sprint(common.FindFreePort(t)),
		"-metrics-port", fmt.Sprint(common.FindFreePort(t)),
		"-bind-address", "127.0.0.1",
	)
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- proc.Cmd.Wait() }()
	select {
	case err := <-done:
		t.Logf("epp exited after %v", time.Since(start))
		if err == nil {
			t.Fatal("expected non-zero exit when only -grpc-tls-cert is set")
		}
	case <-time.After(15 * time.Second):
		proc.Stop(t)
		t.Fatal("epp did not exit within 15s with incomplete TLS config")
	}
	// Close the log file handle (the process is already gone; Kill/Wait are
	// no-ops on an exited child) so the test's TempDir cleanup can remove it.
	proc.Stop(t)
	logBytes, rerr := os.ReadFile(proc.LogPath())
	if rerr != nil {
		t.Fatalf("read epp log: %v", rerr)
	}
	if !strings.Contains(string(logBytes), "grpc-tls-cert and grpc-tls-key must be set together") {
		t.Fatalf("epp log missing fail-fast message:\n%s", logBytes)
	}
}
