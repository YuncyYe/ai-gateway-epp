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
	"context"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServeGRPCListenError(t *testing.T) {
	// Occupy a port, then ask serveGRPC to bind the same address: the listen
	// must fail and the error must identify the listen phase.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	port := lis.Addr().(*net.TCPAddr).Port

	err = serveGRPC(context.Background(), grpc.NewServer(), "127.0.0.1", port)
	if err == nil {
		t.Fatal("expected error when the port is already in use")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error = %v, want it to mention the listen failure", err)
	}
}

func TestServeGRPCInvalidBindAddress(t *testing.T) {
	err := serveGRPC(context.Background(), grpc.NewServer(), "256.256.256.256", 9002)
	if err == nil {
		t.Fatal("expected error for an unbindable address")
	}
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error = %v, want it to mention the listen failure", err)
	}
}

// TestServeGRPCContextCancel mirrors the production wiring: serveGRPC runs in
// a goroutine until the context is cancelled, which stops the server and
// lets the call return nil.
func TestServeGRPCContextCancel(t *testing.T) {
	// Reserve a free port, then release it for serveGRPC. Go listeners set
	// SO_REUSEADDR, so rebinding is reliable even in TIME_WAIT.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	probe.Close()

	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, &stubHealth{})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- serveGRPC(ctx, srv, "127.0.0.1", port)
	}()

	// Wait until the server is actually accepting and answers a health check.
	var lastErr error
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := grpc.Dial(
			net.JoinHostPort("127.0.0.1", strconv.Itoa(port)),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err == nil {
			checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Second)
			_, lastErr = healthpb.NewHealthClient(conn).Check(checkCtx, &healthpb.HealthCheckRequest{})
			checkCancel()
			conn.Close()
			if lastErr == nil {
				break
			}
		} else {
			lastErr = err
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("server did not become ready: %v", lastErr)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("serveGRPC after cancel = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serveGRPC did not return after context cancellation")
	}
}
