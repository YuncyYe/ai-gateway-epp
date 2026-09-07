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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestServerOptionsPlaintext(t *testing.T) {
	opts, err := serverOptions(Config{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if opts != nil {
		t.Fatalf("expected nil options for plaintext, got %v", opts)
	}
}

func TestServerOptionsCertKeyMustComeTogether(t *testing.T) {
	for _, cfg := range []Config{
		{GRPCTLSCertFile: "/tmp/cert.pem"},
		{GRPCTLSKeyFile: "/tmp/key.pem"},
	} {
		if _, err := serverOptions(cfg); err == nil {
			t.Fatalf("expected error for %+v", cfg)
		}
	}
}

func TestServerOptionsBadFiles(t *testing.T) {
	dir := t.TempDir()
	cert := filepath.Join(dir, "cert.pem")
	key := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(cert, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(key, []byte("not a pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := serverOptions(Config{GRPCTLSCertFile: cert, GRPCTLSKeyFile: key}); err == nil {
		t.Fatal("expected error for invalid PEM files")
	}
}

// TestTLSHealthOnExtProcPort mirrors the production wiring: one server carries
// both the ext-proc service and the health service, served over TLS when a
// certificate is configured.
func TestTLSHealthOnExtProcPort(t *testing.T) {
	dir := t.TempDir()
	certFile, keyFile := writeSelfSignedCert(t, dir)

	opts, err := serverOptions(Config{GRPCTLSCertFile: certFile, GRPCTLSKeyFile: keyFile})
	if err != nil {
		t.Fatalf("serverOptions: %v", err)
	}
	srv := grpc.NewServer(opts...)
	extProcPb.RegisterExternalProcessorServer(srv, &stubExtProc{})
	healthpb.RegisterHealthServer(srv, &stubHealth{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	cert, err := os.ReadFile(certFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(cert) {
		t.Fatal("failed to append CA")
	}
	creds := credentials.NewClientTLSFromCert(pool, "")

	// Plaintext dial to a TLS server must fail.
	if conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials())); err == nil {
		hc := healthpb.NewHealthClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, herr := hc.Check(ctx, &healthpb.HealthCheckRequest{})
		cancel()
		conn.Close()
		if herr == nil {
			t.Fatal("expected plaintext dial to TLS server to fail")
		}
	}

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health check over TLS: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %v", resp.GetStatus())
	}
}

type stubExtProc struct {
	extProcPb.UnimplementedExternalProcessorServer
}

type stubHealth struct {
	healthpb.UnimplementedHealthServer
}

func (s *stubHealth) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// writeSelfSignedCert generates a self-signed ECDSA certificate valid for
// 127.0.0.1 and writes cert/key PEM files into dir.
func writeSelfSignedCert(t *testing.T, dir string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "ai-gateway-epp-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, "cert.pem")
	keyFile = filepath.Join(dir, "key.pem")
	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatal(err)
	}
	defer certOut.Close()
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile
}
