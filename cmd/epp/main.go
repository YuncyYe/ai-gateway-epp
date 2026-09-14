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
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/stdr"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	ctrlLog "sigs.k8s.io/controller-runtime/pkg/log"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	stdlog "log"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/demux"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/eppplugin/clustertable"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/poller"
)

var version string
var commit string

func main() {
	if err := run(); err != nil {
		stdlog.Fatal(err)
	}
}

func run() error {
	cfg := parseConfig()

	if *showVersion {
		fmt.Printf("epp version: %s\n", version)
		return nil
	}
	if *showVerbose {
		fmt.Printf("epp version: %s\n", version)
		fmt.Printf("go version: %s\n", runtime.Version())
		fmt.Printf("git commit: %s\n", commit)
		return nil
	}

	//stdlf := stdlog.LstdFlags | stdlog.Lshortfile
	stdlf := stdlog.LstdFlags | stdlog.Llongfile
	stdl := stdlog.New(os.Stderr, "", stdlf)
	logger := stdr.New(stdl).WithName("ai-gateway-epp")
	stdr.SetVerbosity(cfg.logVerbosity())

	// Bridge the EPP logger into controller-runtime's deferred logging so that
	// llm-d-router (handlers/server.go, flowcontrol, etc.) logs are visible.
	ctrlLog.SetLogger(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Composition root: plugins (global registries) -> hub -> manager -> pollers.
	hub := clustertable.NewHub()
	registerPlugins(hub)

	manager := cell.NewManager(ctx, cell.Options{
		Logger:                   logger,
		MetricsRecorder:          ctrlmetrics.Registry,
		PoolNamespace:            cfg.PoolNamespace,
		RefreshMetricsInterval:   cfg.RefreshMetricsInterval,
		DrainTimeout:             cfg.EngineDrainTimeout,
		AllowExperimentalPlugins: cfg.AllowExperimentalPlugins,
	})

	var discoverySrc poller.Source[map[string][]fwkdl.EndpointMetadata]
	var eppDataSrc poller.Source[innerapi.EppDataConfig]
	var discoveryHandle poller.Handle[map[string][]fwkdl.EndpointMetadata]
	var eppDataHandle poller.Handle[innerapi.EppDataConfig]

	if cfg.LocalConfigDir != "" {
		discoverySrc = poller.NewLocalClusterTableSource(cfg.LocalConfigDir, "cluster_table.json")
		eppDataSrc = poller.NewLocalFileSource[innerapi.EppDataConfig](cfg.LocalConfigDir, "epp_data_config.json")

		discovery := poller.NewClusterDiscovery(nil, hub, func(cluster string) bool {
			_, ok := manager.Get(cell.Key(cluster))
			return ok
		}, nil)
		eppData := poller.NewEppDataWatcher(nil, cfg.InstanceID, manager, logger)

		discoveryHandle = discovery.Handle
		eppDataHandle = eppData.Handle
	} else {
		client := innerapi.NewClient(cfg.APIAddr, cfg.APIToken, cfg.PollTimeout)

		discovery := poller.NewClusterDiscovery(client, hub, func(cluster string) bool {
			_, ok := manager.Get(cell.Key(cluster))
			return ok
		}, nil)
		eppData := poller.NewEppDataWatcher(client, cfg.InstanceID, manager, logger)

		discoverySrc = discovery
		eppDataSrc = eppData
		discoveryHandle = discovery.Handle
		eppDataHandle = eppData.Handle
	}

	discoveryLoop := poller.New("discovery", discoverySrc, discoveryHandle, poller.Options{Interval: cfg.PollInterval, Timeout: cfg.PollTimeout, Logger: logger})
	eppDataLoop := poller.New("epp_data", eppDataSrc, eppDataHandle, poller.Options{Interval: cfg.PollInterval, Timeout: cfg.PollTimeout, Logger: logger})

	health := &healthServer{manager: manager}

	serverOpts, err := serverOptions(cfg)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(serverOpts...)
	extProcPb.RegisterExternalProcessorServer(grpcServer, demux.NewServer(demux.ManagerRouter{Manager: manager}, cfg.DefaultPool, logger))
	// Health is registered on the ext-proc server too, so gateways (BFE) that
	// probe the data address with grpc.health.v1 get a meaningful answer; the
	// dedicated health port below stays for K8s-style probes.
	healthpb.RegisterHealthServer(grpcServer, health)

	healthServerGRPC := grpc.NewServer(serverOpts...)
	healthpb.RegisterHealthServer(healthServerGRPC, health)

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(ctrlmetrics.Registry, promhttp.HandlerOpts{}))
	if cfg.EnablePprof {
		metricsMux.HandleFunc("/debug/pprof/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/debug/pprof/", http.StatusFound)
		})
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return serveGRPC(gctx, grpcServer, cfg.BindAddress, cfg.GRPCPort) })
	g.Go(func() error { return serveGRPC(gctx, healthServerGRPC, cfg.BindAddress, cfg.HealthPort) })
	g.Go(func() error {
		srv := &http.Server{Addr: net.JoinHostPort(cfg.BindAddress, fmt.Sprint(cfg.MetricsPort)), Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}
		go func() { <-gctx.Done(); srv.Shutdown(context.Background()) }()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error { return discoveryLoop.Start(gctx) })
	g.Go(func() error { return eppDataLoop.Start(gctx) })
	g.Go(func() error {
		// Readiness gate: epp_data synced, then every assigned cell ready.
		select {
		case <-eppDataLoop.FirstSync():
		case <-gctx.Done():
			return nil
		}
		for {
			allReady := true
			for _, c := range manager.List() {
				select {
				case <-c.Ready():
				default:
					allReady = false
				}
			}
			if allReady && len(manager.List()) > 0 {
				health.setReady(true)
				return nil
			}
			select {
			case <-time.After(200 * time.Millisecond):
			case <-gctx.Done():
				return nil
			}
		}
	})

	logger.Info("epp started",
		"version", version,
		"instance", cfg.InstanceID,
		"grpcPort", cfg.GRPCPort,
		"healthPort", cfg.HealthPort,
		"metricsPort", cfg.MetricsPort,
		"apiAddr", cfg.APIAddr,
		"grpcTLS", cfg.GRPCTLSCertFile != "")

	err = g.Wait()
	grpcServer.GracefulStop()
	healthServerGRPC.GracefulStop()
	return err
}

// serverOptions builds gRPC server options: server-side TLS for both the
// ext-proc and health servers when a certificate is configured, plaintext
// otherwise. Cert and key must be set together.
func serverOptions(cfg Config) ([]grpc.ServerOption, error) {
	if cfg.GRPCTLSCertFile == "" && cfg.GRPCTLSKeyFile == "" {
		return nil, nil
	}
	if cfg.GRPCTLSCertFile == "" || cfg.GRPCTLSKeyFile == "" {
		return nil, errors.New("grpc-tls-cert and grpc-tls-key must be set together")
	}
	creds, err := credentials.NewServerTLSFromFile(cfg.GRPCTLSCertFile, cfg.GRPCTLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("load gRPC TLS credentials: %w", err)
	}
	return []grpc.ServerOption{grpc.Creds(creds)}, nil
}

func serveGRPC(ctx context.Context, srv *grpc.Server, bind string, port int) error {
	lis, err := net.Listen("tcp", net.JoinHostPort(bind, fmt.Sprint(port)))
	if err != nil {
		return fmt.Errorf("listen %s:%d: %w", bind, port, err)
	}
	go func() {
		<-ctx.Done()
		srv.Stop()
	}()
	if err := srv.Serve(lis); err != nil {
		return fmt.Errorf("serve :%d: %w", port, err)
	}
	return nil
}
