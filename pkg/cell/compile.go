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

/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cell

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/llm-d/llm-d-router/pkg/epp/config/loader"
	"github.com/llm-d/llm-d-router/pkg/epp/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/contracts"
	fccontroller "github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/controller"
	fcregistry "github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/registry"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	"github.com/llm-d/llm-d-router/pkg/epp/requestcontrol"
	"github.com/llm-d/llm-d-router/pkg/epp/scheduling"
)

// Dependencies carries the process-level knobs Compile needs.
type Dependencies struct {
	Logger                   logr.Logger
	MetricsRecorder          fwkplugin.MetricsRecorder
	PoolNamespace            string        // label for the endpoint pool (non-K8s deployments)
	RefreshMetricsInterval   time.Duration // datalayer runtime polling interval
	MaxPoolBufferSize        int           // StreamingServer body buffer
	AllowExperimentalPlugins bool
}

// Compile turns a raw EndpointPickerConfig for one cluster into an Engine.
// The Cell's datastore and runtime (data plane) are reused; only the policy
// plane is built. On error the caller keeps the previous engine.
func Compile(cellCtx context.Context, key Key, raw json.RawMessage, c *Cell, deps Dependencies) (*Engine, error) {
	logger := deps.Logger.WithValues("cell", string(key))

	rawConfig, featureGates, err := loader.LoadRawConfig(raw, logger)
	if err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	// Engine-scoped context: canceling it evicts queued FC requests and stops
	// this generation's discovery plugins (the drain mechanism).
	engCtx, cancel := context.WithCancel(cellCtx)

	fail := func(err error) (*Engine, error) {
		cancel()
		return nil, err
	}

	handle := fwkplugin.NewEppHandle(engCtx, makePodListFunc(c.ds), fwkplugin.WithMetricsRecorder(deps.MetricsRecorder))
	cfg, err := loader.InstantiateAndConfigure(rawConfig, handle, logger)
	if err != nil {
		return fail(fmt.Errorf("instantiate plugins: %w", err))
	}

	rcConfig := requestcontrol.NewConfig()

	// Data-producer auto-creation and DAG ordering, mirroring the upstream
	// composition root (cmd/epp/runner/runner.go phase two).
	if err := datalayer.CreateMissingDataProducers(engCtx, fwkplugin.DefaultProducerRegistry, fwkplugin.Registry, handle); err != nil {
		return fail(fmt.Errorf("create data producers: %w", err))
	}
	rcConfig.AddPlugins(handle.GetAllPlugins()...)
	for _, p := range handle.GetAllPlugins() {
		if registrant, ok := p.(fwkdl.Registrant); ok {
			if err := registrant.RegisterDependencies(c.rt); err != nil {
				return fail(fmt.Errorf("plugin %s register dependencies: %w", p.TypedName(), err))
			}
		}
	}
	dag, err := datalayer.ValidateAndOrderDataDependencies(handle.GetAllPlugins())
	if err != nil {
		return fail(fmt.Errorf("order data dependencies: %w", err))
	}
	rcConfig.OrderPlugins(dag)
	datalayer.RegisterScopeSpecs(handle.GetAllPlugins())

	if err := fwkplugin.ValidatePluginStability(handle, deps.AllowExperimentalPlugins); err != nil {
		return fail(fmt.Errorf("plugin stability: %w", err))
	}

	// The datalayer runtime is configured once per cell: the upstream runtime
	// rejects re-registration, and the data plane (sources/extractors/collectors)
	// is resident by design. Later config generations reuse it; data-layer
	// changes take effect on cell recreation.
	if !c.dataConfigured.Load() {
		if err := c.rt.Configure(cfg.DataConfig, logger); err != nil {
			return fail(fmt.Errorf("configure datalayer runtime: %w", err))
		}
		c.dataConfigured.Store(true)
	}

	scheduler := scheduling.NewSchedulerWithConfig(cfg.SchedulerConfig)

	candidates := contracts.EndpointCandidates(requestcontrol.NewDatastoreEndpointCandidates(c.ds))

	var fc *fccontroller.FlowController
	var admission requestcontrol.AdmissionController
	if featureGates[flowcontrol.FeatureGate] && cfg.FlowControlConfig != nil {
		candidates = requestcontrol.NewCachedEndpointCandidates(engCtx, candidates, 50*time.Millisecond)
		registry := fcregistry.NewFlowRegistry(cfg.FlowControlConfig.Registry, logger)
		fc = fccontroller.NewFlowController(engCtx, string(key), cfg.FlowControlConfig.Controller, fccontroller.Deps{
			Registry:           registry,
			SaturationDetector: cfg.SaturationDetector,
			EndpointCandidates: candidates,
			UsageLimitPolicy:   cfg.FlowControlConfig.UsageLimitPolicy,
		})
		admission = requestcontrol.NewFlowControlAdmissionController(fc, string(key), candidates)
	} else {
		admission = requestcontrol.NewLegacyAdmissionController(cfg.SaturationDetector, candidates)
	}

	director := requestcontrol.NewDirectorWithConfig(c.ds, scheduler, admission, candidates, rcConfig)

	eng := &Engine{
		Version:        HashConfig(raw),
		director:       director,
		fc:             fc,
		handle:         handle,
		parserRegistry: cfg.ParserRegistry,
		ctx:            engCtx,
		cancel:         cancel,
	}
	eng.streamer = handlers.NewStreamingServer(c.ds, director, cfg.ParserRegistry, deps.MaxPoolBufferSize)

	if rawConfig.DataLayer != nil && rawConfig.DataLayer.Discovery != nil && rawConfig.DataLayer.Discovery.Endpoints != nil {
		ref := rawConfig.DataLayer.Discovery.Endpoints.PluginRef
		p := handle.Plugin(ref)
		if p == nil {
			return fail(fmt.Errorf("discovery: no plugin found with name %q", ref))
		}
		disc, ok := p.(fwkdl.EndpointDiscovery)
		if !ok {
			return fail(fmt.Errorf("discovery: plugin %q does not implement EndpointDiscovery", ref))
		}
		eng.discovery = append(eng.discovery, disc)
	}

	return eng, nil
}

// makePodListFunc is adapted from llm-d-router (Apache-2.0):
// cmd/epp/runner/runner.go — Copyright 2025 The Kubernetes Authors.
func makePodListFunc(ds datastore.Datastore) func() []k8stypes.NamespacedName {
	return func() []k8stypes.NamespacedName {
		pods := ds.PodList(datastore.AllPodsPredicate)
		names := make([]k8stypes.NamespacedName, 0, len(pods))
		for _, p := range pods {
			names = append(names, p.GetMetadata().ID)
		}
		return names
	}
}
