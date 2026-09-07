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

package cell

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/metrics"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/fairness/globalstrict"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/ordering/fcfs"
	utilization "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/usagelimits"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/anthropic"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/passthrough"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/vllmhttp"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/single"

	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// Compile pulls default plugins from the global registry (populated at
// process startup in cmd/epp); register the minimal set an empty config
// needs so the test binary can compile one.
var registerTestPluginsOnce sync.Once

func registerTestPlugins() {
	registerTestPluginsOnce.Do(func() {
		fwkplugin.Register(single.SingleProfileHandlerType, fwkplugin.StabilityBeta, single.SingleProfileHandlerFactory)
		fwkplugin.Register(maxscore.MaxScorePickerType, fwkplugin.StabilityBeta, maxscore.MaxScorePickerFactory)
		fwkplugin.Register(fcfs.FCFSOrderingPolicyType, fwkplugin.StabilityBeta, fcfs.FCFSOrderingPolicyFactory)
		fwkplugin.Register(globalstrict.GlobalStrictFairnessPolicyType, fwkplugin.StabilityBeta, globalstrict.GlobalStrictFairnessPolicyFactory)
		fwkplugin.Register(usagelimits.StaticUsageLimitPolicyType, fwkplugin.StabilityBeta, usagelimits.StaticPolicyFactory)
		fwkplugin.Register(openai.OpenAIParserType, fwkplugin.StabilityBeta, openai.OpenAIParserPluginFactory)
		fwkplugin.Register(anthropic.AnthropicParserType, fwkplugin.StabilityBeta, anthropic.AnthropicParserPluginFactory)
		fwkplugin.Register(vllmhttp.VllmHTTPParserType, fwkplugin.StabilityBeta, vllmhttp.VllmHTTPParserPluginFactory)
		fwkplugin.Register(passthrough.PassthroughParserType, fwkplugin.StabilityBeta, passthrough.PassthroughParserPluginFactory)
		fwkplugin.Register(utilization.UtilizationDetectorType, fwkplugin.StabilityBeta, utilization.UtilizationDetectorFactory)
		fwkplugin.Register(sourcemetrics.MetricsDataSourceType, fwkplugin.StabilityBeta, sourcemetrics.MetricsDataSourceFactory)
		fwkplugin.Register(metrics.MetricsExtractorType, fwkplugin.StabilityBeta, metrics.CoreMetricsExtractorFactory)
	})
}

// fakeDatastore implements datastore.Datastore by embedding the interface
// (nil) and overriding only PodList; calls to any other method would panic,
// which keeps the fake honest about what the code under test may touch.
type fakeDatastore struct {
	datastore.Datastore
	pods []fwkdl.Endpoint
}

func (f *fakeDatastore) PodList(func(fwkdl.Endpoint) bool) []fwkdl.Endpoint {
	return f.pods
}

func testDependencies() Dependencies {
	return Dependencies{
		Logger:                 logr.Discard(),
		PoolNamespace:          "test-pool",
		RefreshMetricsInterval: 0,
	}
}

func TestCompileInvalidJSON(t *testing.T) {
	c := newTestCell(t, "cell-compile-bad")

	_, err := Compile(context.Background(), "cell-compile-bad", json.RawMessage("{not json"), c, testDependencies())
	if err == nil {
		t.Fatal("expected error for invalid config JSON")
	}
	if !strings.Contains(err.Error(), "decode config") {
		t.Errorf("error = %v, want it to wrap the decode failure", err)
	}
}

func TestCompileEmptyConfig(t *testing.T) {
	registerTestPlugins()
	c := newTestCell(t, "cell-compile-ok")
	raw := json.RawMessage("{}")

	eng, err := Compile(context.Background(), "cell-compile-ok", raw, c, testDependencies())
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	if eng == nil {
		t.Fatal("Compile() returned nil engine")
	}
	if eng.Version != HashConfig(raw) {
		t.Errorf("engine version = %q, want config hash %q", eng.Version, HashConfig(raw))
	}
	if eng.FC() != nil {
		t.Error("FC() should be nil when the flowControl feature gate is off")
	}
	if len(eng.discovery) != 0 {
		t.Errorf("discovery = %d plugins, want 0 for an empty config", len(eng.discovery))
	}
	if c.Engine() != nil {
		t.Error("Compile must not install the engine into the cell; the manager swaps it")
	}

	// The engine context is a child of the cell context; cancelling the engine
	// must not tear down the cell's data plane.
	eng.cancel()
	select {
	case <-c.ctx.Done():
		t.Error("cancelling the engine context cancelled the cell context")
	default:
	}

	// A second compile reuses the configured data plane (dataConfigured latch).
	if _, err := Compile(context.Background(), "cell-compile-ok", raw, c, testDependencies()); err != nil {
		t.Fatalf("second Compile() error = %v", err)
	}
}
