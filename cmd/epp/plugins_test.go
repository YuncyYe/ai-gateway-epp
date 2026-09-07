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
	"testing"

	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	attrconcurrency "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/concurrency"
	attrprefix "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/attribute/prefix"
	extractormetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/metrics"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/fairness/globalstrict"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/fairness/roundrobin"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/ordering/edf"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/ordering/fcfs"
	slodeadline "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/ordering/slodeadline"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/saturationdetector/concurrency"
	utilizationdetector "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/usagelimits"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/approximateprefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/inflightload"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requestcontrol/dataproducer/tokenizer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/passthrough"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/vllmhttp"
	utilizationfilter "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/utilization"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/random"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/weightedrandom"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/single"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/kvcacheutilization"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/loadaware"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/prefix"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/queuedepth"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/runningrequests"
	sessionaffinityscorer "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/sessionaffinity"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/eppplugin/clustertable"
)

// registeredTypes is the full set of plugin types plugins.go must wire into
// the global registry, grouped by role for readable failure output.
var registeredTypes = map[string][]string{
	"scheduling": {
		utilizationfilter.UtilizationFilterType,
		prefix.PrefixCacheScorerPluginType,
		kvcacheutilization.KvCacheUtilizationScorerType,
		queuedepth.QueueScorerType,
		runningrequests.RunningRequestsSizeScorerType,
		loadaware.LoadAwareType,
		sessionaffinityscorer.SessionAffinityType,
		maxscore.MaxScorePickerType,
		random.RandomPickerType,
		weightedrandom.WeightedRandomPickerType,
		single.SingleProfileHandlerType,
	},
	"flowcontrol": {
		globalstrict.GlobalStrictFairnessPolicyType,
		roundrobin.RoundRobinFairnessPolicyType,
		fcfs.FCFSOrderingPolicyType,
		edf.EDFOrderingPolicyType,
		slodeadline.SLODeadlineOrderingPolicyType,
		usagelimits.StaticUsageLimitPolicyType,
		concurrency.ConcurrencyDetectorType,
		utilizationdetector.UtilizationDetectorType,
	},
	"request-level data producers": {
		approximateprefix.ApproxPrefixCachePluginType,
		inflightload.InFlightLoadProducerType,
		tokenizer.PluginType,
	},
	"parsers": {
		openai.OpenAIParserType,
		passthrough.PassthroughParserType,
		vllmhttp.VllmHTTPParserType,
	},
	"metrics collection": {
		sourcemetrics.MetricsDataSourceType,
		extractormetrics.MetricsExtractorType,
	},
	"endpoint discovery": {
		clustertable.PluginType,
	},
}

func TestRegisterPlugins(t *testing.T) {
	registerPlugins(clustertable.NewHub())

	for group, types := range registeredTypes {
		for _, typ := range types {
			if _, ok := fwkplugin.Registry[typ]; !ok {
				t.Errorf("%s: plugin type %q not registered", group, typ)
				continue
			}
			if got := fwkplugin.GetPluginStability(typ); got != fwkplugin.StabilityBeta {
				t.Errorf("plugin type %q stability = %v, want %v", typ, got, fwkplugin.StabilityBeta)
			}
		}
	}
}

func TestRegisterPluginsDefaultProducers(t *testing.T) {
	registerPlugins(clustertable.NewHub())

	want := map[string]string{
		attrprefix.PrefixCacheMatchInfoDataKey.String(): approximateprefix.ApproxPrefixCachePluginType,
		attrconcurrency.InFlightLoadDataKey.String():    inflightload.InFlightLoadProducerType,
		tokenizer.TokenizedPromptDataKey.String():       tokenizer.PluginType,
	}
	for key, typ := range want {
		if got := fwkplugin.DefaultProducerRegistry[key]; got != typ {
			t.Errorf("DefaultProducerRegistry[%q] = %q, want %q", key, got, typ)
		}
	}
}

// TestRegisterPluginsIdempotent guards the startup contract: registration
// overwrites registry entries rather than panicking, so calling it again
// (e.g. after a test reset) leaves the registry consistent.
func TestRegisterPluginsIdempotent(t *testing.T) {
	registerPlugins(clustertable.NewHub())
	registerPlugins(clustertable.NewHub())

	for group, types := range registeredTypes {
		for _, typ := range types {
			if _, ok := fwkplugin.Registry[typ]; !ok {
				t.Errorf("%s: plugin type %q missing after second registration", group, typ)
			}
		}
	}
}
