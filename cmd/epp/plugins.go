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

package main

import (
	"github.com/llm-d/llm-d-router/pkg/epp/config/loader"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol"
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

// registerPlugins registers the flow-control feature gate and the in-tree
// llm-d plugins this deployment uses. It runs once at process startup; the
// global registries are then read by every engine compile.
func registerPlugins(hub *clustertable.Hub) {
	loader.RegisterFeatureGate(flowcontrol.FeatureGate, false)

	// scheduling
	fwkplugin.Register(utilizationfilter.UtilizationFilterType, fwkplugin.StabilityBeta, utilizationfilter.Factory)
	fwkplugin.Register(prefix.PrefixCacheScorerPluginType, fwkplugin.StabilityBeta, prefix.PrefixCachePluginFactory)
	fwkplugin.Register(kvcacheutilization.KvCacheUtilizationScorerType, fwkplugin.StabilityBeta, kvcacheutilization.KvCacheUtilizationScorerFactory)
	fwkplugin.Register(queuedepth.QueueScorerType, fwkplugin.StabilityBeta, queuedepth.QueueScorerFactory)
	fwkplugin.Register(runningrequests.RunningRequestsSizeScorerType, fwkplugin.StabilityBeta, runningrequests.RunningRequestsSizeScorerFactory)
	fwkplugin.Register(loadaware.LoadAwareType, fwkplugin.StabilityBeta, loadaware.Factory)
	fwkplugin.Register(sessionaffinityscorer.SessionAffinityType, fwkplugin.StabilityBeta, sessionaffinityscorer.Factory)
	fwkplugin.Register(maxscore.MaxScorePickerType, fwkplugin.StabilityBeta, maxscore.MaxScorePickerFactory)
	fwkplugin.Register(random.RandomPickerType, fwkplugin.StabilityBeta, random.RandomPickerFactory)
	fwkplugin.Register(weightedrandom.WeightedRandomPickerType, fwkplugin.StabilityBeta, weightedrandom.WeightedRandomPickerFactory)
	fwkplugin.Register(single.SingleProfileHandlerType, fwkplugin.StabilityBeta, single.SingleProfileHandlerFactory)

	// flow control
	fwkplugin.Register(globalstrict.GlobalStrictFairnessPolicyType, fwkplugin.StabilityBeta, globalstrict.GlobalStrictFairnessPolicyFactory)
	fwkplugin.Register(roundrobin.RoundRobinFairnessPolicyType, fwkplugin.StabilityBeta, roundrobin.RoundRobinFairnessPolicyFactory)
	fwkplugin.Register(fcfs.FCFSOrderingPolicyType, fwkplugin.StabilityBeta, fcfs.FCFSOrderingPolicyFactory)
	fwkplugin.Register(edf.EDFOrderingPolicyType, fwkplugin.StabilityBeta, edf.EDFOrderingPolicyFactory)
	fwkplugin.Register(slodeadline.SLODeadlineOrderingPolicyType, fwkplugin.StabilityBeta, slodeadline.SLODeadlineOrderingPolicyFactory)
	fwkplugin.Register(usagelimits.StaticUsageLimitPolicyType, fwkplugin.StabilityBeta, usagelimits.StaticPolicyFactory)
	fwkplugin.Register(concurrency.ConcurrencyDetectorType, fwkplugin.StabilityBeta, concurrency.ConcurrencyDetectorFactory)
	fwkplugin.Register(utilizationdetector.UtilizationDetectorType, fwkplugin.StabilityBeta, utilizationdetector.UtilizationDetectorFactory)

	// request-level default data producers
	fwkplugin.RegisterAsDefaultProducer(approximateprefix.ApproxPrefixCachePluginType, fwkplugin.StabilityBeta, approximateprefix.ApproxPrefixCacheFactory, attrprefix.PrefixCacheMatchInfoDataKey)
	fwkplugin.RegisterAsDefaultProducer(inflightload.InFlightLoadProducerType, fwkplugin.StabilityBeta, inflightload.InFlightLoadProducerFactory, attrconcurrency.InFlightLoadDataKey)
	fwkplugin.RegisterAsDefaultProducer(tokenizer.PluginType, fwkplugin.StabilityBeta, tokenizer.PluginFactory, tokenizer.TokenizedPromptDataKey)

	// parsers
	fwkplugin.Register(openai.OpenAIParserType, fwkplugin.StabilityBeta, openai.OpenAIParserPluginFactory)
	fwkplugin.Register(passthrough.PassthroughParserType, fwkplugin.StabilityBeta, passthrough.PassthroughParserPluginFactory)
	fwkplugin.Register(vllmhttp.VllmHTTPParserType, fwkplugin.StabilityBeta, vllmhttp.VllmHTTPParserPluginFactory)

	// metrics collection
	fwkplugin.Register(sourcemetrics.MetricsDataSourceType, fwkplugin.StabilityBeta, sourcemetrics.MetricsDataSourceFactory)
	fwkplugin.Register(extractormetrics.MetricsExtractorType, fwkplugin.StabilityBeta, extractormetrics.CoreMetricsExtractorFactory)

	// endpoint discovery from the ai-gateway-api cluster table
	fwkplugin.Register(clustertable.PluginType, fwkplugin.StabilityBeta, clustertable.NewFactory(hub))
}
