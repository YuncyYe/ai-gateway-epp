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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-router/pkg/epp/config/loader"
	"github.com/llm-d/llm-d-router/pkg/epp/datastore"
	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	extractormetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/extractor/metrics"
	sourcemetrics "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/datalayer/source/metrics"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/fairness/globalstrict"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/ordering/fcfs"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/saturationdetector/utilization"
	usagelimits "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/flowcontrol/usagelimits"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/requesthandling/parsers/openai"
	utilizationfilter "github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/filter/utilization"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/picker/maxscore"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/profilehandler/single"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/plugins/scheduling/scorer/kvcacheutilization"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/eppplugin/clustertable"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/poller"
)

// registerTestPlugins registers the in-tree plugins the test config uses.
// Registration is global and idempotent.
func registerTestPlugins(hub *clustertable.Hub) {
	loader.RegisterFeatureGate(flowcontrol.FeatureGate, false)

	fwkplugin.Register(utilizationfilter.UtilizationFilterType, fwkplugin.StabilityBeta, utilizationfilter.Factory)
	fwkplugin.Register(kvcacheutilization.KvCacheUtilizationScorerType, fwkplugin.StabilityBeta, kvcacheutilization.KvCacheUtilizationScorerFactory)
	fwkplugin.Register(maxscore.MaxScorePickerType, fwkplugin.StabilityBeta, maxscore.MaxScorePickerFactory)
	fwkplugin.Register(single.SingleProfileHandlerType, fwkplugin.StabilityBeta, single.SingleProfileHandlerFactory)
	fwkplugin.Register(globalstrict.GlobalStrictFairnessPolicyType, fwkplugin.StabilityBeta, globalstrict.GlobalStrictFairnessPolicyFactory)
	fwkplugin.Register(fcfs.FCFSOrderingPolicyType, fwkplugin.StabilityBeta, fcfs.FCFSOrderingPolicyFactory)
	fwkplugin.Register(usagelimits.StaticUsageLimitPolicyType, fwkplugin.StabilityBeta, usagelimits.StaticPolicyFactory)
	fwkplugin.Register(utilization.UtilizationDetectorType, fwkplugin.StabilityBeta, utilization.UtilizationDetectorFactory)
	fwkplugin.Register(openai.OpenAIParserType, fwkplugin.StabilityBeta, openai.OpenAIParserPluginFactory)
	fwkplugin.Register(sourcemetrics.MetricsDataSourceType, fwkplugin.StabilityBeta, sourcemetrics.MetricsDataSourceFactory)
	fwkplugin.Register(extractormetrics.MetricsExtractorType, fwkplugin.StabilityBeta, extractormetrics.CoreMetricsExtractorFactory)
	fwkplugin.Register(clustertable.PluginType, fwkplugin.StabilityBeta, clustertable.NewFactory(hub))
}

// fakeAPI serves assignment, picker_config and cluster_table with version
// increments, mirroring the InnerAPI envelope contract.
type fakeAPI struct {
	srv *httptest.Server

	assignmentVersion atomic.Int64
	assignment        atomic.Value // assignment.View

	configVersion atomic.Int64
	config        atomic.Value // map[string]json.RawMessage

	tableVersion atomic.Int64
	table        atomic.Value // poller-facing JSON document
}

func newFakeAPI(t *testing.T) *fakeAPI {
	f := &fakeAPI{}
	mux := http.NewServeMux()
	mux.HandleFunc("/configs/epp_data/assignment", func(w http.ResponseWriter, r *http.Request) {
		writeVersioned(w, f.assignmentVersion.Load(), f.assignment.Load())
	})
	mux.HandleFunc("/configs/epp_data/assignment/report", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/configs/epp_data/picker_config", func(w http.ResponseWriter, r *http.Request) {
		writeVersioned(w, f.configVersion.Load(), f.config.Load())
	})
	mux.HandleFunc("/configs/gslb_data/cluster_table", func(w http.ResponseWriter, r *http.Request) {
		writeVersioned(w, f.tableVersion.Load(), f.table.Load())
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func writeVersioned(w http.ResponseWriter, version int64, config any) {
	resp := map[string]any{"ErrNum": 200, "ErrMsg": "success", "WorkMode": "ModeNormal"}
	if config != nil {
		resp["Data"] = map[string]any{"Version": fmt.Sprintf("v%d", version), "Config": config}
	} else {
		resp["Data"] = nil
	}
	raw, _ := json.Marshal(resp)
	w.Write(raw)
}

func (f *fakeAPI) setAssignment(v map[string]string) {
	f.assignment.Store(v)
	f.assignmentVersion.Add(1)
}

func (f *fakeAPI) setConfig(v map[string]json.RawMessage) {
	f.config.Store(v)
	f.configVersion.Add(1)
}

func (f *fakeAPI) setTable(v map[string]any) {
	f.table.Store(v)
	f.tableVersion.Add(1)
}

const pickerConfigV1 = `{
  "featureGates": ["flowControl"],
  "plugins": [
    {"name": "ep-discover", "type": "cluster-table-discovery", "parameters": {"clusterName": "cluster-a"}},
    {"name": "util-filter", "type": "utilization-filter", "parameters": {"conditions": [{"metric": "kv-cache-utilization", "maxValue": 0.9}]}},
    {"name": "kv-scorer", "type": "kv-cache-utilization-scorer", "parameters": {}},
    {"name": "max-score", "type": "max-score-picker", "parameters": {}},
    {"name": "util-detector", "type": "utilization-detector", "parameters": {}}
  ],
  "schedulingProfiles": [
    {"name": "default", "plugins": [
      {"pluginRef": "util-filter"},
      {"pluginRef": "kv-scorer"},
      {"pluginRef": "max-score"}
    ]}
  ],
  "dataLayer": {
    "discovery": {"endpoints": {"pluginRef": "ep-discover"}}
  },
  "flowControl": {
    "defaultRequestTTL": "30s",
    "saturationDetector": {"pluginRef": "util-detector"},
    "priorityBands": [{"priority": 1, "maxRequests": "200"}]
  },
  "requestHandler": {"parsers": [{"pluginRef": "openai-parser"}]}
}`

// TestEndToEnd drives the full pipeline against a fake InnerAPI:
// assignment creates the cell, picker_config compiles the engine, the
// cluster table flows through the hub into the datastore.
func TestEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	hub := clustertable.NewHub()
	registerTestPlugins(hub)

	api := newFakeAPI(t)
	api.setAssignment(map[string]string{"cluster-a": "primary"})
	api.setConfig(map[string]json.RawMessage{"cluster-a": json.RawMessage(pickerConfigV1)})
	api.setTable(map[string]any{
		"cluster-a": map[string]any{
			"sub-1": []map[string]any{
				{"Name": "backend-1", "Addr": "10.0.0.1", "Port": 8080, "Weight": 50},
				{"Name": "backend-2", "Addr": "10.0.0.2", "Port": 8080, "Weight": 0},
			},
		},
	})

	client := innerapi.NewClient(api.srv.URL, "", 3*time.Second)
	manager := cell.NewManager(ctx, cell.Options{
		Logger:                 logr.Discard(),
		PoolNamespace:          "test",
		RefreshMetricsInterval: time.Second,
		DrainTimeout:           time.Second,
	})

	discovery := poller.NewClusterDiscovery(client, hub, func(cluster string) bool {
		_, ok := manager.Get(cell.Key(cluster))
		return ok
	}, nil)
	configPoller := poller.NewConfigPoller(client, manager)
	watcher := poller.NewAssignmentWatcher(client, "epp-test-1", manager)

	// Round 1: assignment -> config -> discovery, in dependency order.
	if _, _, view, err := watcher.Fetch(ctx, ""); err != nil {
		t.Fatalf("assignment fetch: %v", err)
	} else if err := watcher.Handle(ctx, view); err != nil {
		t.Fatalf("assignment handle: %v", err)
	}

	c, ok := manager.Get("cluster-a")
	if !ok {
		t.Fatal("cell not created by assignment")
	}
	if c.Role() != cell.RolePrimary {
		t.Fatalf("role=%v", c.Role())
	}

	if _, _, configs, err := configPoller.Fetch(ctx, ""); err != nil {
		t.Fatalf("config fetch: %v", err)
	} else if err := configPoller.Handle(ctx, configs); err != nil {
		t.Fatalf("config handle: %v", err)
	}
	if c.Engine() == nil {
		t.Fatal("engine not compiled")
	}
	ver1 := c.Engine().Version

	if _, _, table, err := discovery.Fetch(ctx, ""); err != nil {
		t.Fatalf("discovery fetch: %v", err)
	} else if err := discovery.Handle(ctx, table); err != nil {
		t.Fatalf("discovery handle: %v", err)
	}

	// Cell becomes ready once discovery has applied its first snapshot.
	select {
	case <-c.Ready():
	case <-ctx.Done():
		t.Fatal("cell never became ready")
	}

	// Weight=50 backend present; Weight=0 backend never applied. The hub push
	// races with the discovery plugin's first (possibly empty) snapshot, so
	// poll until the backend lands.
	deadline := time.Now().Add(5 * time.Second)
	var pods []fwkdl.Endpoint
	for {
		pods = c.Datastore().PodList(datastore.AllPodsPredicate)
		if len(pods) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pods=%d, want 1", len(pods))
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := pods[0].GetMetadata().Name; got != "backend-1" {
		t.Fatalf("endpoint=%q", got)
	}

	// Round 2: config change hot-swaps the engine; version moves.
	configV2 := strings.Replace(pickerConfigV1, `"defaultRequestTTL": "30s"`, `"defaultRequestTTL": "60s"`, 1)
	api.setConfig(map[string]json.RawMessage{"cluster-a": json.RawMessage(configV2)})
	if _, _, configs, err := configPoller.Fetch(ctx, "v1"); err != nil {
		t.Fatalf("config fetch 2: %v", err)
	} else {
		raw, ok := configs["cluster-a"]
		if !ok {
			t.Fatal("cluster-a missing from picker_config")
		}
		swapped, err := manager.ApplyConfig(ctx, "cluster-a", raw)
		if err != nil {
			t.Fatalf("apply config 2: %v", err)
		}
		if !swapped {
			t.Fatal("config 2 not applied")
		}
	}
	if c.Engine().Version == ver1 {
		t.Fatal("engine version did not change after config update")
	}

	// Round 3: backend drained in the table -> endpoint deleted.
	api.setTable(map[string]any{
		"cluster-a": map[string]any{
			"sub-1": []map[string]any{
				{"Name": "backend-1", "Addr": "10.0.0.1", "Port": 8080, "Weight": 0},
			},
		},
	})
	if _, _, table, err := discovery.Fetch(ctx, "v1"); err != nil {
		t.Fatalf("discovery fetch 2: %v", err)
	} else if err := discovery.Handle(ctx, table); err != nil {
		t.Fatalf("discovery handle 2: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if n := len(c.Datastore().PodList(datastore.AllPodsPredicate)); n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("endpoint not drained")
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Round 4: assignment revoked -> cell dropped.
	api.setAssignment(map[string]string{})
	if _, _, view, err := watcher.Fetch(ctx, "v1"); err != nil {
		t.Fatalf("assignment fetch 2: %v", err)
	} else if err := watcher.Handle(ctx, view); err != nil {
		t.Fatalf("assignment handle 2: %v", err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		if _, ok := manager.Get("cluster-a"); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("cell not dropped")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
