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

package poller

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

// assignmentNoMatch is 1 while the last applied epp_data snapshot assigns no
// cluster to this instance's group at all (e.g. -epp-group not in
// /epp-pool): the EPP holds no cells and serves nothing.
var assignmentNoMatch = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "ai_epp",
	Name:      "assignment_no_match",
	Help:      "1 while the last applied epp_data snapshot assigns no cluster to this instance's group.",
})

func init() {
	ctrlmetrics.Registry.MustRegister(assignmentNoMatch)
}

// EppDataWatcher polls the merged epp_data/config snapshot (compiled
// per-cluster configs plus the cluster-to-group assignment, one version) and
// drives the cell lifecycle: it resolves which clusters are assigned to this
// instance's group, ensures / drops cells, and hot-swaps engines from the
// epp_config section. Assignment resolution is binary (mine or not) — there
// is no role distinction. There is no readiness report: failover is driven
// by the gateway side.
type EppDataWatcher struct {
	client   *innerapi.Client
	eppgroup string
	manager  *cell.Manager
	logger   logr.Logger
}

// NewEppDataWatcher creates the watcher. eppgroup is this EPP's group name
// (-epp-group), matched against the assignment section.
func NewEppDataWatcher(client *innerapi.Client, eppgroup string, manager *cell.Manager, logger logr.Logger) *EppDataWatcher {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	return &EppDataWatcher{client: client, eppgroup: eppgroup, manager: manager, logger: logger}
}

// Fetch implements Source; both sections come from the same versioned
// snapshot, so config and assignment changes can never skew across requests.
func (w *EppDataWatcher) Fetch(ctx context.Context, version string) (bool, string, innerapi.EppDataConfig, error) {
	var cfg innerapi.EppDataConfig
	changed, ver, err := w.client.Get(ctx, innerapi.EppDataConfigPath, version, &cfg)
	if err != nil || !changed {
		return changed, ver, innerapi.EppDataConfig{}, err
	}
	return true, ver, cfg, nil
}

// Handle implements Handle: resolve which clusters are assigned to this
// instance's group from the assignment section, diff them against the live
// cells, then apply the epp_config section for the clusters this instance
// holds.
func (w *EppDataWatcher) Handle(ctx context.Context, cfg innerapi.EppDataConfig) error {
	log := w.logger.WithValues("eppgroup", w.eppgroup)

	mine := make(map[string]bool, len(cfg.Assignment))
	for cluster, entry := range cfg.Assignment {
		if !resolveAssignmentEntry(w.eppgroup, entry) {
			log.V(2).Info("cluster not assigned to this instance's group", "cluster", cluster)
			continue
		}
		mine[cluster] = true
		log.V(2).Info("cluster assigned to this instance's group", "cluster", cluster)
	}
	if len(mine) == 0 {
		assignmentNoMatch.Set(1)
		log.Info("WARNING: this instance's group matches no cluster in the epp_data assignment; no cells will be created (check -epp-group against /epp-pool)")
	} else {
		assignmentNoMatch.Set(0)
	}

	for cluster := range mine {
		if _, err := w.manager.Ensure(ctx, cell.Key(cluster)); err != nil {
			return fmt.Errorf("ensure cell %s: %w", cluster, err)
		}
	}
	for _, c := range w.manager.List() {
		if !mine[string(c.Key)] {
			log.V(2).Info("dropping cell (no longer assigned)", "cell", string(c.Key))
			w.manager.Drop(c.Key)
		}
	}

	for cluster, raw := range cfg.EppConfig {
		if !mine[cluster] {
			continue
		}
		log.V(2).Info("applying config to cell", "cell", cluster)
		if _, err := w.manager.ApplyConfig(ctx, cell.Key(cluster), raw); err != nil {
			log.V(2).Info("config apply failed (keeping old engine)", "cell", cluster, "error", err.Error())
			continue
		}
	}
	return nil
}

// resolveAssignmentEntry reports whether the cluster's assigned group is
// this instance's own group: the cluster is then served by this instance.
// Wildcards are not supported: only an exact group-name match assigns the
// cluster. An unset self group (-epp-group empty) matches nothing, which
// surfaces a missing configuration via ai_epp_assignment_no_match.
func resolveAssignmentEntry(self string, e innerapi.AssignmentEntry) (mine bool) {
	if self == "" {
		return false
	}
	return e.Group != "" && e.Group == self
}
