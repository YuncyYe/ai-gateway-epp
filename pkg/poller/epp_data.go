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
// role to this instance at all (e.g. -instance-id not in /epp-pool): the EPP
// holds no cells and serves nothing.
var assignmentNoMatch = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "ai_epp",
	Name:      "assignment_no_match",
	Help:      "1 while the last applied epp_data snapshot assigns no cluster role to this instance.",
})

func init() {
	ctrlmetrics.Registry.MustRegister(assignmentNoMatch)
}

// EppDataWatcher polls the merged epp_data/config snapshot (compiled
// per-cluster configs plus the full assignment view, one version) and drives
// the cell lifecycle: it resolves this instance's roles locally, ensures /
// promotes / demotes / drops cells, and hot-swaps engines from the epp_config
// section. There is no readiness report: failover is driven by the gateway
// side, the standby cell keeps its data plane hot.
type EppDataWatcher struct {
	client   *innerapi.Client
	instance string
	manager  *cell.Manager
	logger   logr.Logger
}

// NewEppDataWatcher creates the watcher. instance is this EPP's instance id
// (-instance-id), matched against the full assignment view.
func NewEppDataWatcher(client *innerapi.Client, instance string, manager *cell.Manager, logger logr.Logger) *EppDataWatcher {
	if logger.GetSink() == nil {
		logger = logr.Discard()
	}
	return &EppDataWatcher{client: client, instance: instance, manager: manager, logger: logger}
}

// Fetch implements Source; both sections come from the same versioned
// snapshot, so config and role changes can never skew across requests.
func (w *EppDataWatcher) Fetch(ctx context.Context, version string) (bool, string, innerapi.EppDataConfig, error) {
	var cfg innerapi.EppDataConfig
	changed, ver, err := w.client.Get(ctx, innerapi.EppDataConfigPath, version, &cfg)
	if err != nil || !changed {
		return changed, ver, innerapi.EppDataConfig{}, err
	}
	return true, ver, cfg, nil
}

// Handle implements Handle: resolve this instance's roles from the full
// assignment view, diff them against the live cells, then apply the
// epp_config section for the clusters this instance holds.
func (w *EppDataWatcher) Handle(ctx context.Context, cfg innerapi.EppDataConfig) error {
	log := w.logger.WithValues("instance", w.instance)

	mine := make(map[string]cell.Role, len(cfg.Assignment))
	for cluster, entry := range cfg.Assignment {
		role, ok := resolveRole(w.instance, entry)
		if !ok {
			log.V(2).Info("cluster not assigned to this instance", "cluster", cluster)
			continue
		}
		mine[cluster] = role
		log.V(2).Info("role resolved", "cluster", cluster, "role", role.String())
		if peer := peerID(w.instance, entry); peer != "" {
			log.Info("assignment peer", "cell", cluster, "role", role.String(), "peer", peer)
		}
	}
	if len(mine) == 0 {
		assignmentNoMatch.Set(1)
		log.Info("WARNING: instance matches no cluster role in the epp_data assignment; no cells will be created (check -instance-id against /epp-pool)")
	} else {
		assignmentNoMatch.Set(0)
	}

	for cluster, role := range mine {
		if _, err := w.manager.Ensure(ctx, cell.Key(cluster), role); err != nil {
			return fmt.Errorf("ensure cell %s: %w", cluster, err)
		}
	}
	for _, c := range w.manager.List() {
		if _, ok := mine[string(c.Key)]; !ok {
			log.V(2).Info("dropping cell (no longer assigned)", "cell", string(c.Key))
			w.manager.Drop(c.Key)
		}
	}

	for cluster, raw := range cfg.EppConfig {
		if _, ok := mine[cluster]; !ok {
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

// resolveRole returns this instance's role for one cluster's assignment
// entry. Wildcards are not supported: only an exact instance-id match
// assigns a role.
func resolveRole(self string, e innerapi.AssignmentEntry) (role cell.Role, mine bool) {
	if e.Primary != nil && *e.Primary == self {
		return cell.RolePrimary, true
	}
	if e.Standby != nil && *e.Standby == self {
		return cell.RoleStandby, true
	}
	return cell.RoleNone, false
}

// peerID returns the other instance id in the group (the standby from a
// primary's perspective and vice versa); "" for a single-instance group.
// The peer is only logged — no connection is established.
func peerID(self string, e innerapi.AssignmentEntry) string {
	if e.Primary != nil && *e.Primary == self {
		if e.Standby != nil {
			return *e.Standby
		}
		return ""
	}
	if e.Standby != nil && *e.Standby == self && e.Primary != nil {
		return *e.Primary
	}
	return ""
}
