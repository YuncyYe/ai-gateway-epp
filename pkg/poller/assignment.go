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
	"net/url"
	"time"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/assignment"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

// AssignmentPath is the InnerAPI endpoint for this instance's role view.
const AssignmentPath = "/configs/epp_data/assignment"

// ReportPath is where the instance posts its per-cell readiness each round.
const ReportPath = "/configs/epp_data/assignment/report"

// AssignmentWatcher polls this instance's cluster roles and drives the cell
// lifecycle (create/promote/demote/drop), then reports readiness so the
// allocator can sequence failovers.
type AssignmentWatcher struct {
	client   *innerapi.Client
	instance string
	manager  *cell.Manager
	now      func() time.Time
}

// NewAssignmentWatcher creates the watcher.
func NewAssignmentWatcher(client *innerapi.Client, instance string, manager *cell.Manager) *AssignmentWatcher {
	return &AssignmentWatcher{client: client, instance: instance, manager: manager, now: time.Now}
}

// Fetch implements Source.
func (w *AssignmentWatcher) Fetch(ctx context.Context, version string) (bool, string, assignment.View, error) {
	path := AssignmentPath + "?instance=" + url.QueryEscape(w.instance)
	var view assignment.View
	changed, ver, err := w.client.Get(ctx, path, version, &view)
	if err != nil || !changed {
		return changed, ver, nil, err
	}
	if view == nil {
		view = assignment.View{}
	}
	return true, ver, view, nil
}

// Handle implements Handle.
func (w *AssignmentWatcher) Handle(ctx context.Context, view assignment.View) error {
	// Ensure desired cells; new cells start as standby until promoted.
	for cluster, role := range view {
		var cr cell.Role
		switch role {
		case assignment.RolePrimary:
			cr = cell.RolePrimary
		default:
			cr = cell.RoleStandby
		}
		if _, err := w.manager.Ensure(ctx, cell.Key(cluster), cr); err != nil {
			return fmt.Errorf("ensure cell %s: %w", cluster, err)
		}
	}

	// Drop cells that lost their assignment.
	desired := make(map[string]bool, len(view))
	for cluster := range view {
		desired[cluster] = true
	}
	for _, c := range w.manager.List() {
		if !desired[string(c.Key)] {
			w.manager.Drop(c.Key)
		}
	}

	// Report readiness for the allocator's sync-before-flip ordering.
	report := w.buildReport()
	if err := w.client.Post(ctx, ReportPath, report); err != nil {
		// The report is best-effort; roles are still applied locally.
		return fmt.Errorf("post readiness report: %w", err)
	}
	return nil
}

func (w *AssignmentWatcher) buildReport() assignment.ReadyReport {
	cells := w.manager.List()
	report := assignment.ReadyReport{
		Instance: w.instance,
		Cells:    make([]assignment.CellStatus, 0, len(cells)),
	}
	for _, c := range cells {
		status := assignment.CellStatus{
			Key:   string(c.Key),
			Role:  c.Role().String(),
			State: c.State().String(),
		}
		if t := c.ReadySince(); !t.IsZero() {
			status.ReadySince = t.Format(time.RFC3339)
		}
		if eng := c.Engine(); eng != nil {
			status.EngineVersion = eng.Version
		}
		report.Cells = append(report.Cells, status)
	}
	return report
}
