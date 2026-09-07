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
	"encoding/json"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

// PickerConfigPath is the InnerAPI endpoint for per-cluster picker configs.
const PickerConfigPath = "/configs/epp_data/picker_config"

// ConfigPoller polls picker_config and hot-swaps engines for assigned cells.
type ConfigPoller struct {
	client  *innerapi.Client
	manager *cell.Manager
}

// NewConfigPoller creates the config poller.
func NewConfigPoller(client *innerapi.Client, manager *cell.Manager) *ConfigPoller {
	return &ConfigPoller{client: client, manager: manager}
}

// Fetch implements Source; the raw per-cluster config bytes are kept so the
// engine hash matches the delivered content exactly.
func (p *ConfigPoller) Fetch(ctx context.Context, version string) (bool, string, map[string]json.RawMessage, error) {
	var configs map[string]json.RawMessage
	changed, ver, err := p.client.Get(ctx, PickerConfigPath, version, &configs)
	if err != nil || !changed {
		return changed, ver, nil, err
	}
	return true, ver, configs, nil
}

// Handle implements Handle: compile and swap each assigned cluster's engine.
// Clusters without a local cell (not assigned, or draining) are skipped; the
// cell readiness contract requires both assignment and config.
func (p *ConfigPoller) Handle(ctx context.Context, configs map[string]json.RawMessage) error {
	for cluster, raw := range configs {
		_, err := p.manager.ApplyConfig(ctx, cell.Key(cluster), raw)
		if err == cell.ErrNotAssigned {
			continue
		}
		if err != nil {
			// Compile failure is isolated to this cluster (old engine keeps
			// serving); keep applying the rest.
			continue
		}
	}
	return nil
}
