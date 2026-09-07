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

package clustertable

import (
	"context"
	"encoding/json"
	"sync"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	fwkplugin "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
)

// PluginType is the EndpointPickerConfig dataLayer.discovery.endpoints pluginRef.
const PluginType = "cluster-table-discovery"

// Factory builds plugin instances bound to a hub. Register it in the
// composition root:
//
//	fwkplugin.Register(clustertable.PluginType, fwkplugin.StabilityBeta, clustertable.NewFactory(hub))
func NewFactory(hub *Hub) fwkplugin.FactoryFunc {
	return func(name string, parameters *json.Decoder, _ fwkplugin.Handle) (fwkplugin.Plugin, error) {
		var params struct {
			ClusterName string `json:"clusterName"`
		}
		if parameters != nil {
			if err := parameters.Decode(&params); err != nil {
				return nil, err
			}
		}
		return &discovery{
			hub:         hub,
			clusterName: params.ClusterName,
			applied:     map[k8stypes.NamespacedName]*fwkdl.EndpointMetadata{},
			ready:       make(chan struct{}),
		}, nil
	}
}

// discovery is one engine generation's view of a cluster's endpoints.
type discovery struct {
	hub         *Hub
	clusterName string

	mu      sync.Mutex
	applied map[k8stypes.NamespacedName]*fwkdl.EndpointMetadata
	ready   chan struct{}
	once    sync.Once
}

var _ fwkdl.EndpointDiscovery = &discovery{}

func (d *discovery) TypedName() fwkplugin.TypedName {
	name := d.clusterName
	if name == "" {
		name = "default"
	}
	return fwkplugin.TypedName{Type: PluginType, Name: name}
}

// Ready closes after the first endpoint set has been applied.
func (d *discovery) Ready() <-chan struct{} { return d.ready }

// Start applies hub snapshots until ctx ends. All notifier calls are made
// from this single goroutine, honoring the DiscoveryNotifier ordering
// contract; deletes are issued before upserts.
func (d *discovery) Start(ctx context.Context, notifier fwkdl.DiscoveryNotifier) error {
	for {
		desired, version := d.hub.Snapshot(d.clusterName)
		d.apply(notifier, desired)
		d.once.Do(func() { close(d.ready) })

		if _, err := d.hub.WaitChange(ctx, version); err != nil {
			return err
		}
	}
}

// apply diffs the desired set against the applied set. The notifier is not
// goroutine-safe; apply is only ever called from Start's loop.
func (d *discovery) apply(notifier fwkdl.DiscoveryNotifier, desired []fwkdl.EndpointMetadata) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Deletes first: endpoints that disappeared or were drained (Weight=0
	// entries never reach the desired set).
	for id := range d.applied {
		if !containsID(desired, id) {
			notifier.Delete(id)
			delete(d.applied, id)
		}
	}

	for i := range desired {
		meta := desired[i]
		id := meta.ID
		if old, ok := d.applied[id]; ok && old.Equal(&meta) {
			continue
		}
		notifier.Upsert(&meta)
		m := meta
		d.applied[id] = &m
	}
}

func containsID(eps []fwkdl.EndpointMetadata, id k8stypes.NamespacedName) bool {
	for i := range eps {
		if eps[i].ID == id {
			return true
		}
	}
	return false
}
