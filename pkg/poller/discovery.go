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
	"net"
	"strconv"

	k8stypes "k8s.io/apimachinery/pkg/types"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/eppplugin/clustertable"
	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

// ClusterTablePath is the InnerAPI endpoint for the cluster table.
const ClusterTablePath = "/configs/gslb_data/cluster_table"

// BackendConf mirrors ai-gateway-api's cluster_table backend entry.
type BackendConf struct {
	Name   string `json:"Name"`
	Addr   string `json:"Addr"`
	Port   int    `json:"Port"`
	Weight int    `json:"Weight"`
}

// clusterTableConfig is Config[cluster][subCluster][]BackendConf.
type clusterTableConfig map[string]map[string][]BackendConf

// ClusterDiscovery polls the cluster table and pushes the desired endpoint
// set per cluster into the clustertable hub; the discovery plugins inside
// each cell diff and apply it to their datastore.
type ClusterDiscovery struct {
	client     *innerapi.Client
	hub        *clustertable.Hub
	assigned   func(cluster string) bool
	unassigned prometheusCounter
}

// prometheusCounter avoids importing prometheus here just for the skip count.
type prometheusCounter interface {
	Inc()
}

// NewClusterDiscovery creates the discovery source. assigned filters out
// clusters this instance does not hold (they are counted and skipped).
func NewClusterDiscovery(client *innerapi.Client, hub *clustertable.Hub, assigned func(cluster string) bool, unassigned prometheusCounter) *ClusterDiscovery {
	return &ClusterDiscovery{client: client, hub: hub, assigned: assigned, unassigned: unassigned}
}

// Fetch implements Source.
func (d *ClusterDiscovery) Fetch(ctx context.Context, version string) (bool, string, map[string][]fwkdl.EndpointMetadata, error) {
	var table clusterTableConfig
	changed, ver, err := d.client.Get(ctx, ClusterTablePath, version, &table)
	if err != nil || !changed {
		return changed, ver, nil, err
	}

	desired := make(map[string][]fwkdl.EndpointMetadata, len(table))
	for cluster, subClusters := range table {
		eps := []fwkdl.EndpointMetadata{}
		for _, backends := range subClusters {
			for _, b := range backends {
				if b.Weight == 0 {
					// Weight 0 means the instance is drained; absence from the
					// desired set drives the delete in the plugin diff.
					continue
				}
				eps = append(eps, fwkdl.EndpointMetadata{
					ID: k8stypes.NamespacedName{
						Namespace: cluster,
						Name:      backendID(b),
					},
					Name:        b.Name,
					Address:     unwrapIPv6(b.Addr),
					Port:        strconv.Itoa(b.Port),
					MetricsHost: net.JoinHostPort(unwrapIPv6(b.Addr), strconv.Itoa(b.Port)),
				})
			}
		}
		desired[cluster] = eps
	}
	return true, ver, desired, nil
}

// Handle implements Handle: replace the hub content with the fetched table so
// clusters that disappeared are cleared as well.
func (d *ClusterDiscovery) Handle(ctx context.Context, desired map[string][]fwkdl.EndpointMetadata) error {
	filtered := make(map[string][]fwkdl.EndpointMetadata, len(desired))
	for cluster, eps := range desired {
		if d.assigned != nil && !d.assigned(cluster) {
			if d.unassigned != nil {
				d.unassigned.Inc()
			}
			continue
		}
		filtered[cluster] = eps
	}
	d.hub.ReplaceAll(filtered)
	return nil
}

func backendID(b BackendConf) string {
	if b.Name != "" {
		return b.Name
	}
	return net.JoinHostPort(unwrapIPv6(b.Addr), strconv.Itoa(b.Port))
}

// unwrapIPv6 strips the brackets the cluster table wraps IPv6 addresses in.
func unwrapIPv6(addr string) string {
	if len(addr) > 2 && addr[0] == '[' && addr[len(addr)-1] == ']' {
		return addr[1 : len(addr)-1]
	}
	return addr
}
