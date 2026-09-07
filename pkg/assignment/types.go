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

// Package assignment defines the role-view contract between ai-gateway-api
// and an EPP instance: which clusters this instance serves, and as what role.
package assignment

// Role of this instance for a cluster.
type Role string

const (
	// RolePrimary: the instance serves traffic for the cluster.
	RolePrimary Role = "primary"
	// RoleStandby: the instance keeps hot data but admits no traffic.
	RoleStandby Role = "standby"
)

// View is this instance's role assignment: cluster name -> role.
type View map[string]Role

// CellStatus describes one local Cell for the readiness report.
type CellStatus struct {
	Key           string `json:"key"`
	Role          string `json:"role"`
	State         string `json:"state"`
	ReadySince    string `json:"ready_since,omitempty"`
	EngineVersion string `json:"engine_version,omitempty"`
}

// ReadyReport is posted by the EPP instance each assignment poll round so the
// allocator can sequence failovers ("sync before flip").
type ReadyReport struct {
	Instance string       `json:"instance"`
	Cells    []CellStatus `json:"cells"`
}

// Endpoint describes one EPP instance in the instance pool. The full pool
// topology lives on the ai-gateway-api side; the instance itself only learns
// its own roles through View.
type Endpoint struct {
	ID   string `json:"id"`
	Addr string `json:"addr"` // host:port
}
