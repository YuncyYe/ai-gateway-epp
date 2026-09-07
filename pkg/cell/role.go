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

package cell

// Key identifies a Cell; it equals the cluster name carried in the
// inference-pool ext-proc metadata and the cluster_table config key.
type Key string

// Role is this instance's assignment for a cluster.
type Role int32

const (
	// RoleNone: freshly created, assignment not yet applied.
	RoleNone Role = iota
	// RoleStandby: keeps hot data, admits no traffic.
	RoleStandby
	// RolePrimary: serves traffic.
	RolePrimary
)

func (r Role) String() string {
	switch r {
	case RoleStandby:
		return "standby"
	case RolePrimary:
		return "primary"
	default:
		return "none"
	}
}

// State is the Cell lifecycle state.
type State int32

const (
	// StateCreating: created, waiting for discovery sync and first engine compile.
	StateCreating State = iota
	// StateStandby: ready as standby.
	StateStandby
	// StatePrimary: ready and serving.
	StatePrimary
	// StateDraining: assignment removed, tearing down.
	StateDraining
)

func (s State) String() string {
	switch s {
	case StateCreating:
		return "creating"
	case StateStandby:
		return "standby"
	case StatePrimary:
		return "primary"
	case StateDraining:
		return "draining"
	default:
		return "unknown"
	}
}
