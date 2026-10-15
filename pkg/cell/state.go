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

// State is the Cell lifecycle state. There is no role dimension: a held cell
// always serves (single-instance group model, see
// docs/zh_cn/modifications/2026-10-14-epp-single-instance/).
type State int32

const (
	// StateCreating: created, waiting for discovery sync and first engine compile.
	StateCreating State = iota
	// StateActive: ready and serving.
	StateActive
	// StateDraining: assignment removed, tearing down.
	StateDraining
)

func (s State) String() string {
	switch s {
	case StateCreating:
		return "creating"
	case StateActive:
		return "active"
	case StateDraining:
		return "draining"
	default:
		return "unknown"
	}
}
