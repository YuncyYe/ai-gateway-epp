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

package assignment

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRoleConstants(t *testing.T) {
	if RolePrimary != "primary" {
		t.Errorf("RolePrimary = %q, want %q", RolePrimary, "primary")
	}
	if RoleStandby != "standby" {
		t.Errorf("RoleStandby = %q, want %q", RoleStandby, "standby")
	}
	if RolePrimary == RoleStandby {
		t.Error("RolePrimary and RoleStandby must differ")
	}
}

func TestViewRoleLookup(t *testing.T) {
	tests := []struct {
		name    string
		view    View
		cluster string
		want    Role
		wantOK  bool
	}{
		{"cluster with primary role", View{"c1": RolePrimary}, "c1", RolePrimary, true},
		{"cluster with standby role", View{"c1": RoleStandby}, "c1", RoleStandby, true},
		{"missing cluster", View{"c1": RolePrimary}, "c2", "", false},
		{"nil view", nil, "c1", "", false},
		{"empty view", View{}, "c1", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			role, ok := tt.view[tt.cluster]
			if role != tt.want {
				t.Errorf("view[%q] = %q, want %q", tt.cluster, role, tt.want)
			}
			if ok != tt.wantOK {
				t.Errorf("view[%q] present = %v, want %v", tt.cluster, ok, tt.wantOK)
			}
		})
	}
}

func TestViewMutationAndIsolation(t *testing.T) {
	v := View{}
	v["c1"] = RolePrimary
	if got := v["c1"]; got != RolePrimary {
		t.Errorf("after insert, view[%q] = %q, want %q", "c1", got, RolePrimary)
	}
	delete(v, "c1")
	if _, ok := v["c1"]; ok {
		t.Error("entry still present after delete")
	}

	// Maps are shared by reference: mutating the copy affects the original.
	base := View{"c1": RolePrimary}
	alias := base
	alias["c2"] = RoleStandby
	if _, ok := base["c2"]; !ok {
		t.Error("expected aliased map mutation to be visible in original view")
	}
}

func TestCellStatusJSONRoundTrip(t *testing.T) {
	in := CellStatus{
		Key:           "cluster-a",
		Role:          "primary",
		State:         "ready",
		ReadySince:    "2026-03-01T00:00:00Z",
		EngineVersion: "v1.2.3",
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out CellStatus
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestCellStatusJSONFieldNames(t *testing.T) {
	data, err := json.Marshal(CellStatus{
		Key: "k", Role: "primary", State: "ready",
		ReadySince: "ts", EngineVersion: "v",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"key":"k","role":"primary","state":"ready","ready_since":"ts","engine_version":"v"}`
	if string(data) != want {
		t.Errorf("marshaled = %s, want %s", data, want)
	}
}

func TestCellStatusOmitEmpty(t *testing.T) {
	// ReadySince and EngineVersion carry omitempty; the other fields do not.
	data, err := json.Marshal(CellStatus{Key: "k", Role: "primary", State: "ready"})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"key":"k","role":"primary","state":"ready"}`
	if string(data) != want {
		t.Errorf("marshaled = %s, want %s", data, want)
	}
}

func TestCellStatusUnmarshalZeroValue(t *testing.T) {
	var cs CellStatus
	if err := json.Unmarshal([]byte(`{}`), &cs); err != nil {
		t.Fatalf("unmarshal empty object: %v", err)
	}
	if cs != (CellStatus{}) {
		t.Errorf("expected zero value, got %+v", cs)
	}
}

func TestReadyReportJSONRoundTrip(t *testing.T) {
	in := ReadyReport{
		Instance: "epp-1",
		Cells: []CellStatus{
			{Key: "c1", Role: "primary", State: "ready"},
			{Key: "c2", Role: "standby", State: "syncing", EngineVersion: "v2"},
		},
	}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"instance":"epp-1","cells":[{"key":"c1","role":"primary","state":"ready"},` +
		`{"key":"c2","role":"standby","state":"syncing","engine_version":"v2"}]}`
	if string(data) != want {
		t.Errorf("marshaled = %s, want %s", data, want)
	}

	var out ReadyReport
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestReadyReportEmptyCells(t *testing.T) {
	in := ReadyReport{Instance: "epp-1"}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Cells has no omitempty, so it serializes as null when nil.
	want := `{"instance":"epp-1","cells":null}`
	if string(data) != want {
		t.Errorf("marshaled = %s, want %s", data, want)
	}

	var out ReadyReport
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}

func TestReadyReportUnmarshalTypeMismatch(t *testing.T) {
	var r ReadyReport
	if err := json.Unmarshal([]byte(`{"cells":"not-an-array"}`), &r); err == nil {
		t.Error("expected error unmarshaling cells as string, got nil")
	}
}

func TestEndpointJSONRoundTrip(t *testing.T) {
	in := Endpoint{ID: "epp-1", Addr: "10.0.0.1:9000"}
	data, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := `{"id":"epp-1","addr":"10.0.0.1:9000"}`
	if string(data) != want {
		t.Errorf("marshaled = %s, want %s", data, want)
	}

	var out Endpoint
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out != in {
		t.Errorf("round trip mismatch: got %+v, want %+v", out, in)
	}
}
