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
	"os"
	"path/filepath"
	"strings"
	"testing"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/innerapi"
)

func TestLocalFileSource_Fetch(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "test.json", `{"k":"v"}`)
	src := NewLocalFileSource[map[string]string](dir, "test.json")

	changed, ver, data, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected changed=true")
	}
	if ver != "" {
		t.Fatalf("expected empty version, got %q", ver)
	}
	if data["k"] != "v" {
		t.Fatalf("data = %v", data)
	}
}

func TestLocalFileSource_FileNotFound(t *testing.T) {
	dir := t.TempDir()
	src := NewLocalFileSource[map[string]string](dir, "nonexistent.json")
	_, _, _, err := src.Fetch(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "local source read") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLocalFileSource_BadJSON(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "bad.json", `{not json`)
	src := NewLocalFileSource[map[string]string](dir, "bad.json")
	_, _, _, err := src.Fetch(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "local source decode") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLocalFileSource_VersionIgnored(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "test.json", `{"a":1}`)
	src := NewLocalFileSource[map[string]int](dir, "test.json")

	for _, version := range []string{"", "v123", "any"} {
		changed, ver, data, err := src.Fetch(context.Background(), version)
		if err != nil {
			t.Fatalf("version=%q: %v", version, err)
		}
		if !changed {
			t.Fatalf("version=%q: expected changed=true", version)
		}
		if ver != "" {
			t.Fatalf("version=%q: expected empty new version, got %q", version, ver)
		}
		if data["a"] != 1 {
			t.Fatalf("version=%q: data = %v", version, data)
		}
	}
}

func TestLocalFileSource_EppDataConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := innerapi.EppDataConfig{
		EppConfig: map[string]json.RawMessage{"c1": json.RawMessage(`{"plugins":[]}`)},
		Assignment: map[string]innerapi.AssignmentEntry{
			"c1": {Primary: ptr("epp-1"), Standby: nil},
		},
	}
	writeFile(t, dir, "epp_data_config.json", mustMarshal(t, cfg))

	src := NewLocalFileSource[innerapi.EppDataConfig](dir, "epp_data_config.json")
	changed, ver, data, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected changed=true")
	}
	if ver != "" {
		t.Fatalf("expected empty version, got %q", ver)
	}
	if data.Assignment["c1"].Primary == nil || *data.Assignment["c1"].Primary != "epp-1" {
		t.Fatalf("assignment = %+v", data.Assignment)
	}
}

func TestNewLocalClusterTableSource_Fetch(t *testing.T) {
	dir := t.TempDir()
	table := ClusterTableConfig{
		"cluster_epp_sim": {
			"cluster_epp_sim": []BackendConf{
				{Name: "ep1", Addr: "10.0.0.1", Port: 8080, Weight: 100},
				{Name: "ep2", Addr: "10.0.0.2", Port: 8080, Weight: 50},
			},
		},
	}
	writeFile(t, dir, "cluster_table.json", mustMarshal(t, table))

	src := NewLocalClusterTableSource(dir, "cluster_table.json")
	changed, ver, eps, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed {
		t.Fatal("expected changed=true")
	}
	if ver != "" {
		t.Fatalf("expected empty version, got %q", ver)
	}
	list := eps["cluster_epp_sim"]
	if len(list) != 2 {
		t.Fatalf("expected 2 endpoints, got %d", len(list))
	}
	if list[0].Address != "10.0.0.1" || list[0].Port != "8080" {
		t.Fatalf("ep1 = %+v", list[0])
	}
	if list[1].Address != "10.0.0.2" || list[1].Port != "8080" {
		t.Fatalf("ep2 = %+v", list[1])
	}
}

func TestNewLocalClusterTableSource_WeightZeroSkipped(t *testing.T) {
	dir := t.TempDir()
	table := ClusterTableConfig{
		"c1": {
			"c1": []BackendConf{
				{Name: "keep", Addr: "10.0.0.1", Port: 8080, Weight: 100},
				{Name: "skip", Addr: "10.0.0.2", Port: 8080, Weight: 0},
			},
		},
	}
	writeFile(t, dir, "cluster_table.json", mustMarshal(t, table))

	src := NewLocalClusterTableSource(dir, "cluster_table.json")
	_, _, eps, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	list := eps["c1"]
	if len(list) != 1 {
		t.Fatalf("expected 1 endpoint (weight=0 skipped), got %d", len(list))
	}
	if list[0].Name != "keep" {
		t.Fatalf("expected 'keep', got %q", list[0].Name)
	}
}

func TestNewLocalClusterTableSource_IPv6Unwrap(t *testing.T) {
	dir := t.TempDir()
	table := ClusterTableConfig{
		"c1": {
			"c1": []BackendConf{
				{Name: "v6", Addr: "[::1]", Port: 8080, Weight: 100},
			},
		},
	}
	writeFile(t, dir, "cluster_table.json", mustMarshal(t, table))

	src := NewLocalClusterTableSource(dir, "cluster_table.json")
	_, _, eps, err := src.Fetch(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if eps["c1"][0].Address != "::1" {
		t.Fatalf("expected unwrapped '::1', got %q", eps["c1"][0].Address)
	}
}

func TestNewLocalClusterTableSource_FileNotFound(t *testing.T) {
	src := NewLocalClusterTableSource(t.TempDir(), "missing.json")
	_, _, _, err := src.Fetch(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestNewLocalClusterTableSource_BadJSON(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, dir, "cluster_table.json", `{bad`)
	src := NewLocalClusterTableSource(dir, "cluster_table.json")
	_, _, _, err := src.Fetch(context.Background(), "")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestConvertClusterTableToEndpoints_Empty(t *testing.T) {
	eps := ConvertClusterTableToEndpoints(nil)
	if len(eps) != 0 {
		t.Fatalf("expected empty, got %v", eps)
	}

	var table ClusterTableConfig
	var want map[string][]fwkdl.EndpointMetadata
	// nil map and empty map both yield empty result on range.
	eps = ConvertClusterTableToEndpoints(table)
	if len(eps) != len(want) {
		t.Fatalf("expected empty, got %v", eps)
	}
}
func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func ptr(s string) *string { return &s }