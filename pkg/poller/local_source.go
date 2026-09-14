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
	"fmt"
	"os"
	"path/filepath"

	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
)

// LocalFileSource loads config data from a local JSON file on every Fetch
// call. It implements Source[T] for use with the generic Poller framework.
// T is the target type that the JSON file is unmarshaled into.
type LocalFileSource[T any] struct {
	path string
}

// NewLocalFileSource creates a Source that reads and JSON-decodes the file at
// dir/filename. The file must contain a single JSON value of type T.
func NewLocalFileSource[T any](dir, filename string) *LocalFileSource[T] {
	return &LocalFileSource[T]{path: filepath.Join(dir, filename)}
}

// Fetch implements Source. Each call reads and unmarshals the file; changed
// is always true (no version tracking in local file mode), newVersion is
// always empty.
func (s *LocalFileSource[T]) Fetch(_ context.Context, _ string) (bool, string, T, error) {
	var data T
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return false, "", data, fmt.Errorf("local source read %s: %w", s.path, err)
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return false, "", data, fmt.Errorf("local source decode %s: %w", s.path, err)
	}
	return true, "", data, nil
}

// localClusterTableSource reads cluster_table.json in the raw
// ClusterTableConfig (BackendConf) format and converts it on-the-fly to the
// endpoint metadata map consumed by ClusterDiscovery.Handle.
type localClusterTableSource struct {
	path string
}

// NewLocalClusterTableSource creates a Source[map[string][]EndpointMetadata]
// that loads the cluster table from a local file in ClusterTableConfig format.
func NewLocalClusterTableSource(dir, filename string) Source[map[string][]fwkdl.EndpointMetadata] {
	return &localClusterTableSource{path: filepath.Join(dir, filename)}
}

func (s *localClusterTableSource) Fetch(_ context.Context, _ string) (bool, string, map[string][]fwkdl.EndpointMetadata, error) {
	raw, err := os.ReadFile(s.path)
	if err != nil {
		return false, "", nil, fmt.Errorf("local source read %s: %w", s.path, err)
	}
	var table ClusterTableConfig
	if err := json.Unmarshal(raw, &table); err != nil {
		return false, "", nil, fmt.Errorf("local source decode %s: %w", s.path, err)
	}
	return true, "", ConvertClusterTableToEndpoints(table), nil
}