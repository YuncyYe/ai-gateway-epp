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

// Package innerapi implements the HTTP client for the ai-gateway-api
// InnerAPI: auth header, unified envelope parsing, version-increment fetch.
package innerapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const errNumOK = 200

// envelope mirrors the unified InnerAPI response envelope.
type envelope struct {
	ErrNum   int             `json:"ErrNum"`
	ErrMsg   string          `json:"ErrMsg"`
	Data     json.RawMessage `json:"Data"`
	WorkMode string          `json:"WorkMode"`
}

// Versioned is the shared shape of versioned config payloads
// (Data.Config carries the actual config, keyed by cluster).
type Versioned[T any] struct {
	Version string `json:"Version"`
	Config  T      `json:"Config"`
}

// EppDataConfigPath is the InnerAPI endpoint for the merged epp_data
// snapshot: compiled per-cluster configs and the full assignment view in
// one versioned payload.
const EppDataConfigPath = "/configs/epp_data/config"

// EppDataConfig is the two-section epp_data snapshot: EppConfig maps a
// cluster to the api-compiled EndpointPickerConfig, Assignment maps a
// cluster to its instance group (full view, identical for every instance).
type EppDataConfig struct {
	EppConfig  map[string]json.RawMessage `json:"epp_config"`
	Assignment map[string]AssignmentEntry `json:"assignment"`
}

// AssignmentEntry is one cluster's instance group in the full assignment
// view. Primary/Standby hold instance ids; a null Primary is abnormal (the
// cluster matches no instance as primary), a null Standby is a
// single-instance group.
type AssignmentEntry struct {
	Primary *string `json:"primary"`
	Standby *string `json:"standby"`
}

// Client is a thin HTTP client for the InnerAPI. It is safe for concurrent
// use; a single instance is shared by all pollers.
type Client struct {
	baseURL string
	token   string
	hc      *http.Client
}

// NewClient creates a client for baseURL (e.g. "http://api-server:8181/inner-api/v1").
// timeout bounds a single request including retries inside net/http.
func NewClient(baseURL, token string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 3 * time.Second
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		hc:      &http.Client{Timeout: timeout},
	}
}

// Get fetches path with the client-side version. It returns changed=false
// when the server reports no newer data (Data == null). out receives the
// decoded Config payload; pass *json.RawMessage to keep the raw config.
func (c *Client) Get(ctx context.Context, path, version string, out any) (changed bool, newVersion string, err error) {
	u := c.baseURL + path
	if version != "" {
		sep := "?"
		if strings.Contains(u, "?") {
			sep = "&"
		}
		u += sep + "version=" + url.QueryEscape(version)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, "", err
	}
	c.setAuth(req)

	resp, err := c.hc.Do(req)
	if err != nil {
		return false, "", fmt.Errorf("innerapi get %s: %w", path, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	if err != nil {
		return false, "", fmt.Errorf("innerapi read %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		return false, "", fmt.Errorf("innerapi get %s: status %d: %s", path, resp.StatusCode, truncate(string(body), 200))
	}

	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return false, "", fmt.Errorf("innerapi decode %s: %w", path, err)
	}
	if env.ErrNum != errNumOK {
		return false, "", fmt.Errorf("innerapi get %s: errnum %d: %s", path, env.ErrNum, env.ErrMsg)
	}
	if len(env.Data) == 0 || bytes.Equal(bytes.TrimSpace(env.Data), []byte("null")) {
		return false, version, nil
	}

	var payload struct {
		Version string          `json:"Version"`
		Config  json.RawMessage `json:"Config"`
	}
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		return false, "", fmt.Errorf("innerapi decode data %s: %w", path, err)
	}
	if out != nil {
		if err := json.Unmarshal(payload.Config, out); err != nil {
			return false, "", fmt.Errorf("innerapi decode config %s: %w", path, err)
		}
	}
	return true, payload.Version, nil
}

func (c *Client) setAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Token "+c.token)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
