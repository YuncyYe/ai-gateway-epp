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

// Package common provides the integration-test harness: real ai-gateway-epp
// and llm-d-inference-sim processes plus an in-process ai-gateway-api mock.
// Modeled after bfe/tests/integration/common.
package common

import (
	"fmt"
	"net"
	"os"
	"time"
)

// FindFreePort returns a free loopback TCP port.
func FindFreePort(t fatalT) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}

// WaitForTCP polls addr until it accepts a connection or the timeout ends.
func WaitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s: %w", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// RepoRoot locates the ai-gateway-epp repository root by walking up from the
// test file location until go.mod with our module path appears.
func RepoRoot(t fatalT) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(wd + "/go.mod"); err == nil {
			return wd
		}
		parent := dirOf(wd)
		if parent == wd {
			t.Fatalf("repo root not found from %s", wd)
		}
		wd = parent
	}
}

func dirOf(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			if i == 0 {
				return p[:1]
			}
			return p[:i]
		}
	}
	return p
}

type fatalT interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
	Skip(args ...any)
}
