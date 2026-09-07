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

package common

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

const eppBinaryName = "ai-gateway-epp-integration.exe"

var (
	eppBuildOnce sync.Once
	eppBuildErr  error
	eppBinPath   string
)

// EppBinary builds (or reuses, within this test process, the build of) the
// epp binary and returns its path. Each test package runs in its own test
// binary process and gets its own private copy in a process-unique temp dir,
// so parallel packages never share one executable path — on Windows an .exe
// that is being started or is running cannot be replaced or removed, which
// made a shared cache under test/ racy. The build itself is still cheap after
// the first one thanks to the go build cache. The temp dir is intentionally
// left in place for the OS to clean up: it must outlive every test in this
// process, and tests within a package may run in any order.
func EppBinary(t fatalT) string {
	t.Helper()
	root := RepoRoot(t)
	eppBuildOnce.Do(func() {
		binDir, err := os.MkdirTemp("", "ai-gateway-epp-test-bin")
		if err != nil {
			eppBuildErr = err
			return
		}
		bin := filepath.Join(binDir, eppBinaryName)
		cmd := exec.Command("go", "build", "-o", bin, "./cmd/epp")
		cmd.Dir = root
		cmd.Stderr = os.Stderr
		if out, err := cmd.Output(); err != nil {
			eppBuildErr = fmt.Errorf("build epp: %v: %s", err, out)
			return
		}
		eppBinPath = bin
	})
	if eppBuildErr != nil {
		t.Fatalf("%v", eppBuildErr)
	}
	return eppBinPath
}

// Process is a managed child process with log capture.
type Process struct {
	Cmd     *exec.Cmd
	logPath string
	logFile *os.File
}

// StartProcess launches argv[0] with args, redirecting output to logPath.
func StartProcess(t fatalT, logDir, name string, args ...string) *Process {
	t.Helper()
	logPath := filepath.Join(logDir, name+".log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create log file: %v", err)
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		logFile.Close()
		t.Fatalf("start %s: %v", name, err)
	}
	return &Process{Cmd: cmd, logPath: logPath, logFile: logFile}
}

// Stop kills the process, waits for exit, and closes the log file handle
// (required on Windows, where an open handle blocks TempDir cleanup). Wait is
// skipped if the caller already waited on the command (e.g. tests asserting
// startup failure).
func (p *Process) Stop(t fatalT) {
	t.Helper()
	if p.Cmd.Process != nil {
		p.Cmd.Process.Kill()
	}
	if p.Cmd.ProcessState == nil {
		if err := p.Cmd.Wait(); err != nil {
			// ExitError from Kill is expected; surface anything else via log dump.
			if _, ok := err.(*exec.ExitError); !ok {
				t.Logf("wait %s: %v (log: %s)", p.Cmd.Path, err, p.logPath)
			}
		}
	}
	if p.logFile != nil {
		p.logFile.Close()
		p.logFile = nil
	}
}

// LogPath returns the process log file path for diagnostics.
func (p *Process) LogPath() string { return p.logPath }

// SimBinary locates the inference-sim binary: $INFERENCE_SIM_BIN, then the
// sibling checkout at <repo>/../llm-d-inference-sim/bin, then
// test/.sim-bin/. Returns "" when unavailable (tests should t.Skip).
func SimBinary(t fatalT) string {
	t.Helper()
	if p := os.Getenv("INFERENCE_SIM_BIN"); p != "" {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	root := RepoRoot(t)
	for _, p := range []string{
		filepath.Join(root, "..", "llm-d-inference-sim", "bin", "llm-d-inference-sim.exe"),
		filepath.Join(root, "..", "llm-d-inference-sim", "bin", "llm-d-inference-sim"),
		filepath.Join(root, "test", ".sim-bin", "llm-d-inference-sim.exe"),
		filepath.Join(root, "test", ".sim-bin", "llm-d-inference-sim"),
	} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// StartSim starts one inference-sim instance on a free port in echo mode with
// near-zero latency, and waits for its HTTP port.
func StartSim(t fatalT, logDir, name, model string) (proc *Process, addr string) {
	t.Helper()
	return StartSimWithLatency(t, logDir, name, model, 2*time.Millisecond, time.Millisecond)
}

// StartSimWithLatency is StartSim with explicit simulated latencies, for
// scenarios that need a slow backend (e.g. flow-control contention).
func StartSimWithLatency(t fatalT, logDir, name, model string, ttf, itl time.Duration) (proc *Process, addr string) {
	t.Helper()
	return startSim(t, logDir, name, model, ttf, itl)
}

// StartSimWithFakeMetrics starts a sim with fake metrics enabled (initially
// fakeMetricsJSON, e.g. `{}` to enable with no values). Enabling at startup is
// required: a sim reporting real metrics rejects /admin/config fake-metrics
// updates.
func StartSimWithFakeMetrics(t fatalT, logDir, name, model, fakeMetricsJSON string) (proc *Process, addr string) {
	t.Helper()
	return startSim(t, logDir, name, model, 2*time.Millisecond, time.Millisecond, "--fake-metrics", fakeMetricsJSON)
}

func startSim(t fatalT, logDir, name, model string, ttf, itl time.Duration, extra ...string) (proc *Process, addr string) {
	t.Helper()
	bin := SimBinary(t)
	if bin == "" {
		t.Skip("llm-d-inference-sim binary not found (set INFERENCE_SIM_BIN or build ../llm-d-inference-sim)")
	}
	port := FindFreePort(t)
	args := []string{
		bin,
		"--port", fmt.Sprint(port),
		"--model", model,
		"--mode", "echo",
		"--time-to-first-token", ttf.String(),
		"--inter-token-latency", itl.String(),
	}
	args = append(args, extra...)
	proc = StartProcess(t, logDir, name, args...)
	addr = fmt.Sprintf("127.0.0.1:%d", port)
	if err := WaitForTCP(addr, 30*time.Second); err != nil {
		proc.Stop(t)
		t.Fatalf("sim %s not ready: %v (log: %s)", name, err, proc.LogPath())
	}
	return proc, addr
}
