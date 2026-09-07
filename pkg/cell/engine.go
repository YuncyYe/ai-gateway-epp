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

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-router/pkg/epp/flowcontrol/controller"
	fwkdl "github.com/llm-d/llm-d-router/pkg/epp/framework/interface/datalayer"
	"github.com/llm-d/llm-d-router/pkg/epp/framework/interface/plugin"
	"github.com/llm-d/llm-d-router/pkg/epp/handlers"
	"github.com/llm-d/llm-d-router/pkg/epp/requestcontrol"
)

// Engine is one compiled generation of a Cell's policy plane. All request
// path state lives in llm-d components owned by this struct; swapping engines
// never mutates them in place.
type Engine struct {
	// Version is the content hash of the raw config that produced this engine.
	Version string

	director       *requestcontrol.Director
	fc             *controller.FlowController
	handle         plugin.Handle
	parserRegistry *handlers.ParserRegistry
	streamer       *handlers.StreamingServer
	discovery      []fwkdl.EndpointDiscovery

	ctx    context.Context
	cancel context.CancelFunc

	// wg counts in-flight requests; drained before the engine is dropped.
	wg sync.WaitGroup
}

// Streamer serves ext-proc streams against this engine's director.
func (e *Engine) Streamer() *handlers.StreamingServer { return e.streamer }

// FC returns the flow controller, nil when the flowControl feature gate is off.
func (e *Engine) FC() *controller.FlowController { return e.fc }

// start launches this generation's discovery plugins and marks the cell ready
// once the initial endpoint sync has landed.
func (e *Engine) start(c *Cell, logger logr.Logger) {
	if len(e.discovery) == 0 {
		c.markReady()
		return
	}
	readies := make([]<-chan struct{}, 0, len(e.discovery))
	for _, disc := range e.discovery {
		readies = append(readies, disc.Ready())
		go func(d fwkdl.EndpointDiscovery) {
			if err := d.Start(e.ctx, fwkdl.NewDiscoveryNotifier(c.ds)); err != nil {
				logger.Error(err, "discovery plugin stopped", "cell", string(c.Key))
			}
		}(disc)
	}
	go func() {
		for _, r := range readies {
			select {
			case <-r:
			case <-e.ctx.Done():
				return
			}
		}
		c.markReady()
	}()
}

// drain stops this generation (queued FC requests are evicted with a terminal
// outcome) and waits for in-flight requests up to timeout.
func (e *Engine) drain(timeout time.Duration) {
	if e.cancel != nil {
		e.cancel()
	}
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		// Force-drop: in-flight request metering is lost; llm-d metric TTLs recover.
	}
}

// HashConfig returns the canonical fingerprint of a raw cluster config.
func HashConfig(raw json.RawMessage) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
