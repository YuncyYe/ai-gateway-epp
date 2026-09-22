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

// Package poller implements the generic version-increment polling framework
// and the two concrete pollers (cluster discovery, epp_data) that pull state
// from ai-gateway-api into the local cells.
package poller

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	lastSyncTimestamp = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ai_epp",
		Name:      "poller_last_sync_timestamp",
		Help:      "Unix timestamp of the last successful sync by poller.",
	}, []string{"poller"})

	failuresTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ai_epp",
		Name:      "poller_failures_total",
		Help:      "Failed fetch/handle rounds by poller.",
	}, []string{"poller"})

	backoffState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ai_epp",
		Name:      "poller_backoff_state",
		Help:      "1 while the poller is in exponential backoff.",
	}, []string{"poller"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(lastSyncTimestamp, failuresTotal, backoffState)
}

// Source fetches one round of data. changed=false means the server reported
// no newer version; version is the last server-side version seen.
type Source[T any] interface {
	Fetch(ctx context.Context, version string) (changed bool, newVersion string, data T, err error)
}

// Handle applies one round of changed data. An error discards the round
// without advancing the version, so corrupt data never becomes sticky.
type Handle[T any] func(ctx context.Context, data T) error

// Options tunes the polling loop.
type Options struct {
	Interval time.Duration // normal tick
	Timeout  time.Duration // per-fetch deadline
	Logger   logr.Logger
}

// Poller is a generic version-increment polling loop with exponential
// backoff. The zero value is not usable; construct with New.
type Poller[T any] struct {
	name   string
	src    Source[T]
	handle Handle[T]
	opts   Options

	version   string
	firstSync chan struct{}
	once      syncOnce
	backoff   backoff
}

// New creates a Poller. name labels metrics.
func New[T any](name string, src Source[T], h Handle[T], opts Options) *Poller[T] {
	return &Poller[T]{
		name:      name,
		src:       src,
		handle:    h,
		opts:      opts,
		firstSync: make(chan struct{}),
	}
}

// FirstSync closes after the first round that fetched and applied cleanly.
func (p *Poller[T]) FirstSync() <-chan struct{} { return p.firstSync }

// Version returns the last applied server-side version.
func (p *Poller[T]) Version() string { return p.version }

// Start runs the loop until ctx is canceled. Handle failures do not stop the
// loop (fail-static: consumers keep the last known good state).
func (p *Poller[T]) Start(ctx context.Context) error {
	log := p.opts.Logger.WithValues("poller", p.name)
	log.V(2).Info("poller starting", "interval", p.opts.Interval)
	interval := p.opts.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	delay := time.Duration(0)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}

		fetchCtx, cancel := context.WithTimeout(ctx, p.opts.Timeout)
		changed, ver, data, err := p.src.Fetch(fetchCtx, p.version)
		cancel()

		if err != nil {
			p.fail(log, err)
			delay = p.backoff.next()
			backoffState.WithLabelValues(p.name).Set(1)
			continue
		}
		log.V(2).Info("fetch completed", "changed", changed, "version", ver)
		p.backoff.reset()
		backoffState.WithLabelValues(p.name).Set(0)

		if changed {
			log.V(2).Info("applying changes")
			if err := p.handle(ctx, data); err != nil {
				// Do not advance the version: re-fetch the same round next tick.
				p.fail(log, err)
				delay = interval
				continue
			}
			log.V(2).Info("changes applied", "version", ver)
			p.version = ver
		}
		lastSyncTimestamp.WithLabelValues(p.name).Set(float64(time.Now().Unix()))
		p.once.do(func() { close(p.firstSync) })
		delay = interval
	}
}

func (p *Poller[T]) fail(log logr.Logger, err error) {
	failuresTotal.WithLabelValues(p.name).Inc()
	log.Error(err, "poll round failed")
}
