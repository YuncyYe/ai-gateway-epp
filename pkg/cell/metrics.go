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
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	engineReloads = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "ai_epp",
		Name:      "engine_reloads_total",
		Help:      "Engine compile/swap attempts by cluster and result.",
	}, []string{"cluster", "result"})

	engineCurrentVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ai_epp",
		Name:      "engine_current_version",
		Help:      "Content hash of the currently active engine config, as a labeled gauge value of 1.",
	}, []string{"cluster", "version"})

	cellStateGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "ai_epp",
		Name:      "cell_state",
		Help:      "Cell state by cluster, role and state; 1 for the current combination.",
	}, []string{"cluster", "role", "state"})

	drainDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "ai_epp",
		Name:      "drain_duration_seconds",
		Help:      "Time to drain a replaced engine by cluster.",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12),
	}, []string{"cluster"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(engineReloads, engineCurrentVersion, cellStateGauge, drainDuration)
}
