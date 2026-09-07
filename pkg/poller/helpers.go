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

import "time"

// syncOnce is a tiny sync.Once wrapper so the generic file reads cleanly.
type syncOnce struct{ done bool }

func (o *syncOnce) do(f func()) {
	if !o.done {
		o.done = true
		f()
	}
}

// backoff implements exponential delay with a ceiling, for fetch failures.
type backoff struct {
	current time.Duration
	max     time.Duration
}

func (b *backoff) next() time.Duration {
	if b.current <= 0 {
		b.current = time.Second
		return b.current
	}
	b.current *= 2
	if b.max <= 0 {
		b.max = 30 * time.Second
	}
	if b.current > b.max {
		b.current = b.max
	}
	return b.current
}

func (b *backoff) reset() { b.current = 0 }
