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
	"testing"
	"time"
)

func TestSyncOnceDo(t *testing.T) {
	var o syncOnce
	calls := 0
	o.do(func() { calls++ })
	o.do(func() { calls++ })
	o.do(func() { calls++ })
	if calls != 1 {
		t.Fatalf("do ran %d times, want 1", calls)
	}
	if o.done != true {
		t.Fatal("done flag not set")
	}
}

func TestBackoffNext(t *testing.T) {
	tests := []struct {
		name string
		max  time.Duration
		want []time.Duration
	}{
		{
			name: "doubles until the default ceiling",
			max:  0,
			want: []time.Duration{
				time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
				16 * time.Second, 30 * time.Second, 30 * time.Second,
			},
		},
		{
			name: "respects a custom ceiling",
			max:  5 * time.Second,
			want: []time.Duration{
				time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &backoff{max: tt.max}
			for i, want := range tt.want {
				if got := b.next(); got != want {
					t.Fatalf("next() #%d = %v, want %v", i, got, want)
				}
			}
		})
	}
}

func TestBackoffReset(t *testing.T) {
	b := &backoff{}
	if got := b.next(); got != time.Second {
		t.Fatalf("first next() = %v, want 1s", got)
	}
	if got := b.next(); got != 2*time.Second {
		t.Fatalf("second next() = %v, want 2s", got)
	}
	b.reset()
	if b.current != 0 {
		t.Fatalf("current = %v after reset", b.current)
	}
	if got := b.next(); got != time.Second {
		t.Fatalf("next() after reset = %v, want 1s", got)
	}
}
