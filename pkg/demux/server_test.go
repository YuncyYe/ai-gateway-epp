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

package demux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"unsafe"

	configPb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	ctrlLog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/llm-d/llm-d-router/pkg/epp/handlers"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
)

// fakeProcessStream is a scripted ExternalProcessor_ProcessServer: it replays
// the queued messages, then returns restErr (io.EOF by default).
type fakeProcessStream struct {
	ctx     context.Context
	mu      sync.Mutex
	msgs    []*extProcPb.ProcessingRequest
	restErr error
	recvFn  func() (*extProcPb.ProcessingRequest, error)
	sent    []*extProcPb.ProcessingResponse
}

func newFakeStream(msgs ...*extProcPb.ProcessingRequest) *fakeProcessStream {
	return &fakeProcessStream{
		ctx:     ctrlLog.IntoContext(context.Background(), logr.Discard()),
		msgs:    msgs,
		restErr: io.EOF,
	}
}

func (f *fakeProcessStream) Recv() (*extProcPb.ProcessingRequest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.recvFn != nil {
		return f.recvFn()
	}
	if len(f.msgs) == 0 {
		return nil, f.restErr
	}
	msg := f.msgs[0]
	f.msgs = f.msgs[1:]
	return msg, nil
}

func (f *fakeProcessStream) Send(resp *extProcPb.ProcessingResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, resp)
	return nil
}

// Unused methods to satisfy the grpc.ServerStream interface.
func (f *fakeProcessStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeProcessStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeProcessStream) SetTrailer(metadata.MD)       {}
func (f *fakeProcessStream) Context() context.Context     { return f.ctx }
func (f *fakeProcessStream) SendMsg(any) error            { return nil }
func (f *fakeProcessStream) RecvMsg(any) error            { return nil }

// headersRequest builds the first ext-proc message BFE would send. pool == ""
// omits the inference-pool metadata.
func headersRequest(pool string) *extProcPb.ProcessingRequest {
	req := &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extProcPb.HttpHeaders{
				Headers: &configPb.HeaderMap{Headers: []*configPb.HeaderValue{
					{Key: ":path", Value: "/v1/chat/completions"},
					{Key: ":method", Value: "POST"},
				}},
				// EndOfStream=false keeps the scripted stream on the cheap
				// header-only path of the llm-d streamer.
				EndOfStream: false,
			},
		},
	}
	if pool != "" {
		md, err := structpb.NewStruct(map[string]any{PoolMetadataKey: pool})
		if err != nil {
			panic(err)
		}
		req.MetadataContext = &configPb.Metadata{
			FilterMetadata: map[string]*structpb.Struct{MetadataNamespace: md},
		}
	}
	return req
}

func bodyRequest() *extProcPb.ProcessingRequest {
	return &extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestBody{
			RequestBody: &extProcPb.HttpBody{Body: []byte("{}"), EndOfStream: true},
		},
	}
}

// compileWithStreamer returns a CompileFunc whose engines carry a real
// StreamingServer. cell.Engine exposes no setter for the streamer, so the
// unexported field is injected via reflection to keep the production API
// surface unchanged.
func compileWithStreamer(streamer *handlers.StreamingServer) cell.CompileFunc {
	return func(_ context.Context, _ cell.Key, raw json.RawMessage, _ *cell.Cell) (*cell.Engine, error) {
		eng := &cell.Engine{Version: cell.HashConfig(raw)}
		f := reflect.ValueOf(eng).Elem().FieldByName("streamer")
		reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Set(reflect.ValueOf(streamer))
		return eng, nil
	}
}

// setupPool ensures the cell exists with the given role and applies a config
// generation (unless raw is empty, which leaves the engine unset).
func setupPool(t *testing.T, m *cell.Manager, key string, role cell.Role, raw string) *cell.Cell {
	t.Helper()
	c, err := m.Ensure(context.Background(), cell.Key(key), role)
	if err != nil {
		t.Fatal(err)
	}
	if raw != "" {
		if _, err := m.ApplyConfig(context.Background(), cell.Key(key), json.RawMessage(raw)); err != nil {
			t.Fatal(err)
		}
	}
	return c
}

func codeOf(err error) codes.Code { return status.Code(err) }

func TestExtractPool(t *testing.T) {
	mdFor := func(values map[string]any) *configPb.Metadata {
		md, err := structpb.NewStruct(values)
		if err != nil {
			t.Fatal(err)
		}
		return &configPb.Metadata{
			FilterMetadata: map[string]*structpb.Struct{MetadataNamespace: md},
		}
	}
	otherNs := &configPb.Metadata{
		FilterMetadata: map[string]*structpb.Struct{
			"other.io": func() *structpb.Struct {
				md, _ := structpb.NewStruct(map[string]any{"k": "v"})
				return md
			}(),
		},
	}

	tests := []struct {
		name string
		req  *extProcPb.ProcessingRequest
		want string
	}{
		{"nil request", nil, ""},
		{"no metadata context", &extProcPb.ProcessingRequest{}, ""},
		{"empty filter metadata", &extProcPb.ProcessingRequest{MetadataContext: &configPb.Metadata{}}, ""},
		{"namespace absent", &extProcPb.ProcessingRequest{MetadataContext: otherNs}, ""},
		{"pool present", &extProcPb.ProcessingRequest{MetadataContext: mdFor(map[string]any{PoolMetadataKey: "pool-a"})}, "pool-a"},
		{"pool empty string", &extProcPb.ProcessingRequest{MetadataContext: mdFor(map[string]any{PoolMetadataKey: ""})}, ""},
		{"pool wrong type", &extProcPb.ProcessingRequest{MetadataContext: mdFor(map[string]any{PoolMetadataKey: 42})}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractPool(tt.req); got != tt.want {
				t.Fatalf("extractPool() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBufferedStreamReplay(t *testing.T) {
	first := headersRequest("pool-a")
	second := bodyRequest()
	inner := newFakeStream(second)

	bs := &bufferedStream{ExternalProcessor_ProcessServer: inner, first: first}

	got, err := bs.Recv()
	if err != nil || got != first {
		t.Fatalf("first Recv = %v, %v; want the buffered message", got, err)
	}
	got, err = bs.Recv()
	if err != nil || got != second {
		t.Fatalf("second Recv = %v, %v; want the underlying stream's message", got, err)
	}
	if _, err := bs.Recv(); err != io.EOF {
		t.Fatalf("third Recv err = %v, want io.EOF", err)
	}
	// The buffered first message must not have been consumed from the inner stream.
	if len(inner.msgs) != 0 {
		t.Fatalf("inner stream holds %d messages, want 0", len(inner.msgs))
	}
}

func TestProcessRecvError(t *testing.T) {
	s := NewServer(ManagerRouter{Manager: newTestManager(t, stubCompile("v", nil))}, "", logr.Discard())
	if err := s.Process(newFakeStream()); !errors.Is(err, io.EOF) {
		t.Fatalf("err=%v, want io.EOF", err)
	}
}

func TestProcessFirstMessageNotHeaders(t *testing.T) {
	s := NewServer(ManagerRouter{Manager: newTestManager(t, stubCompile("v", nil))}, "", logr.Discard())
	err := s.Process(newFakeStream(bodyRequest()))
	if codeOf(err) != codes.Internal {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
	if !strings.Contains(status.Convert(err).Message(), "not RequestHeaders") {
		t.Fatalf("unexpected message: %v", err)
	}
}

func TestProcessNoPoolNoDefault(t *testing.T) {
	m := newTestManager(t, stubCompile("v", nil))
	setupPool(t, m, "pool-a", cell.RolePrimary, `{"a":1}`)
	s := NewServer(ManagerRouter{Manager: m}, "", logr.Discard())

	err := s.Process(newFakeStream(headersRequest("")))
	if codeOf(err) != codes.Internal || status.Convert(err).Message() != ErrNoPoolMetadata.Error() {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
}

func TestProcessUnknownPool(t *testing.T) {
	m := newTestManager(t, stubCompile("v", nil))
	setupPool(t, m, "pool-a", cell.RolePrimary, `{"a":1}`)
	s := NewServer(ManagerRouter{Manager: m}, "", logr.Discard())

	// Named pool wins over the default: "ghost" must not fall back to pool-a.
	err := s.Process(newFakeStream(headersRequest("ghost")))
	if codeOf(err) != codes.Internal || status.Convert(err).Message() != ErrUnknownPool.Error() {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
}

func TestProcessCellNotReady(t *testing.T) {
	m := newTestManager(t, stubCompile("v", nil))
	setupPool(t, m, "pool-a", cell.RolePrimary, "") // no config applied: engine unset
	s := NewServer(ManagerRouter{Manager: m}, "pool-a", logr.Discard())

	err := s.Process(newFakeStream(headersRequest("pool-a")))
	if codeOf(err) != codes.Unavailable || status.Convert(err).Message() != ErrCellDraining.Error() {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
}

func TestProcessStandbyCell(t *testing.T) {
	m := newTestManager(t, stubCompile("v", nil))
	setupPool(t, m, "pool-a", cell.RoleStandby, `{"a":1}`)
	s := NewServer(ManagerRouter{Manager: m}, "pool-a", logr.Discard())

	err := s.Process(newFakeStream(headersRequest("pool-a")))
	if codeOf(err) != codes.Unavailable || status.Convert(err).Message() != ErrCellDraining.Error() {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
}

func TestProcessDelegatesToStreamer(t *testing.T) {
	streamer := handlers.NewStreamingServer(nil, nil, nil, 0)
	m := newTestManager(t, compileWithStreamer(streamer))
	setupPool(t, m, "pool-a", cell.RolePrimary, `{"a":1}`)
	s := NewServer(ManagerRouter{Manager: m}, "pool-b", logr.Discard())

	// Metadata routes to pool-b even though the default points elsewhere...
	setupPool(t, m, "pool-b", cell.RolePrimary, `{"b":1}`)
	if err := s.Process(newFakeStream(headersRequest("pool-b"))); err != nil {
		t.Fatalf("err=%v", err)
	}

	// ...and a missing metadata falls back to the default pool.
	if err := s.Process(newFakeStream(headersRequest(""))); err != nil {
		t.Fatalf("err=%v", err)
	}
}

func TestProcessPanicRecovery(t *testing.T) {
	// Engines from stubCompile have no streamer; invoking Process on them
	// panics, which the demux server must convert to an Internal error.
	m := newTestManager(t, stubCompile("v", nil))
	setupPool(t, m, "pool-a", cell.RolePrimary, `{"a":1}`)
	s := NewServer(ManagerRouter{Manager: m}, "pool-a", logr.Discard())

	err := s.Process(newFakeStream(headersRequest("pool-a")))
	if codeOf(err) != codes.Internal {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
	if !strings.HasPrefix(status.Convert(err).Message(), "internal error:") {
		t.Fatalf("unexpected message: %v", err)
	}
}

func TestProcessRecvPanicRecovered(t *testing.T) {
	m := newTestManager(t, stubCompile("v", nil))
	setupPool(t, m, "pool-a", cell.RolePrimary, `{"a":1}`)
	s := NewServer(ManagerRouter{Manager: m}, "pool-a", logr.Discard())

	stream := newFakeStream(headersRequest("pool-a"))
	stream.recvFn = func() (*extProcPb.ProcessingRequest, error) {
		panic("boom in Recv")
	}
	err := s.Process(stream)
	if codeOf(err) != codes.Internal {
		t.Fatalf("code=%v err=%v", codeOf(err), err)
	}
	if !strings.HasPrefix(status.Convert(err).Message(), "internal error:") {
		t.Fatalf("unexpected message: %v", err)
	}
}

// TestProcessConcurrentRace hammers Process with concurrent streams while the
// engine is hot-swapped underneath, exercising the Track fast path/failure
// path and the routing atomics. Run with -race.
func TestProcessConcurrentRace(t *testing.T) {
	streamer := handlers.NewStreamingServer(nil, nil, nil, 0)
	m := newTestManager(t, compileWithStreamer(streamer))
	setupPool(t, m, "pool-a", cell.RolePrimary, `{"a":0}`)
	s := NewServer(ManagerRouter{Manager: m}, "pool-a", logr.Discard())

	var workers sync.WaitGroup
	stop := make(chan struct{})

	// Config swapper: continuously hot-swaps the engine.
	var swapper sync.WaitGroup
	swapper.Add(1)
	go func() {
		defer swapper.Done()
		for i := 1; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := m.ApplyConfig(context.Background(), "pool-a", json.RawMessage(fmt.Sprintf(`{"a":%d}`, i))); err != nil {
				t.Errorf("ApplyConfig: %v", err)
				return
			}
		}
	}()

	// Stream processors: every outcome is acceptable (success or draining),
	// but there must be no race and no other error.
	for g := 0; g < 8; g++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 30; j++ {
				var stream *fakeProcessStream
				switch j % 3 {
				case 0:
					stream = newFakeStream(headersRequest("pool-a"))
				case 1:
					stream = newFakeStream(headersRequest("")) // default pool
				default:
					stream = newFakeStream(headersRequest("ghost")) // unknown pool
				}
				err := s.Process(stream)
				if err == nil {
					continue
				}
				if code := codeOf(err); code == codes.Unavailable || code == codes.Internal {
					continue
				}
				t.Errorf("unexpected error: %v", err)
				return
			}
		}()
	}

	workers.Wait()
	close(stop)
	swapper.Wait()

	if _, ok := m.Get("pool-a"); !ok {
		t.Fatal("pool-a cell vanished")
	}
}
