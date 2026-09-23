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

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	llmdenvoy "github.com/llm-d/llm-d-router/pkg/common/envoy"
	reqcommon "github.com/llm-d/llm-d-router/pkg/common/request"
	ctrllog "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/rainway-ai-gateway/ai-gateway-epp/pkg/cell"
)

// Server is the multi-pool external processor. One Server serves every cell;
// each stream is routed by the pool metadata BFE attaches to the request.
type Server struct {
	extProcPb.UnimplementedExternalProcessorServer

	router      Router
	defaultPool string
	logger      logr.Logger
}

// NewServer creates the demux server. defaultPool is the fallback cluster
// when a request carries no pool metadata; empty means reject such requests.
func NewServer(router Router, defaultPool string, logger logr.Logger) *Server {
	return &Server{router: router, defaultPool: defaultPool, logger: logger.WithName("demux")}
}

// Process implements ExternalProcessorServer. Routing failures return gRPC
// errors so BFE can fall back; once routed, a per-request panic is converted
// to an error instead of killing the process.
func (s *Server) Process(stream extProcPb.ExternalProcessor_ProcessServer) (retErr error) {
	// The panic log needs the request-scoped logger, so keep it in a variable
	// the deferred closure reads at panic time.
	reqLogger := s.logger
	defer func() {
		if r := recover(); r != nil {
			reqLogger.Error(nil, "panic in request processing", "panic", r)
			retErr = status.Errorf(codes.Internal, "internal error: %v", r)
		}
	}()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	// Keep the oneof wrapper (not just the inner HttpHeaders) so we can reuse
	// llm-d's ExtractHeaderValue, which expects *ProcessingRequest_RequestHeaders.
	reqHeadersMsg, _ := first.Request.(*extProcPb.ProcessingRequest_RequestHeaders)
	if reqHeadersMsg == nil || reqHeadersMsg.RequestHeaders == nil {
		return status.Error(codes.Internal, "first ext-proc message is not RequestHeaders")
	}
	reqHeaders := reqHeadersMsg.RequestHeaders

	// x-request-id is the correlation key for the whole request: the llm-d engine
	// attaches the same value to every log line it emits (see
	// llm-d-router/pkg/epp/handlers/server.go). Read it here, generate one when the
	// gateway did not supply it, and write it back into the headers so the engine
	// below reads back the exact same value instead of generating its own.
	requestID := llmdenvoy.ExtractHeaderValue(reqHeadersMsg, reqcommon.RequestIDHeaderKey)
	generated := false
	if requestID == "" {
		requestID = uuid.NewString()
		if reqHeaders.Headers == nil {
			reqHeaders.Headers = &corev3.HeaderMap{}
		}
		reqHeaders.Headers.Headers = append(reqHeaders.Headers.Headers, &corev3.HeaderValue{
			Key:      reqcommon.RequestIDHeaderKey,
			RawValue: []byte(requestID),
		})
		generated = true
	}
	reqLogger = s.logger.WithValues(reqcommon.RequestIDHeaderKey, requestID)
	if generated {
		reqLogger.V(2).Info("request id not found in request, generated one")
	}

	pool := extractPool(first)
	if pool == "" {
		reqLogger.V(2).Info("no pool metadata, using default", "defaultPool", s.defaultPool)
		pool = s.defaultPool
		if pool == "" {
			demuxErrors.WithLabelValues("no-pool").Inc()
			return toStatus(ErrNoPoolMetadata)
		}
	}

	reqLogger.V(2).Info("routing request", "pool", pool)

	c, err := s.router.Route(pool)
	if err != nil {
		demuxErrors.WithLabelValues("unknown-pool").Inc()
		return toStatus(err)
	}

	eng := c.Engine()
	if eng == nil || c.Role() != cell.RolePrimary {
		demuxErrors.WithLabelValues("cell-draining").Inc()
		return toStatus(ErrCellDraining)
	}
	reqLogger.V(2).Info("cell routed", "pool", pool, "role", c.Role().String(), "engineVersion", eng.Version)

	done, ok := c.Track(eng)
	if !ok {
		// Engine swapped between load and track; the client may retry.
		demuxErrors.WithLabelValues("cell-draining").Inc()
		return toStatus(ErrCellDraining)
	}
	defer done()

	// Hand the request-scoped logger down to the engine so its logs come out with
	// the demux name and the pool. Only the pool is added here: llm-d appends the
	// request ID itself and logr accumulates values, so adding it here too would
	// print the field twice.
	ctx := ctrllog.IntoContext(stream.Context(), s.logger.WithValues("pool", pool))

	return eng.Streamer().Process(&bufferedStream{ExternalProcessor_ProcessServer: stream, first: first, ctx: ctx})
}

// extractPool reads the inference-pool value from the ext-proc metadata.
func extractPool(req *extProcPb.ProcessingRequest) string {
	md := llmdenvoy.ExtractMetadataValues(req)
	ns, ok := md[MetadataNamespace].(map[string]any)
	if !ok {
		return ""
	}
	v, ok := ns[PoolMetadataKey].(string)
	if !ok {
		return ""
	}
	return v
}

// bufferedStream replays the already-received first message, then delegates
// to the real stream, so the routed StreamingServer sees a complete stream.
// It also overrides Context so the request-scoped logger reaches the engine.
type bufferedStream struct {
	extProcPb.ExternalProcessor_ProcessServer
	first *extProcPb.ProcessingRequest
	ctx   context.Context
	used  bool
}

// Context returns the augmented context when set, so llm-d's log.FromContext
// picks up the demux request logger.
func (b *bufferedStream) Context() context.Context {
	if b.ctx != nil {
		return b.ctx
	}
	return b.ExternalProcessor_ProcessServer.Context()
}

func (b *bufferedStream) Recv() (*extProcPb.ProcessingRequest, error) {
	if !b.used {
		b.used = true
		return b.first, nil
	}
	return b.ExternalProcessor_ProcessServer.Recv()
}
