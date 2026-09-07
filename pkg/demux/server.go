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
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	llmdenvoy "github.com/llm-d/llm-d-router/pkg/common/envoy"

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
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error(nil, "panic in request processing", "panic", r)
			retErr = status.Errorf(codes.Internal, "internal error: %v", r)
		}
	}()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.GetRequestHeaders() == nil {
		return status.Error(codes.Internal, "first ext-proc message is not RequestHeaders")
	}

	pool := extractPool(first)
	if pool == "" {
		pool = s.defaultPool
		if pool == "" {
			demuxErrors.WithLabelValues("no-pool").Inc()
			return toStatus(ErrNoPoolMetadata)
		}
	}

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
	done, ok := c.Track(eng)
	if !ok {
		// Engine swapped between load and track; the client may retry.
		demuxErrors.WithLabelValues("cell-draining").Inc()
		return toStatus(ErrCellDraining)
	}
	defer done()

	return eng.Streamer().Process(&bufferedStream{ExternalProcessor_ProcessServer: stream, first: first})
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
type bufferedStream struct {
	extProcPb.ExternalProcessor_ProcessServer
	first *extProcPb.ProcessingRequest
	used  bool
}

func (b *bufferedStream) Recv() (*extProcPb.ProcessingRequest, error) {
	if !b.used {
		b.used = true
		return b.first, nil
	}
	return b.ExternalProcessor_ProcessServer.Recv()
}
