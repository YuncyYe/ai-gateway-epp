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
	"context"
	"crypto/x509"
	"fmt"
	"io"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/llm-d/llm-d-router/pkg/epp/metadata"
)

// PickEndpoint plays the BFE ext-proc client: it opens a stream, sends
// RequestHeaders (with the inference-pool metadata) and a RequestBody, then
// reads responses until the scheduler's destination endpoint appears in the
// dynamic metadata. It returns the endpoint as "ip:port".
func PickEndpoint(grpcAddr, pool, path string, body []byte, timeout time.Duration) (string, error) {
	return PickEndpointWithHeaders(grpcAddr, pool, path, body, timeout, nil)
}

// PickEndpointWithHeaders is PickEndpoint with additional request headers
// (e.g. a session id header for session-affinity scenarios).
func PickEndpointWithHeaders(grpcAddr, pool, path string, body []byte, timeout time.Duration, headers map[string]string) (string, error) {
	return PickEndpointWithHeadersCreds(grpcAddr, pool, path, body, timeout, headers, insecure.NewCredentials())
}

// PickEndpointTLS is PickEndpoint over TLS, verifying the server against pool.
func PickEndpointTLS(grpcAddr, pool, path string, body []byte, timeout time.Duration, poolCA *x509.CertPool) (string, error) {
	return PickEndpointWithHeadersCreds(grpcAddr, pool, path, body, timeout, nil, credentials.NewClientTLSFromCert(poolCA, ""))
}

// PickEndpointWithHeadersCreds is PickEndpoint with custom transport
// credentials (plaintext or TLS).
func PickEndpointWithHeadersCreds(grpcAddr, pool, path string, body []byte, timeout time.Duration, headers map[string]string, creds credentials.TransportCredentials) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, grpcAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return "", err
	}
	defer conn.Close()

	stream, err := extProcPb.NewExternalProcessorClient(conn).Process(ctx)
	if err != nil {
		return "", err
	}

	md, err := structpb.NewStruct(map[string]any{"inference-pool": pool})
	if err != nil {
		return "", err
	}

	hdr := []*core.HeaderValue{
		{Key: ":path", Value: path},
		{Key: ":method", Value: "POST"},
		{Key: "content-type", Value: "application/json"},
	}
	for k, v := range headers {
		hdr = append(hdr, &core.HeaderValue{Key: k, Value: v})
	}

	if err := stream.Send(&extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestHeaders{
			RequestHeaders: &extProcPb.HttpHeaders{
				Headers: &core.HeaderMap{Headers: hdr},
			},
		},
		MetadataContext: &core.Metadata{
			FilterMetadata: map[string]*structpb.Struct{"llm-d.ai": md},
		},
	}); err != nil {
		return "", err
	}

	if err := stream.Send(&extProcPb.ProcessingRequest{
		Request: &extProcPb.ProcessingRequest_RequestBody{
			RequestBody: &extProcPb.HttpBody{Body: body, EndOfStream: true},
		},
	}); err != nil {
		return "", err
	}
	if err := stream.CloseSend(); err != nil {
		return "", err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return "", fmt.Errorf("stream closed without a destination endpoint")
		}
		if err != nil {
			return "", fmt.Errorf("ext-proc error: %w", err)
		}
		if ep := destinationFromResponse(resp); ep != "" {
			return ep, nil
		}
	}
}

// destinationFromResponse extracts x-gateway-destination-endpoint from a
// response's dynamic metadata (namespace envoy.lb).
func destinationFromResponse(resp *extProcPb.ProcessingResponse) string {
	md := resp.GetDynamicMetadata()
	if md == nil {
		return ""
	}
	ns, ok := md.Fields[metadata.DestinationEndpointNamespace]
	if !ok || ns.GetStructValue() == nil {
		return ""
	}
	v, ok := ns.GetStructValue().Fields[metadata.DestinationEndpointKey]
	if !ok {
		return ""
	}
	return v.GetStringValue()
}
