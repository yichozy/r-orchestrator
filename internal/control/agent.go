package control

import (
	"context"
	"sync"

	"github.com/google/uuid"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	grpc "google.golang.org/grpc"
)

// agentStream wraps a gRPC bidi stream with a mutex for concurrent Send calls.
type agentStream struct {
	stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage]
	sendMu sync.Mutex
}

func (stream *agentStream) Send(message *controlv1.ServerMessage) error {
	stream.sendMu.Lock()
	defer stream.sendMu.Unlock()
	return stream.stream.Send(message)
}

func (stream *agentStream) Context() context.Context {
	return stream.stream.Context()
}

func (stream *agentStream) SendWithContext(ctx context.Context, message *controlv1.ServerMessage) error {
	result := make(chan error, 1)
	go func() {
		result <- stream.Send(message)
	}()

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// agentSession groups the per-connection state that every control-stream handler needs.
// Replaces the former 5-tuple (streamRef, stream, agentID, tenantID, backend).
type agentSession struct {
	stream   *agentStream
	server   *Server
	agentID  string
	tenantID uuid.UUID
	backend  string
}

// Context returns the stream context.
func (s *agentSession) Context() context.Context { return s.stream.Context() }

// Send sends a server message on the control stream.
func (s *agentSession) Send(msg *controlv1.ServerMessage) error { return s.stream.Send(msg) }
