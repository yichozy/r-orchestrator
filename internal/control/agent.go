package control

import (
	"context"
	"sync"

	controlv1 "github.com/yichozy/r-orchestrator/proto"
	grpc "google.golang.org/grpc"
)

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
