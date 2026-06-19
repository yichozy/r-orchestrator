package control

import (
	"context"
	"errors"

	"github.com/google/uuid"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
)

func (server *Server) NotifyCancelShard(ctx context.Context, agentID string, shardID uuid.UUID) error {
	value, ok := server.streams.Load(agentID)
	if !ok {
		return nil // agent not connected, skip
	}

	stream, ok := value.(*agentStream)
	if !ok || stream == nil {
		server.streams.Delete(agentID)
		return nil
	}
	if err := stream.SendWithContext(ctx, &controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_CancelShard{
			CancelShard: &controlv1.CancelShard{
				ShardId: shardID.String(),
			},
		},
	}); err != nil {
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			server.streams.Delete(agentID)
		}
		server.logger.Error("send cancel shard to agent failed",
			zap.String("agent_id", agentID),
			zap.Stringer("shard_id", shardID),
			zap.Error(err),
		)
		return err
	}

	server.logger.Info("cancel shard sent to agent",
		zap.String("agent_id", agentID),
		zap.Stringer("shard_id", shardID),
	)
	return nil
}
