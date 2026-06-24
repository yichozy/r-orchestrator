package control

import (
	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Server) HandleShardFailed(sess *agentSession, shard_failed *controlv1.ShardFailed) error {
	shardID, err := uuid.Parse(shard_failed.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}

	shard, err := task_service.LoadValidatedShard(sess.Context(), shardID, sess.agentID, sess.tenantID, sess.backend)
	if err != nil {
		return err
	}

	// Normal path: shard is RUNNING, mark it FAILED.
	if shard.Status == model.ShardStatusRunning {
		errMsg := shard_failed.GetErrorMessage()
		if err := task_service.ReportShardStatus(sess.Context(), task_service.ReportShardStatusParams{
			ShardID:      shardID,
			ShardStatus:  model.ShardStatusFailed,
			ErrorMessage: &errMsg,
		}); err != nil {
			return status.Errorf(codes.Internal, "mark shard failed: %v", err)
		}
		server.logger.Warn("shard failed",
			zap.String("agent_id", sess.agentID),
			zap.Stringer("shard_id", shardID),
			zap.String("error", shard_failed.GetErrorMessage()),
		)
		return server.resetAgentAndAssign(sess)
	}

	// Shard is not RUNNING — already terminal, cancelled, or rolled back.
	// Only reset the agent if it's still on this shard; otherwise it has
	// already moved on and there's nothing to do.
	server.logger.Info("shard failed for non-running shard, skipping",
		zap.String("agent_id", sess.agentID),
		zap.Stringer("shard_id", shardID),
		zap.String("shard_status", shard.Status),
	)
	agent, agentErr := agent_service.GetAgent(sess.agentID)
	if agentErr != nil || agent.CurrentShardID == nil || *agent.CurrentShardID != shardID.String() {
		return nil
	}
	return server.resetAgentAndAssign(sess)
}
