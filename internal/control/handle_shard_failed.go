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
	if err := task_service.ValidateShardReport(sess.Context(), shardID, sess.agentID, sess.tenantID, sess.backend, model.ShardStatusRunning, "mark shard failed"); err != nil {
		return err
	}
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
	// ShardFailed doesn't send ShardResultStored, just reset agent and reassign.
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        sess.agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return status.Errorf(codes.Internal, "update agent idle state: %v", err)
	}
	if err := server.TryAssignShard(sess); err != nil {
		return err
	}
	return nil
}
