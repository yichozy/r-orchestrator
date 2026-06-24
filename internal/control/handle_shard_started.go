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

func (server *Server) HandleShardStarted(sess *agentSession, shard_started *controlv1.ShardStarted) error {
	shardID, err := uuid.Parse(shard_started.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}
	shardIDStr := shardID.String()
	if err := task_service.ValidateShardReport(sess.Context(), shardID, sess.agentID, sess.tenantID, sess.backend, model.ShardStatusLeased, "mark shard started"); err != nil {
		return err
	}
	if err := task_service.ReportShardStatus(sess.Context(), task_service.ReportShardStatusParams{ShardID: shardID, ShardStatus: model.ShardStatusRunning}); err != nil {
		return status.Errorf(codes.Internal, "mark shard started: %v", err)
	}
	if err := agent_service.HeartbeatAgent(sess.agentID, agent_service.AgentStatusRunning, &shardIDStr); err != nil {
		return status.Errorf(codes.Internal, "update agent running state: %v", err)
	}
	server.logger.Info("shard started",
		zap.String("agent_id", sess.agentID),
		zap.Stringer("shard_id", shardID),
	)
	return nil
}
