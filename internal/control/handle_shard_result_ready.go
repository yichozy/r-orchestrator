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

func (server *Server) HandleShardResultReady(sess *agentSession, shard_ready *controlv1.ShardResultReady) error {
	shardID, err := uuid.Parse(shard_ready.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}
	shardIDStr := shardID.String()
	if err := task_service.ValidateShardReport(sess.Context(), shardID, sess.agentID, sess.tenantID, sess.backend, model.ShardStatusRunning, "mark shard result ready"); err != nil {
		return err
	}
	if err := task_service.ReportShardStatus(sess.Context(), task_service.ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusResultReady,
	}); err != nil {
		return status.Errorf(codes.Internal, "mark shard result ready: %v", err)
	}
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        sess.agentID,
		Status:         agent_service.AgentStatusResultReady,
		CurrentShardID: &shardIDStr,
	}); err != nil {
		return status.Errorf(codes.Internal, "update agent result-ready state: %v", err)
	}
	server.logger.Info("shard result ready",
		zap.String("agent_id", sess.agentID),
		zap.Stringer("shard_id", shardID),
	)
	if err := sess.Send(&controlv1.ServerMessage{Payload: &controlv1.ServerMessage_FetchShardResult{FetchShardResult: &controlv1.FetchShardResult{ShardId: shardID.String()}}}); err != nil {
		return err
	}
	return nil
}
