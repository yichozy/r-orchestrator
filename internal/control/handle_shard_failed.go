package control

import (
	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Server) HandleShardFailed(
	streamRef *agentStream,
	stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage],
	shard_failed *controlv1.ShardFailed,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) error {
	shardID, err := uuid.Parse(shard_failed.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}
	if err := server.validateShardReport(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name, model.ShardStatusRunning, "mark shard failed"); err != nil {
		return err
	}
	errMsg := shard_failed.GetErrorMessage()
	if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{
		ShardID:      shardID,
		ShardStatus:  model.ShardStatusFailed,
		ErrorMessage: &errMsg,
	}); err != nil {
		return status.Errorf(codes.Internal, "mark shard failed: %v", err)
	}
	server.logger.Warn("shard failed",
		zap.String("agent_id", current_agent_id),
		zap.Stringer("shard_id", shardID),
		zap.String("error", shard_failed.GetErrorMessage()),
	)
	// ShardFailed doesn't send ShardResultStored, just reset agent and reassign.
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        current_agent_id,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return status.Errorf(codes.Internal, "update agent idle state: %v", err)
	}
	if err := server.tryAssignShard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
		return err
	}
	return nil
}
