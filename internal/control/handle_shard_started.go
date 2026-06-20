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

func (server *Server) HandleShardStarted(
	streamRef *agentStream,
	stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage],
	shard_started *controlv1.ShardStarted,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) error {
	shardID, err := uuid.Parse(shard_started.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}
	shardIDStr := shardID.String()
	if err := server.validateShardReport(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name, model.ShardStatusLeased, "mark shard started"); err != nil {
		return err
	}
	if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{ShardID: shardID, ShardStatus: model.ShardStatusRunning}); err != nil {
		return status.Errorf(codes.Internal, "mark shard started: %v", err)
	}
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        current_agent_id,
		Status:         model.ShardStatusRunning,
		CurrentShardID: &shardIDStr,
	}); err != nil {
		return status.Errorf(codes.Internal, "update agent running state: %v", err)
	}
	server.logger.Info("shard started",
		zap.String("agent_id", current_agent_id),
		zap.Stringer("shard_id", shardID),
	)
	return nil
}
