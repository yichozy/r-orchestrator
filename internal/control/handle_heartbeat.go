package control

import (
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleHeartbeat processes a heartbeat message from the agent.
func (server *Server) HandleHeartbeat(
	streamRef *agentStream,
	stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage],
	heartbeat *controlv1.Heartbeat,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) error {
	server.logger.Debug("received heartbeat from agent",
		zap.String("agent_id", current_agent_id),
		zap.String("status", heartbeat.GetStatus()),
	)
	if heartbeat.GetAgentId() != "" && heartbeat.GetAgentId() != current_agent_id {
		return status.Error(codes.InvalidArgument, "heartbeat agent_id does not match registered agent")
	}
	if heartbeat.GetStatus() == "IDLE" {
		if server.db != nil {
			activeShards, err := task_shard_orm.GetActiveTaskShardByAgent(stream.Context(), server.db, current_agent_id)
			if err != nil {
				return status.Errorf(codes.Internal, "load active shards for agent %s: %v", current_agent_id, err)
			}
			for _, shard := range activeShards {
				server.logger.Warn("rolling back orphaned shard on agent IDLE report",
					zap.String("agent_id", current_agent_id),
					zap.Stringer("shard_id", shard.ID),
					zap.String("shard_status", shard.Status),
				)
				if rollbackErr := task_shard_orm.UpdateShardStatus(stream.Context(), server.db, task_shard_orm.UpdateShardStatusParams{
					ShardID:         shard.ID,
					Status:          model.ShardStatusQueued,
					CurrentStatuses: []string{shard.Status},
					ClearAgent:      true,
				}); rollbackErr != nil {
					server.logger.Error("failed to roll back orphaned shard",
						zap.Stringer("shard_id", shard.ID),
						zap.Error(rollbackErr),
					)
				}
			}
		}
	}

	var shardID *string
	if sid := heartbeat.GetCurrentShardId(); sid != "" {
		shardID = &sid
	}

	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        current_agent_id,
		Status:         heartbeat.GetStatus(),
		CurrentShardID: shardID,
	}); err != nil {
		if errors.Is(err, agent_service.ErrAgentNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		return status.Errorf(codes.Internal, "heartbeat agent: %v", err)
	}

	server.agentService.ResetHeartbeatTimer(current_agent_id)

	switch heartbeat.GetStatus() {
	case agent_service.AgentStatusResultReady:
		if shardID == nil || *shardID == "" {
			return status.Error(codes.InvalidArgument, "result-ready heartbeat requires current_shard_id")
		}
		parsedShardID, parseErr := uuid.Parse(*shardID)
		if parseErr != nil {
			return status.Errorf(codes.InvalidArgument, "invalid shard_id in result-ready heartbeat: %v", parseErr)
		}
		if server.db != nil {
			shard, err := server.loadValidatedShard(streamRef.Context(), parsedShardID, current_agent_id, current_tenant_id, current_backend_name)
			if err != nil {
				code := status.Code(err)
				if code == codes.NotFound || code == codes.PermissionDenied {
					server.logger.Warn("result-ready heartbeat references invalid shard",
						zap.Stringer("shard_id", parsedShardID),
						zap.String("code", code.String()),
						zap.Error(err),
					)
				} else {
					return err
				}
			} else {
				switch shard.Status {
				case model.ShardStatusResultReady:
					if err := streamRef.Send(&controlv1.ServerMessage{Payload: &controlv1.ServerMessage_FetchShardResult{FetchShardResult: &controlv1.FetchShardResult{ShardId: *shardID}}}); err != nil {
						return err
					}
				case model.ShardStatusSucceeded:
					if err := server.completeCurrentWorkAndReassign(streamRef, current_agent_id, current_tenant_id, current_backend_name, *shardID); err != nil {
						return err
					}
				}
			}
		}

	case agent_service.AgentStatusIdle:
		if err := server.tryAssignShard(streamRef, current_agent_id, current_tenant_id, current_backend_name); err != nil {
			return err
		}
	}

	return nil
}
