package control

import (
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func (server *Server) tryAssignShard(
	stream *agentStream,
	agent_id string, tenant_id uuid.UUID, backend_name string,
) (ret_err error) {
	if server.db == nil || agent_id == "" || tenant_id == uuid.Nil || backend_name == "" {
		return nil
	}

	task, shard, err := task_service.LeaseNextShard(stream.Context(), tenant_id, backend_name, agent_id)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			server.logger.Debug("no queued shard available for assignment",
				zap.String("agent_id", agent_id),
				zap.Stringer("tenant_id", tenant_id),
				zap.String("backend_name", backend_name),
			)
			return nil
		}
		return status.Errorf(codes.Internal, "lease next shard: %v", err)
	}
	should_rollback := true
	defer func() {
		if !should_rollback {
			return
		}
		if rollback_err := server.rollbackAssignedShard(stream.Context(), task, shard, agent_id); rollback_err != nil {
			if ret_err == nil {
				ret_err = status.Errorf(codes.Internal, "rollback assigned shard: %v", rollback_err)
				return
			}
			ret_err = status.Errorf(codes.Internal, "%v (rollback failed: %v)", ret_err, rollback_err)
		}
	}()

	shardIDStr := shard.ID.String()
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agent_id,
		Status:         agent_service.AgentStatusRunning,
		CurrentShardID: &shardIDStr,
	}); err != nil {
		return status.Errorf(codes.Internal, "mark agent busy: %v", err)
	}

	if err := stream.Send(&controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_AssignShard{
			AssignShard: &controlv1.AssignShard{
				ShardId:            shard.ID.String(),
				TaskId:             task.ID.String(),
				BundleArtifactId:   task.BundleArtifactID.String(),
				InputCsvArtifactId: task.InputCSVArtifactID.String(),
				ShardIndex:         int32(shard.ShardIndex),
				TotalShards:        int32(task.ShardCount),
			},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "send assign shard: %v", err)
	}

	server.logger.Info("shard assigned",
		zap.String("agent_id", agent_id),
		zap.Stringer("task_id", task.ID),
		zap.Stringer("shard_id", shard.ID),
	)

	should_rollback = false
	return nil
}
