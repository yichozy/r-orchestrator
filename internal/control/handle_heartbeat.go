package control

import (
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleHeartbeat processes a heartbeat message from the agent.
func (server *Server) HandleHeartbeat(sess *agentSession, heartbeat *controlv1.Heartbeat) error {
	server.logger.Debug("received heartbeat from agent",
		zap.String("agent_id", sess.agentID),
		zap.String("status", heartbeat.GetStatus()),
	)
	if heartbeat.GetAgentId() != "" && heartbeat.GetAgentId() != sess.agentID {
		return status.Error(codes.InvalidArgument, "heartbeat agent_id does not match registered agent")
	}
	if heartbeat.GetStatus() == "IDLE" {
		n, err := task_service.RollbackOrphanShardsForAgent(sess.Context(), sess.agentID)
		if err != nil {
			return status.Errorf(codes.Internal, "rollback orphan shards for agent %s: %v", sess.agentID, err)
		}
		if n > 0 {
			server.logger.Warn("rolled back orphaned shards on agent IDLE report",
				zap.String("agent_id", sess.agentID),
				zap.Int("count", n),
			)
		}
	}

	var shardID *string
	if sid := heartbeat.GetCurrentShardId(); sid != "" {
		shardID = &sid
	}

	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        sess.agentID,
		Status:         heartbeat.GetStatus(),
		CurrentShardID: shardID,
	}); err != nil {
		if errors.Is(err, agent_service.ErrAgentNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		return status.Errorf(codes.Internal, "heartbeat agent: %v", err)
	}

	agent_service.ResetHeartbeat(sess.agentID)

	switch heartbeat.GetStatus() {
	case agent_service.AgentStatusResultReady:
		if shardID == nil || *shardID == "" {
			return status.Error(codes.InvalidArgument, "result-ready heartbeat requires current_shard_id")
		}
		parsedShardID, parseErr := uuid.Parse(*shardID)
		if parseErr != nil {
			return status.Errorf(codes.InvalidArgument, "invalid shard_id in result-ready heartbeat: %v", parseErr)
		}
		shard, err := task_service.LoadValidatedShard(sess.Context(), parsedShardID, sess.agentID, sess.tenantID, sess.backend)
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
			case model.ShardStatusSucceeded:
				// Result already stored, reset agent to idle and assign next.
				if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
					AgentID:        sess.agentID,
					Status:         agent_service.AgentStatusIdle,
					CurrentShardID: nil,
				}); err != nil {
					return status.Errorf(codes.Internal, "reset agent idle: %v", err)
				}
				return server.TryAssignShard(sess)
			default:
				// Agent still processing, wait for ShardResultReady message.
			}
		}

	case agent_service.AgentStatusIdle:
		if err := server.TryAssignShard(sess); err != nil {
			return err
		}
	}

	return nil
}
