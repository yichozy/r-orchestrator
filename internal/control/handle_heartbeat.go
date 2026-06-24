package control

import (
	"errors"

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

	if err := agent_service.HeartbeatAgent(sess.agentID, heartbeat.GetStatus(), shardID); err != nil {
		if errors.Is(err, agent_service.ErrAgentNotFound) {
			return status.Error(codes.NotFound, err.Error())
		}
		return status.Errorf(codes.Internal, "heartbeat agent: %v", err)
	}

	agent_service.ResetHeartbeat(sess.agentID)

	if heartbeat.GetStatus() == agent_service.AgentStatusIdle {
		if err := server.TryAssignShard(sess); err != nil {
			return err
		}
	}

	return nil
}
