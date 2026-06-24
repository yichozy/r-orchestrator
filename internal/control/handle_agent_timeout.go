package control

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleAgentTimeout is the callback invoked by agent_service when a heartbeat
// or grace timer fires. It rolls back the agent's shard in the DB.
func (server *Server) HandleAgentTimeout(agentID string, reason string) {
	agent, err := agent_service.GetAgent(agentID)
	if err != nil {
		return // agent already removed
	}

	ctx := context.Background()
	if agent.CurrentShardID != nil && *agent.CurrentShardID != "" {
		shardID, parseErr := uuid.Parse(*agent.CurrentShardID)
		if parseErr == nil {
			if err := task_service.RollbackTimedOutShard(ctx, shardID, agentID, agent.TenantID, agent.BackendName); err != nil {
				code := status.Code(err)
				if code != codes.NotFound && code != codes.PermissionDenied {
					server.logger.Error("rollback timed-out shard failed",
						zap.String("agent_id", agentID),
						zap.Stringer("shard_id", shardID),
						zap.Error(err))
				}
			} else {
				server.logger.Warn("rolled back timed-out shard",
					zap.String("agent_id", agentID),
					zap.Stringer("shard_id", shardID),
					zap.String("reason", reason),
				)
			}
		}
	}

	// For grace_expired, the agent is still DISCONNECTED. Mark as TIMED_OUT
	// so it's excluded from active tenant counts.
	if reason == "grace_expired" {
		if err := agent_service.HeartbeatAgent(agentID, agent_service.AgentStatusTimedOut, nil); err != nil {
			server.logger.Warn("mark grace-expired agent as timed out",
				zap.String("agent_id", agentID),
				zap.Error(err),
			)
		}
	}
}
