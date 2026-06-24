package control

import (
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleReconnectedAgent drives the post-registration state machine for a
// reconnecting agent. Agents have no PVC so in-memory state is lost on restart.
// A RUNNING agent is reset to IDLE and its stale shards are rolled back so
// they can be re-queued immediately. If the agent had a pending result, it
// re-sends ShardResultReady which is handled gracefully even if the shard was
// already rolled back.
func (server *Server) HandleReconnectedAgent(sess *agentSession, agent agent_service.Agent) (agent_service.Agent, error) {
	if agent.Status == agent_service.AgentStatusRunning {
		if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
			AgentID:        agent.ID,
			Status:         agent_service.AgentStatusIdle,
			CurrentShardID: nil,
		}); err != nil {
			return agent_service.Agent{}, status.Errorf(codes.Internal, "reset stale agent state: %v", err)
		}

		// Roll back stale shards from before reconnect so they're re-queued
		// immediately rather than waiting for the next IDLE heartbeat.
		if n, rollbackErr := task_service.RollbackOrphanShardsForAgent(sess.Context(), agent.ID); rollbackErr != nil {
			server.logger.Warn("rollback stale shards on reconnect failed",
				zap.String("agent_id", agent.ID),
				zap.Error(rollbackErr),
			)
		} else if n > 0 {
			server.logger.Info("rolled back stale shards on reconnect",
				zap.String("agent_id", agent.ID),
				zap.Int("count", n),
			)
		}

		var err error
		agent, err = agent_service.GetAgent(agent.ID)
		if err != nil {
			return agent_service.Agent{}, status.Errorf(codes.Internal, "get reset agent: %v", err)
		}
	}

	if agent.Status != agent_service.AgentStatusIdle {
		return agent, nil
	}

	// IDLE — try to assign new shard.
	return agent, server.TryAssignShard(sess)
}
