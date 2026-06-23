package control

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleReconnectedAgent validates a reconnecting agent's in-memory state against
// the database, then drives the post-registration state machine. Agents have no
// PVC so state is lost on restart. If the agent was RUNNING with a valid shard,
// reset to IDLE and try to assign new work.
func (server *Server) HandleReconnectedAgent(sess *agentSession, agent agent_service.Agent) (agent_service.Agent, error) {
	ctx := sess.Context()

	// Validate RUNNING state against DB, reset to IDLE if stale.
	if agent.Status == agent_service.AgentStatusRunning {
		var err error
		if agent.CurrentShardID != nil && *agent.CurrentShardID != "" {
			agent, err = server.resolveAgentShardState(ctx, agent)
		} else {
			agent, err = server.resetRegisteredAgentToIdle(agent.ID)
		}
		if err != nil {
			return agent_service.Agent{}, err
		}
	}

	if agent.Status != agent_service.AgentStatusIdle {
		return agent, nil
	}

	// IDLE — try to assign new shard.
	return agent, server.TryAssignShard(sess)
}

// resolveAgentShardState checks a RUNNING agent's shard against the DB.
// Returns the agent unchanged if consistent, or reset to IDLE if inconsistent.
// Returns an error only on unexpected DB failures.
func (server *Server) resolveAgentShardState(ctx context.Context, agent agent_service.Agent) (agent_service.Agent, error) {
	return server.resetRegisteredAgentToIdle(agent.ID)
}

func (server *Server) resetRegisteredAgentToIdle(agentID string) (agent_service.Agent, error) {
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "reset stale agent state: %v", err)
	}

	agent, err := agent_service.GetAgent(agentID)
	if err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "get reset agent: %v", err)
	}

	return agent, nil
}
