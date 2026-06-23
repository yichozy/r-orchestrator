package control

import (
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleReconnectedAgent drives the post-registration state machine for a
// reconnecting agent. Agents have no PVC so in-memory state is lost on restart.
// A RUNNING agent is always reset to IDLE — any in-flight shard will be rolled
// back by orphan cleanup or re-sent as ShardResultReady by the agent.
func (server *Server) HandleReconnectedAgent(sess *agentSession, agent agent_service.Agent) (agent_service.Agent, error) {
	if agent.Status == agent_service.AgentStatusRunning {
		var err error
		agent, err = server.resetRegisteredAgentToIdle(agent.ID)
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
