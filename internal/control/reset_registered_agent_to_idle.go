package control

import (
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Server) ResetRegisteredAgentToIdle(agentID string) (agent_service.Agent, error) {
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "reset stale agent state: %v", err)
	}

	agent, err := server.agentService.GetAgent(agentID)
	if err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "get reset agent: %v", err)
	}

	return agent, nil
}
