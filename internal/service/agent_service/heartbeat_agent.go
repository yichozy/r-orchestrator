package agent_service

import (
	"fmt"
	"time"
)

func (service *Service) HeartbeatAgent(params HeartbeatAgentParams) error {
	service.mu.Lock()
	defer service.mu.Unlock()

	agent, ok := service.agents[params.AgentID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAgentNotFound, params.AgentID)
	}

	now := time.Now().Unix()
	agent.Status = params.Status
	agent.CurrentShardID = params.CurrentShardID
	agent.LastHeartbeatAt = &now
	service.agents[params.AgentID] = agent

	return nil
}
