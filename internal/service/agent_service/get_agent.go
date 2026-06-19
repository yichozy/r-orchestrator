package agent_service

import (
	"fmt"
)

func (service *Service) GetAgent(agent_id string) (Agent, error) {
	service.mu.Lock()
	defer service.mu.Unlock()

	agent, ok := service.agents[agent_id]
	if !ok {
		return Agent{}, fmt.Errorf("%w: %s", ErrAgentNotFound, agent_id)
	}

	return agent, nil
}
