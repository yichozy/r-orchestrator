package agent_service

import (
	"fmt"
)

func GetAgent(agent_id string) (Agent, error) {
	mu.Lock()
	defer mu.Unlock()

	agent, ok := agents[agent_id]
	if !ok {
		return Agent{}, fmt.Errorf("%w: %s", ErrAgentNotFound, agent_id)
	}

	return agent, nil
}
