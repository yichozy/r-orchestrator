package agent_service

import (
	"fmt"
	"time"
)

func HeartbeatAgent(params HeartbeatAgentParams) error {
	mu.Lock()
	defer mu.Unlock()

	agent, ok := agents[params.AgentID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAgentNotFound, params.AgentID)
	}

	now := time.Now().Unix()
	agent.Status = params.Status
	agent.CurrentShardID = params.CurrentShardID
	agent.LastHeartbeatAt = &now
	agents[params.AgentID] = agent

	return nil
}
