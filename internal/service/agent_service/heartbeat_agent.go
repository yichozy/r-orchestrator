package agent_service

import (
	"fmt"
	"time"
)

func HeartbeatAgent(agentID string, status string, currentShardID *string) error {
	mu.Lock()
	defer mu.Unlock()

	agent, ok := agents[agentID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrAgentNotFound, agentID)
	}

	now := time.Now().Unix()
	agent.Status = status
	agent.CurrentShardID = currentShardID
	agent.LastHeartbeatAt = &now
	agents[agentID] = agent

	return nil
}
