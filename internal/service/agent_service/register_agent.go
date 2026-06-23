package agent_service

import (
	"fmt"
	"time"
)

func RegisterAgent(params RegisterAgentParams) error {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now().Unix()
	registered_agent := Agent{
		ID:              params.AgentID,
		TenantID:        params.TenantID,
		BackendName:     params.BackendName,
		Status:          AgentStatusIdle,
		LastHeartbeatAt: &now,
	}
	if existing_agent, ok := agents[params.AgentID]; ok {
		if existing_agent.TenantID != params.TenantID || existing_agent.BackendName != params.BackendName {
			return fmt.Errorf(
				"%w: agent=%s existing=%s/%s requested=%s/%s",
				ErrAgentIdentityConflict,
				params.AgentID,
				existing_agent.TenantID,
				existing_agent.BackendName,
				params.TenantID,
				params.BackendName,
			)
		}

		switch existing_agent.Status {
		case AgentStatusDisconnected, AgentStatusTimedOut, AgentStatusUnresponsive:
			recoveryStatus := existing_agent.PreDisconnectStatus
			if recoveryStatus == "" || recoveryStatus == AgentStatusDisconnected ||
				recoveryStatus == AgentStatusIdle || recoveryStatus == AgentStatusTimedOut ||
				recoveryStatus == AgentStatusUnresponsive {
				recoveryStatus = AgentStatusIdle
			}
			registered_agent.Status = recoveryStatus
			registered_agent.CurrentShardID = existing_agent.CurrentShardID
		case AgentStatusRunning:
			registered_agent.Status = existing_agent.Status
			registered_agent.CurrentShardID = existing_agent.CurrentShardID
			return fmt.Errorf("%w: %s already has an active control stream", ErrAgentIdentityConflict, params.AgentID)
		case AgentStatusIdle:
			return fmt.Errorf("%w: %s is already registered", ErrAgentIdentityConflict, params.AgentID)
		default:
		}
	}

	agents[params.AgentID] = registered_agent

	return nil
}
