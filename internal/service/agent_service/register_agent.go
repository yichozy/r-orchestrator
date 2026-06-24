package agent_service

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

func RegisterAgent(agentID string, tenantID uuid.UUID, backendName string) error {
	mu.Lock()
	defer mu.Unlock()

	now := time.Now().Unix()
	registered_agent := Agent{
		ID:              agentID,
		TenantID:        tenantID,
		BackendName:     backendName,
		Status:          AgentStatusIdle,
		LastHeartbeatAt: &now,
	}
	if existing_agent, ok := agents[agentID]; ok {
		if existing_agent.TenantID != tenantID || existing_agent.BackendName != backendName {
			return fmt.Errorf(
				"%w: agent=%s existing=%s/%s requested=%s/%s",
				ErrAgentIdentityConflict,
				agentID,
				existing_agent.TenantID,
				existing_agent.BackendName,
				tenantID,
				backendName,
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
			return fmt.Errorf("%w: %s already has an active control stream", ErrAgentIdentityConflict, agentID)
		case AgentStatusIdle:
			return fmt.Errorf("%w: %s is already registered", ErrAgentIdentityConflict, agentID)
		default:
		}
	}

	agents[agentID] = registered_agent

	return nil
}
