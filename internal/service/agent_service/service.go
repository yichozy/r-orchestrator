package agent_service

import (
	"errors"
	"sync"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var ErrAgentNotFound = errors.New("agent not found")
var ErrAgentIdentityConflict = errors.New("agent identity conflict")

const (
	AgentStatusIdle         = "IDLE"
	AgentStatusRunning      = "RUNNING"
	AgentStatusResultReady  = "RESULT_READY"
	AgentStatusDisconnected = "DISCONNECTED"
)

type Service struct {
	mu     sync.Mutex
	agents map[string]Agent // key: agent ID (pod name 或自定义字符串)
	logger *zap.Logger
}

type Agent struct {
	ID                  string // agent 标识（如 K8s pod name）
	TenantID            uuid.UUID
	BackendName         string
	Status              string
	PreDisconnectStatus string  // 断连前状态，仅 DISCONNECTED 时有值
	CurrentShardID      *string // 当前执行的 shard ID（proto 层为字符串）
	LastHeartbeatAt     *int64  // unix timestamp
}

type RegisterAgentParams struct {
	AgentID     string
	TenantID    uuid.UUID
	BackendName string
}

type HeartbeatAgentParams struct {
	AgentID        string
	Status         string
	CurrentShardID *string
}

func NewService() *Service {
	return &Service{
		agents: map[string]Agent{},
		logger: zap.L().Named("agent_service"),
	}
}

// GetActiveTenantIDs returns the set of tenant IDs that have at least one
// IDLE or RUNNING agent (excludes DISCONNECTED).
func (s *Service) GetActiveTenantIDs() map[uuid.UUID]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	tenants := make(map[uuid.UUID]bool)
	for _, a := range s.agents {
		if a.Status == AgentStatusIdle || a.Status == AgentStatusRunning || a.Status == AgentStatusResultReady {
			tenants[a.TenantID] = true
		}
	}
	return tenants
}

// DisconnectAgent marks the agent as DISCONNECTED, preserving its previous
// status in PreDisconnectStatus for recovery on reconnect.
func (s *Service) DisconnectAgent(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if agent, ok := s.agents[agentID]; ok {
		agent.PreDisconnectStatus = agent.Status
		if agent.PreDisconnectStatus == "" || agent.PreDisconnectStatus == AgentStatusDisconnected {
			agent.PreDisconnectStatus = AgentStatusIdle
		}
		agent.Status = AgentStatusDisconnected
		s.agents[agentID] = agent
	}
}

// RemoveAgent removes the agent from the in-memory map entirely.
func (s *Service) RemoveAgent(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.agents, agentID)
}
