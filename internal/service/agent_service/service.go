package agent_service

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/config"
	"go.uber.org/zap"
)

var ErrAgentNotFound = errors.New("agent not found")
var ErrAgentIdentityConflict = errors.New("agent identity conflict")

const (
	AgentStatusIdle         = "IDLE"
	AgentStatusRunning      = "RUNNING"
	AgentStatusResultReady  = "RESULT_READY"
	AgentStatusDisconnected = "DISCONNECTED"
	AgentStatusUnresponsive = "UNRESPONSIVE"
	AgentStatusTimedOut     = "TIMED_OUT"
)

// TimeoutCallback is invoked by agent_service when an agent heartbeat or grace
// timer fires. The callback runs on the timer goroutine and must NOT call back
// into agent_service while holding external locks.
type TimeoutCallback func(agentID string, reason string)

type Service struct {
	mu     sync.Mutex
	agents map[string]Agent // key: agent ID (pod name or custom string)
	logger *zap.Logger

	heartbeatTimeout time.Duration
	gracePeriod      time.Duration
	onTimeout        TimeoutCallback

	timers  map[string]*time.Timer
	timerMu sync.Mutex // protects timers map; never hold simultaneously with mu
}

type Agent struct {
	ID                  string
	TenantID            uuid.UUID
	BackendName         string
	Status              string
	PreDisconnectStatus string  // status before disconnect, only set when DISCONNECTED
	CurrentShardID      *string // shard ID the agent is currently executing
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

// NewService creates an agent service with per-agent heartbeat and grace timers.
// Timeout parameters are read from config.GlobalConfig.
func NewService() *Service {
	return &Service{
		agents:           map[string]Agent{},
		timers:           map[string]*time.Timer{},
		logger:           zap.L().Named("agent_service"),
		heartbeatTimeout: config.GlobalConfig.Cluster.AgentHeartbeatTimeout,
		gracePeriod:      config.GlobalConfig.Cluster.AgentDisconnectGrace,
	}
}

// GetActiveTenantIDs returns the set of tenant IDs that have at least one
// active agent (IDLE, RUNNING, RESULT_READY, or UNRESPONSIVE).
func (s *Service) GetActiveTenantIDs() map[uuid.UUID]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	tenants := make(map[uuid.UUID]bool)
	for _, a := range s.agents {
		if a.Status == AgentStatusIdle || a.Status == AgentStatusRunning ||
			a.Status == AgentStatusResultReady || a.Status == AgentStatusUnresponsive {
			tenants[a.TenantID] = true
		}
	}
	return tenants
}

// GetAllAgentIDs returns all registered agent IDs.
func (s *Service) GetAllAgentIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.agents))
	for id := range s.agents {
		ids = append(ids, id)
	}
	return ids
}

// DisconnectAgent marks the agent as DISCONNECTED, preserving its previous
// status in PreDisconnectStatus for recovery on reconnect.
func (s *Service) DisconnectAgent(agentID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if agent, ok := s.agents[agentID]; ok {
		agent.PreDisconnectStatus = agent.Status
		if agent.PreDisconnectStatus == "" || agent.PreDisconnectStatus == AgentStatusDisconnected ||
			agent.PreDisconnectStatus == AgentStatusIdle || agent.PreDisconnectStatus == AgentStatusUnresponsive ||
			agent.PreDisconnectStatus == AgentStatusTimedOut {
			agent.PreDisconnectStatus = AgentStatusIdle
		}
		agent.Status = AgentStatusDisconnected
		s.agents[agentID] = agent
	}
}

// RemoveAgent removes the agent from the in-memory map and stops its timer.
func (s *Service) RemoveAgent(agentID string) {
	s.StopTimer(agentID)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.agents, agentID)
}

// SetTimeoutCallback sets the timeout callback.
func (s *Service) SetTimeoutCallback(cb TimeoutCallback) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onTimeout = cb
}

// SetTimeouts overrides heartbeat and grace timeouts. For testing only.
func (s *Service) SetTimeouts(heartbeatTimeout, gracePeriod time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.heartbeatTimeout = heartbeatTimeout
	s.gracePeriod = gracePeriod
}
