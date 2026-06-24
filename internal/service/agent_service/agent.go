package agent_service

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

var ErrAgentNotFound = errors.New("agent not found")
var ErrAgentIdentityConflict = errors.New("agent identity conflict")

const (
	AgentStatusIdle         = "IDLE"
	AgentStatusRunning      = "RUNNING"
	AgentStatusDisconnected = "DISCONNECTED"
	AgentStatusUnresponsive = "UNRESPONSIVE"
	AgentStatusTimedOut     = "TIMED_OUT"
)

type TimeoutCallback func(agentID string, reason string)

type Agent struct {
	ID                  string
	TenantID            uuid.UUID
	BackendName         string
	Status              string
	PreDisconnectStatus string
	CurrentShardID      *string
	LastHeartbeatAt     *int64
}

var (
	mu     sync.Mutex
	agents = make(map[string]Agent)

	timerMu          sync.Mutex
	timers           = make(map[string]*time.Timer)
	heartbeatTimeout = 30 * time.Second
	gracePeriod      = 5 * time.Minute
	onTimeout        TimeoutCallback
)

// SetTimeoutCallback sets the timeout callback.
func SetTimeoutCallback(cb TimeoutCallback) {
	mu.Lock()
	defer mu.Unlock()
	onTimeout = cb
}

// SetTimeouts overrides heartbeat and grace timeouts.
func SetTimeouts(hb, gp time.Duration) {
	mu.Lock()
	defer mu.Unlock()
	heartbeatTimeout = hb
	gracePeriod = gp
}

// GetActiveTenantIDs returns the set of tenant IDs that have at least one
// active agent (IDLE, RUNNING, or UNRESPONSIVE).
func GetActiveTenantIDs() map[uuid.UUID]bool {
	mu.Lock()
	defer mu.Unlock()
	tenants := make(map[uuid.UUID]bool)
	for _, a := range agents {
		if a.Status == AgentStatusIdle || a.Status == AgentStatusRunning ||
			a.Status == AgentStatusUnresponsive {
			tenants[a.TenantID] = true
		}
	}
	return tenants
}

// GetAllAgentIDs returns all registered agent IDs.
func GetAllAgentIDs() []string {
	mu.Lock()
	defer mu.Unlock()
	ids := make([]string, 0, len(agents))
	for id := range agents {
		ids = append(ids, id)
	}
	return ids
}

// DisconnectAgent marks the agent as DISCONNECTED, preserving its previous
// status in PreDisconnectStatus for recovery on reconnect.
func DisconnectAgent(agentID string) {
	mu.Lock()
	defer mu.Unlock()
	if agent, ok := agents[agentID]; ok {
		agent.PreDisconnectStatus = agent.Status
		if agent.PreDisconnectStatus == "" || agent.PreDisconnectStatus == AgentStatusDisconnected ||
			agent.PreDisconnectStatus == AgentStatusIdle || agent.PreDisconnectStatus == AgentStatusUnresponsive ||
			agent.PreDisconnectStatus == AgentStatusTimedOut {
			agent.PreDisconnectStatus = AgentStatusIdle
		}
		agent.Status = AgentStatusDisconnected
		agents[agentID] = agent
	}
}

// RemoveAgent removes the agent from the in-memory map and cancels its timer.
func RemoveAgent(agentID string) {
	CancelTimer(agentID)
	mu.Lock()
	defer mu.Unlock()
	delete(agents, agentID)
}

// ResetHeartbeat stops any existing heartbeat timer for the agent and starts a
// new one. When it fires, the agent transitions UNRESPONSIVE → TIMED_OUT.
func ResetHeartbeat(agentID string) {
	timerMu.Lock()
	if t, ok := timers[agentID]; ok && t != nil {
		t.Stop()
	}
	timers[agentID] = time.AfterFunc(heartbeatTimeout, func() { onHeartbeatTimeout(agentID) })
	timerMu.Unlock()
}

// BeginGrace stops any existing timer and starts a grace-period timer. When it
// fires, the timeout callback is invoked with reason "grace_expired".
func BeginGrace(agentID string) {
	timerMu.Lock()
	if t, ok := timers[agentID]; ok && t != nil {
		t.Stop()
	}
	timers[agentID] = time.AfterFunc(gracePeriod, func() { onGraceExpired(agentID) })
	timerMu.Unlock()
}

// CancelTimer stops and removes the timer for the given agent.
func CancelTimer(agentID string) {
	timerMu.Lock()
	if t, ok := timers[agentID]; ok && t != nil {
		t.Stop()
		delete(timers, agentID)
	}
	timerMu.Unlock()
}

func cancelAllTimers() {
	timerMu.Lock()
	defer timerMu.Unlock()
	for id, t := range timers {
		if t != nil {
			t.Stop()
		}
		delete(timers, id)
	}
}

func onHeartbeatTimeout(agentID string) {
	timerMu.Lock()
	delete(timers, agentID)
	timerMu.Unlock()

	mu.Lock()
	agent, ok := agents[agentID]
	if !ok {
		mu.Unlock()
		return
	}

	// Guard against stale timer callbacks. ResetHeartbeat creates a new timer
	// but can't cancel an already-fired callback from the previous timer.
	// If the agent sent a heartbeat after this timer was scheduled, the
	// callback is stale — re-arm and skip.
	if agent.LastHeartbeatAt != nil {
		elapsed := time.Since(time.Unix(*agent.LastHeartbeatAt, 0))
		if elapsed < heartbeatTimeout {
			mu.Unlock()
			ResetHeartbeat(agentID)
			return
		}
	}

	switch agent.Status {
	case AgentStatusDisconnected, AgentStatusTimedOut:
		mu.Unlock()
		return

	case AgentStatusUnresponsive:
		agent.Status = AgentStatusTimedOut
		agent.CurrentShardID = nil
		agents[agentID] = agent
		cb := onTimeout
		mu.Unlock()
		if cb != nil {
			cb(agentID, "heartbeat_timed_out")
		}
		return

	default: // IDLE, RUNNING
		agent.Status = AgentStatusUnresponsive
		agents[agentID] = agent
		mu.Unlock()
		zap.L().Named("agent_service").Warn("agent unresponsive, waiting for one more heartbeat",
			zap.String("agent_id", agentID),
		)
		ResetHeartbeat(agentID)
	}
}

func onGraceExpired(agentID string) {
	timerMu.Lock()
	delete(timers, agentID)
	timerMu.Unlock()

	mu.Lock()
	agent, ok := agents[agentID]
	if !ok || agent.Status != AgentStatusDisconnected {
		mu.Unlock()
		return
	}
	mu.Unlock()

	cb := onTimeout
	if cb != nil {
		cb(agentID, "grace_expired")
	}
}
