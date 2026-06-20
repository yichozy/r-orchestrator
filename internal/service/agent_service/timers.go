package agent_service

import (
	"time"

	"go.uber.org/zap"
)

// ResetHeartbeatTimer stops any existing timer for the agent and starts a new
// heartbeat timer. Should be called after each successful heartbeat.
func (s *Service) ResetHeartbeatTimer(agentID string) {
	s.timerMu.Lock()
	if t, ok := s.timers[agentID]; ok && t != nil {
		t.Stop()
	}
	s.timers[agentID] = time.AfterFunc(s.heartbeatTimeout, func() { s.heartbeatFired(agentID) })
	s.timerMu.Unlock()
}

// StartGraceTimer stops any existing timer for the agent and starts a grace
// timer. Should be called when the agent's gRPC stream disconnects.
func (s *Service) StartGraceTimer(agentID string) {
	s.timerMu.Lock()
	if t, ok := s.timers[agentID]; ok && t != nil {
		t.Stop()
	}
	s.timers[agentID] = time.AfterFunc(s.gracePeriod, func() { s.graceFired(agentID) })
	s.timerMu.Unlock()
}

// StopTimer stops and removes the timer for the agent. Safe to call when no
// timer exists.
func (s *Service) StopTimer(agentID string) {
	s.timerMu.Lock()
	if t, ok := s.timers[agentID]; ok && t != nil {
		t.Stop()
		delete(s.timers, agentID)
	}
	s.timerMu.Unlock()
}

// heartbeatFired runs on the timer goroutine. It acquires mu to check agent
// state and take action.
func (s *Service) heartbeatFired(agentID string) {
	// Remove the expired timer entry so it doesn't leak.
	s.timerMu.Lock()
	delete(s.timers, agentID)
	s.timerMu.Unlock()

	s.mu.Lock()
	agent, ok := s.agents[agentID]
	if !ok {
		s.mu.Unlock()
		return
	}

	switch agent.Status {
	case AgentStatusDisconnected, AgentStatusTimedOut:
		s.mu.Unlock()
		return

	case AgentStatusUnresponsive:
		agent.Status = AgentStatusTimedOut
		agent.CurrentShardID = nil
		s.agents[agentID] = agent
		cb := s.onTimeout
		s.mu.Unlock()
		if cb != nil {
			cb(agentID, "heartbeat_timed_out")
		}
		return

	default: // IDLE, RUNNING, RESULT_READY
		agent.Status = AgentStatusUnresponsive
		s.agents[agentID] = agent
		s.mu.Unlock()
		s.logger.Warn("agent unresponsive, waiting for one more heartbeat",
			zap.String("agent_id", agentID),
		)
		s.ResetHeartbeatTimer(agentID)
	}
}

// graceFired runs on the timer goroutine.
func (s *Service) graceFired(agentID string) {
	s.timerMu.Lock()
	delete(s.timers, agentID)
	s.timerMu.Unlock()

	s.mu.Lock()
	agent, ok := s.agents[agentID]
	if !ok || agent.Status == AgentStatusTimedOut {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	cb := s.onTimeout
	if cb != nil {
		cb(agentID, "grace_expired")
	}
}
