package agent_service

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

// newTestService creates a service with short timeouts for testing.
func newTestService(t *testing.T, cb TimeoutCallback) *Service {
	t.Helper()
	s := NewService()
	s.SetTimeoutCallback(cb)
	s.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	return s
}

func TestRegisterAgentAndHeartbeat(t *testing.T) {
	service := newTestService(t, nil)

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID:     agent1,
		TenantID:    tenantA,
		BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	registered, ok := service.agents[agent1]
	if !ok {
		t.Fatalf("expected agent to be stored")
	}
	if registered.Status != "IDLE" {
		t.Fatalf("expected registered agent to be idle, got %q", registered.Status)
	}
	if registered.LastHeartbeatAt == nil {
		t.Fatalf("expected heartbeat timestamp")
	}

	if err := service.HeartbeatAgent(HeartbeatAgentParams{
		AgentID:        agent1,
		Status:         "RUNNING",
		CurrentShardID: &shard1,
	}); err != nil {
		t.Fatalf("heartbeat agent: %v", err)
	}

	updated := service.agents[agent1]
	if updated.Status != "RUNNING" {
		t.Fatalf("expected running status after heartbeat, got %q", updated.Status)
	}
	if updated.CurrentShardID == nil || *updated.CurrentShardID != shard1 {
		t.Fatalf("expected current shard %s, got %v", shard1, updated.CurrentShardID)
	}
	if updated.LastHeartbeatAt == nil {
		t.Fatalf("expected heartbeat timestamp after heartbeat")
	}
}

func TestRegisterAgentPreservesRunningStateForSameIdentityReconnect(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := service.HeartbeatAgent(HeartbeatAgentParams{
		AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1,
	}); err != nil {
		t.Fatalf("heartbeat running agent: %v", err)
	}

	service.DisconnectAgent(agent1)

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("re-register disconnected agent: %v", err)
	}

	registered := service.agents[agent1]
	if registered.Status != "RUNNING" {
		t.Fatalf("expected RUNNING state to be recovered, got %q", registered.Status)
	}
	if registered.CurrentShardID == nil || *registered.CurrentShardID != shard1 {
		t.Fatalf("expected current shard %s to be recovered, got %v", shard1, registered.CurrentShardID)
	}
}

func TestRegisterAgentPreservesResultReadyStateForSameIdentityReconnect(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}
	if err := service.HeartbeatAgent(HeartbeatAgentParams{
		AgentID: agent1, Status: AgentStatusResultReady, CurrentShardID: &shard1,
	}); err != nil {
		t.Fatalf("heartbeat result-ready agent: %v", err)
	}

	service.DisconnectAgent(agent1)

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("re-register disconnected agent: %v", err)
	}

	registered := service.agents[agent1]
	if registered.Status != AgentStatusResultReady {
		t.Fatalf("expected RESULT_READY state to be recovered, got %q", registered.Status)
	}
	if registered.CurrentShardID == nil || *registered.CurrentShardID != shard1 {
		t.Fatalf("expected current shard %s to be recovered, got %v", shard1, registered.CurrentShardID)
	}
}

func TestRegisterAgentRejectsCrossTenantOrBackendRebind(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	tenantB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantB, BackendName: "backend-a",
	}); err == nil {
		t.Fatalf("expected cross-tenant rebind to fail")
	}

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-b",
	}); err == nil {
		t.Fatalf("expected cross-backend rebind to fail")
	}

	registered := service.agents[agent1]
	if registered.TenantID != tenantA || registered.BackendName != "backend-a" {
		t.Fatalf("expected original binding to remain, got %s/%s", registered.TenantID, registered.BackendName)
	}
}

func TestGetActiveTenantIDs(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	agent2 := "pod-agent-1"
	agent3 := "pod-agent-2"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	tenantB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	tenants := service.GetActiveTenantIDs()
	if len(tenants) != 1 || !tenants[tenantA] {
		t.Fatalf("expected tenantA to be active, got %v", tenants)
	}

	service.RegisterAgent(RegisterAgentParams{AgentID: agent2, TenantID: tenantA, BackendName: "backend-a"})
	service.RegisterAgent(RegisterAgentParams{AgentID: agent3, TenantID: tenantB, BackendName: "backend-b"})

	tenants = service.GetActiveTenantIDs()
	if len(tenants) != 2 || !tenants[tenantA] || !tenants[tenantB] {
		t.Fatalf("expected 2 tenants active, got %v", tenants)
	}
}

func TestGetActiveTenantIDsExcludesDisconnected(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	tenants := service.GetActiveTenantIDs()
	if len(tenants) != 1 || !tenants[tenantA] {
		t.Fatalf("expected tenantA to be active before disconnect, got %v", tenants)
	}

	service.DisconnectAgent(agent1)

	tenants = service.GetActiveTenantIDs()
	if len(tenants) != 0 {
		t.Fatalf("expected no active tenants after disconnect, got %v", tenants)
	}
}

func TestGetActiveTenantIDsIncludesUnresponsive(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	// Simulate UNRESPONSIVE
	service.mu.Lock()
	agent := service.agents[agent1]
	agent.Status = AgentStatusUnresponsive
	service.agents[agent1] = agent
	service.mu.Unlock()

	tenants := service.GetActiveTenantIDs()
	if len(tenants) != 1 || !tenants[tenantA] {
		t.Fatalf("expected UNRESPONSIVE agent to count as active, got %v", tenants)
	}
}

func TestGetActiveTenantIDsExcludesTimedOut(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	// Simulate TIMED_OUT
	service.mu.Lock()
	agent := service.agents[agent1]
	agent.Status = AgentStatusTimedOut
	service.agents[agent1] = agent
	service.mu.Unlock()

	tenants := service.GetActiveTenantIDs()
	if len(tenants) != 0 {
		t.Fatalf("expected TIMED_OUT agent to NOT count as active, got %v", tenants)
	}
}

func TestDisconnectAndReconnect(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	service.DisconnectAgent(agent1)
	agent := service.agents[agent1]
	if agent.Status != "DISCONNECTED" {
		t.Fatalf("expected DISCONNECTED, got %q", agent.Status)
	}
	if agent.PreDisconnectStatus != "RUNNING" {
		t.Fatalf("expected PreDisconnectStatus RUNNING, got %q", agent.PreDisconnectStatus)
	}

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	agent = service.agents[agent1]
	if agent.Status != "RUNNING" {
		t.Fatalf("expected RUNNING after reconnect, got %q", agent.Status)
	}
	if agent.CurrentShardID == nil || *agent.CurrentShardID != shard1 {
		t.Fatalf("expected shard to be preserved after reconnect, got %v", agent.CurrentShardID)
	}
}

func TestRemoveAgent(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	if _, ok := service.agents[agent1]; !ok {
		t.Fatalf("expected agent to exist")
	}

	service.RemoveAgent(agent1)
	if _, ok := service.agents[agent1]; ok {
		t.Fatalf("expected agent to be removed")
	}
}

func TestDisconnectIdleAgentReconnectsAsIdle(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.DisconnectAgent(agent1)

	agent := service.agents[agent1]
	if agent.Status != "DISCONNECTED" {
		t.Fatalf("expected DISCONNECTED, got %q", agent.Status)
	}
	if agent.PreDisconnectStatus != "IDLE" {
		t.Fatalf("expected PreDisconnectStatus IDLE, got %q", agent.PreDisconnectStatus)
	}

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	agent = service.agents[agent1]
	if agent.Status != "IDLE" {
		t.Fatalf("expected IDLE after reconnect, got %q", agent.Status)
	}
}

func TestRegisterAgentRejectsDuplicateLiveConnection(t *testing.T) {
	service := newTestService(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err == nil {
		t.Fatalf("expected duplicate live register to fail")
	}
}

// --- Timer tests ---

func TestHeartbeatTimeoutTransitionsToUnresponsive(t *testing.T) {
	s := NewService(); s.SetTimeouts(200*time.Millisecond, 200*time.Millisecond)

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	s.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	s.ResetHeartbeatTimer(agent1)

	// Wait for first timeout (UNRESPONSIVE) but not long enough for second
	time.Sleep(250 * time.Millisecond)

	agent := s.agents[agent1]
	if agent.Status != AgentStatusUnresponsive {
		t.Fatalf("expected UNRESPONSIVE after first timeout, got %q", agent.Status)
	}

	// Clean up timer to prevent second fire
	s.StopTimer(agent1)
}

func TestSecondHeartbeatTimeoutTransitionsToTimedOut(t *testing.T) {
	var mu sync.Mutex
	var timedOutAgent string
	var timedOutReason string

	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	service.SetTimeoutCallback(func(agentID, reason string) {
		mu.Lock()
		timedOutAgent = agentID
		timedOutReason = reason
		mu.Unlock()
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})
	service.ResetHeartbeatTimer(agent1)

	// Wait for first timeout (UNRESPONSIVE) + second timeout (TIMED_OUT)
	time.Sleep(200 * time.Millisecond)

	agent := service.agents[agent1]
	if agent.Status != AgentStatusTimedOut {
		t.Fatalf("expected TIMED_OUT after second timeout, got %q", agent.Status)
	}
	if agent.CurrentShardID != nil {
		t.Fatalf("expected CurrentShardID to be cleared on TIMED_OUT, got %v", agent.CurrentShardID)
	}

	mu.Lock()
	if timedOutAgent != agent1 {
		t.Fatalf("expected callback for agent1, got %q", timedOutAgent)
	}
	if timedOutReason != "heartbeat_timed_out" {
		t.Fatalf("expected heartbeat_timed_out reason, got %q", timedOutReason)
	}
	mu.Unlock()
}

func TestHeartbeatResetsUnresponsiveBack(t *testing.T) {
	service := NewService()
	service.SetTimeouts(200*time.Millisecond, 200*time.Millisecond)

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})
	service.ResetHeartbeatTimer(agent1)

	// Wait for first timeout (UNRESPONSIVE) but not second
	time.Sleep(250 * time.Millisecond)
	if service.agents[agent1].Status != AgentStatusUnresponsive {
		t.Fatalf("expected UNRESPONSIVE, got %q", service.agents[agent1].Status)
	}

	// Stop the pending second timer, then simulate heartbeat recovery
	service.StopTimer(agent1)
	service.HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	agent := service.agents[agent1]
	if agent.Status != AgentStatusRunning {
		t.Fatalf("expected RUNNING after heartbeat recovery, got %q", agent.Status)
	}
}

func TestGraceTimerFiresOnTimeout(t *testing.T) {
	var mu sync.Mutex
	var timedOutAgent string
	var timedOutReason string

	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	service.SetTimeoutCallback(func(agentID, reason string) {
		mu.Lock()
		timedOutAgent = agentID
		timedOutReason = reason
		mu.Unlock()
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.StopTimer(agent1)
	service.StartGraceTimer(agent1)

	// Wait for grace timer to fire
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	if timedOutAgent != agent1 {
		t.Fatalf("expected callback for agent1, got %q", timedOutAgent)
	}
	if timedOutReason != "grace_expired" {
		t.Fatalf("expected grace_expired reason, got %q", timedOutReason)
	}
	mu.Unlock()
}

func TestStopTimerPreventsFiring(t *testing.T) {
	var callbackFired bool
	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	service.SetTimeoutCallback(func(_, _ string) {
		callbackFired = true
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.ResetHeartbeatTimer(agent1)
	service.StopTimer(agent1)

	time.Sleep(100 * time.Millisecond)
	if callbackFired {
		t.Fatalf("expected callback NOT to fire after StopTimer")
	}
}

func TestRegisterAgentRecoversFromTimedOut(t *testing.T) {
	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	// Simulate TIMED_OUT state
	service.mu.Lock()
	agent := service.agents[agent1]
	agent.Status = AgentStatusTimedOut
	agent.PreDisconnectStatus = AgentStatusIdle
	service.agents[agent1] = agent
	service.mu.Unlock()

	// Disconnect
	service.DisconnectAgent(agent1)
	disconnected := service.agents[agent1]
	if disconnected.Status != AgentStatusDisconnected {
		t.Fatalf("expected DISCONNECTED, got %q", disconnected.Status)
	}

	// Reconnect
	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("reconnect from TIMED_OUT: %v", err)
	}

	reconnected := service.agents[agent1]
	if reconnected.Status != AgentStatusIdle {
		t.Fatalf("expected IDLE after reconnect from TIMED_OUT, got %q", reconnected.Status)
	}
}

func TestRegisterAgentRecoversFromUnresponsive(t *testing.T) {
	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	// Simulate UNRESPONSIVE state
	service.mu.Lock()
	agent := service.agents[agent1]
	agent.Status = AgentStatusUnresponsive
	service.agents[agent1] = agent
	service.mu.Unlock()

	// Disconnect
	service.DisconnectAgent(agent1)
	disconnected := service.agents[agent1]
	if disconnected.Status != AgentStatusDisconnected {
		t.Fatalf("expected DISCONNECTED, got %q", disconnected.Status)
	}

	// Reconnect
	if err := service.RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("reconnect from UNRESPONSIVE: %v", err)
	}

	reconnected := service.agents[agent1]
	if reconnected.Status != AgentStatusIdle {
		t.Fatalf("expected IDLE after reconnect from UNRESPONSIVE, got %q", reconnected.Status)
	}
}

func TestStaleTimerFireIgnored(t *testing.T) {
	var callbackFired bool
	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	service.SetTimeoutCallback(func(_, _ string) {
		callbackFired = true
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.ResetHeartbeatTimer(agent1)

	// Remove agent before timer fires
	service.RemoveAgent(agent1)

	time.Sleep(100 * time.Millisecond)
	if callbackFired {
		t.Fatalf("expected no callback after agent removal")
	}
}

func TestHeartbeatTimerResetPreventsTimeout(t *testing.T) {
	var callbackFired bool
	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	service.SetTimeoutCallback(func(_, _ string) {
		callbackFired = true
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	// Rapidly reset the timer to prevent timeout
	for i := 0; i < 5; i++ {
		service.ResetHeartbeatTimer(agent1)
		time.Sleep(30 * time.Millisecond)
	}

	if callbackFired {
		t.Fatalf("expected no callback when timer is repeatedly reset")
	}

	agent := service.agents[agent1]
	if agent.Status != AgentStatusIdle {
		t.Fatalf("expected IDLE when timer is kept alive, got %q", agent.Status)
	}
}

func TestDisconnectOfUnresponsiveAgent(t *testing.T) {
	service := NewService()
	service.SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	// Simulate UNRESPONSIVE
	service.mu.Lock()
	agent := service.agents[agent1]
	agent.Status = AgentStatusUnresponsive
	service.agents[agent1] = agent
	service.mu.Unlock()

	service.DisconnectAgent(agent1)
	disconnected := service.agents[agent1]
	if disconnected.Status != AgentStatusDisconnected {
		t.Fatalf("expected DISCONNECTED, got %q", disconnected.Status)
	}
	if disconnected.PreDisconnectStatus != AgentStatusIdle {
		t.Fatalf("expected PreDisconnectStatus IDLE for UNRESPONSIVE, got %q", disconnected.PreDisconnectStatus)
	}
}
