package agent_service

import (
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func setupTest(t *testing.T, cb TimeoutCallback) {
	t.Helper()
	Init()
	SetTimeoutCallback(cb)
	SetTimeouts(50*time.Millisecond, 50*time.Millisecond)
	t.Cleanup(func() {
		cancelAllTimers()
		mu.Lock()
		agents = nil
		mu.Unlock()
	})
}

func TestRegisterAgentAndHeartbeat(t *testing.T) {
	setupTest(t, nil)

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	if err := RegisterAgent(RegisterAgentParams{
		AgentID:     agent1,
		TenantID:    tenantA,
		BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	registered, ok := agents[agent1]
	if !ok {
		t.Fatalf("expected agent to be stored")
	}
	if registered.Status != "IDLE" {
		t.Fatalf("expected registered agent to be idle, got %q", registered.Status)
	}
	if registered.LastHeartbeatAt == nil {
		t.Fatalf("expected heartbeat timestamp")
	}

	if err := HeartbeatAgent(HeartbeatAgentParams{
		AgentID:        agent1,
		Status:         "RUNNING",
		CurrentShardID: &shard1,
	}); err != nil {
		t.Fatalf("heartbeat agent: %v", err)
	}

	updated := agents[agent1]
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
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: AgentStatusRunning, CurrentShardID: &shard1})

	DisconnectAgent(agent1)

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("re-register disconnected agent: %v", err)
	}

	registered := agents[agent1]
	if registered.Status != AgentStatusRunning {
		t.Fatalf("expected RUNNING state to be recovered, got %q", registered.Status)
	}
	if registered.CurrentShardID == nil || *registered.CurrentShardID != shard1 {
		t.Fatalf("expected current shard %s to be recovered, got %v", shard1, registered.CurrentShardID)
	}
}

func TestRegisterAgentRejectsCrossTenantOrBackendRebind(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	tenantB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantB, BackendName: "backend-a",
	}); err == nil {
		t.Fatalf("expected cross-tenant rebind to fail")
	}

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-b",
	}); err == nil {
		t.Fatalf("expected cross-backend rebind to fail")
	}

	registered := agents[agent1]
	if registered.TenantID != tenantA || registered.BackendName != "backend-a" {
		t.Fatalf("expected original binding to remain, got %s/%s", registered.TenantID, registered.BackendName)
	}
}

func TestGetActiveTenantIDs(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	agent2 := "pod-agent-1"
	agent3 := "pod-agent-2"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	tenantB := uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	tenants := GetActiveTenantIDs()
	if len(tenants) != 1 || !tenants[tenantA] {
		t.Fatalf("expected tenantA to be active, got %v", tenants)
	}

	RegisterAgent(RegisterAgentParams{AgentID: agent2, TenantID: tenantA, BackendName: "backend-a"})
	RegisterAgent(RegisterAgentParams{AgentID: agent3, TenantID: tenantB, BackendName: "backend-b"})

	tenants = GetActiveTenantIDs()
	if len(tenants) != 2 || !tenants[tenantA] || !tenants[tenantB] {
		t.Fatalf("expected 2 tenants active, got %v", tenants)
	}
}

func TestGetActiveTenantIDsExcludesDisconnected(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	tenants := GetActiveTenantIDs()
	if len(tenants) != 1 || !tenants[tenantA] {
		t.Fatalf("expected tenantA to be active before disconnect, got %v", tenants)
	}

	DisconnectAgent(agent1)

	tenants = GetActiveTenantIDs()
	if len(tenants) != 0 {
		t.Fatalf("expected no active tenants after disconnect, got %v", tenants)
	}
}

func TestGetActiveTenantIDsIncludesUnresponsive(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	mu.Lock()
	agent := agents[agent1]
	agent.Status = AgentStatusUnresponsive
	agents[agent1] = agent
	mu.Unlock()

	tenants := GetActiveTenantIDs()
	if len(tenants) != 1 || !tenants[tenantA] {
		t.Fatalf("expected UNRESPONSIVE agent to count as active, got %v", tenants)
	}
}

func TestGetActiveTenantIDsExcludesTimedOut(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	mu.Lock()
	agent := agents[agent1]
	agent.Status = AgentStatusTimedOut
	agents[agent1] = agent
	mu.Unlock()

	tenants := GetActiveTenantIDs()
	if len(tenants) != 0 {
		t.Fatalf("expected TIMED_OUT agent to NOT count as active, got %v", tenants)
	}
}

func TestDisconnectAndReconnect(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	DisconnectAgent(agent1)
	agent := agents[agent1]
	if agent.Status != "DISCONNECTED" {
		t.Fatalf("expected DISCONNECTED, got %q", agent.Status)
	}
	if agent.PreDisconnectStatus != "RUNNING" {
		t.Fatalf("expected PreDisconnectStatus RUNNING, got %q", agent.PreDisconnectStatus)
	}

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	agent = agents[agent1]
	if agent.Status != "RUNNING" {
		t.Fatalf("expected RUNNING after reconnect, got %q", agent.Status)
	}
	if agent.CurrentShardID == nil || *agent.CurrentShardID != shard1 {
		t.Fatalf("expected shard to be preserved after reconnect, got %v", agent.CurrentShardID)
	}
}

func TestRemoveAgent(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	if _, ok := agents[agent1]; !ok {
		t.Fatalf("expected agent to exist")
	}

	RemoveAgent(agent1)
	if _, ok := agents[agent1]; ok {
		t.Fatalf("expected agent to be removed")
	}
}

func TestDisconnectIdleAgentReconnectsAsIdle(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	DisconnectAgent(agent1)

	agent := agents[agent1]
	if agent.Status != "DISCONNECTED" {
		t.Fatalf("expected DISCONNECTED, got %q", agent.Status)
	}
	if agent.PreDisconnectStatus != "IDLE" {
		t.Fatalf("expected PreDisconnectStatus IDLE, got %q", agent.PreDisconnectStatus)
	}

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	agent = agents[agent1]
	if agent.Status != "IDLE" {
		t.Fatalf("expected IDLE after reconnect, got %q", agent.Status)
	}
}

func TestRegisterAgentRejectsDuplicateLiveConnection(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("register agent: %v", err)
	}

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err == nil {
		t.Fatalf("expected duplicate live register to fail")
	}
}

// --- Timer tests ---

func TestHeartbeatTimeoutTransitionsToUnresponsive(t *testing.T) {
	Init()
	SetTimeouts(200*time.Millisecond, 200*time.Millisecond)
	defer func() {
		cancelAllTimers()
	}()

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	ResetHeartbeat(agent1)

	time.Sleep(250 * time.Millisecond)

	agent := agents[agent1]
	if agent.Status != AgentStatusUnresponsive {
		t.Fatalf("expected UNRESPONSIVE after first timeout, got %q", agent.Status)
	}

	CancelTimer(agent1)
}

func TestSecondHeartbeatTimeoutTransitionsToTimedOut(t *testing.T) {
	var cbMu sync.Mutex
	var timedOutAgent string
	var timedOutReason string

	setupTest(t, func(agentID, reason string) {
		cbMu.Lock()
		timedOutAgent = agentID
		timedOutReason = reason
		cbMu.Unlock()
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})
	ResetHeartbeat(agent1)

	time.Sleep(200 * time.Millisecond)

	agent := agents[agent1]
	if agent.Status != AgentStatusTimedOut {
		t.Fatalf("expected TIMED_OUT after second timeout, got %q", agent.Status)
	}
	if agent.CurrentShardID != nil {
		t.Fatalf("expected CurrentShardID to be cleared on TIMED_OUT, got %v", agent.CurrentShardID)
	}

	cbMu.Lock()
	if timedOutAgent != agent1 {
		t.Fatalf("expected callback for agent1, got %q", timedOutAgent)
	}
	if timedOutReason != "heartbeat_timed_out" {
		t.Fatalf("expected heartbeat_timed_out reason, got %q", timedOutReason)
	}
	cbMu.Unlock()
}

func TestHeartbeatResetsUnresponsiveBack(t *testing.T) {
	Init()
	SetTimeouts(200*time.Millisecond, 200*time.Millisecond)
	defer func() {
		cancelAllTimers()
	}()

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})
	ResetHeartbeat(agent1)

	time.Sleep(250 * time.Millisecond)
	if agents[agent1].Status != AgentStatusUnresponsive {
		t.Fatalf("expected UNRESPONSIVE, got %q", agents[agent1].Status)
	}

	CancelTimer(agent1)
	HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	agent := agents[agent1]
	if agent.Status != AgentStatusRunning {
		t.Fatalf("expected RUNNING after heartbeat recovery, got %q", agent.Status)
	}
}

func TestGraceTimerFiresOnTimeout(t *testing.T) {
	var cbMu sync.Mutex
	var timedOutAgent string
	var timedOutReason string

	setupTest(t, func(agentID, reason string) {
		cbMu.Lock()
		timedOutAgent = agentID
		timedOutReason = reason
		cbMu.Unlock()
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	CancelTimer(agent1)
		BeginGrace(agent1)

	time.Sleep(100 * time.Millisecond)

	cbMu.Lock()
	if timedOutAgent != agent1 {
		t.Fatalf("expected callback for agent1, got %q", timedOutAgent)
	}
	if timedOutReason != "grace_expired" {
		t.Fatalf("expected grace_expired reason, got %q", timedOutReason)
	}
	cbMu.Unlock()
}

func TestCancelTimerPreventsFiring(t *testing.T) {
	var callbackFired bool
	setupTest(t, func(_, _ string) {
		callbackFired = true
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	ResetHeartbeat(agent1)
	CancelTimer(agent1)

	time.Sleep(100 * time.Millisecond)
	if callbackFired {
		t.Fatalf("expected callback NOT to fire after CancelTimer")
	}
}

func TestRegisterAgentRecoversFromTimedOut(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	mu.Lock()
	agent := agents[agent1]
	agent.Status = AgentStatusTimedOut
	agent.PreDisconnectStatus = AgentStatusIdle
	agents[agent1] = agent
	mu.Unlock()

	DisconnectAgent(agent1)
	disconnected := agents[agent1]
	if disconnected.Status != AgentStatusDisconnected {
		t.Fatalf("expected DISCONNECTED, got %q", disconnected.Status)
	}

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("reconnect from TIMED_OUT: %v", err)
	}

	reconnected := agents[agent1]
	if reconnected.Status != AgentStatusIdle {
		t.Fatalf("expected IDLE after reconnect from TIMED_OUT, got %q", reconnected.Status)
	}
}

func TestRegisterAgentRecoversFromUnresponsive(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	mu.Lock()
	agent := agents[agent1]
	agent.Status = AgentStatusUnresponsive
	agents[agent1] = agent
	mu.Unlock()

	DisconnectAgent(agent1)
	disconnected := agents[agent1]
	if disconnected.Status != AgentStatusDisconnected {
		t.Fatalf("expected DISCONNECTED, got %q", disconnected.Status)
	}

	if err := RegisterAgent(RegisterAgentParams{
		AgentID: agent1, TenantID: tenantA, BackendName: "backend-a",
	}); err != nil {
		t.Fatalf("reconnect from UNRESPONSIVE: %v", err)
	}

	reconnected := agents[agent1]
	if reconnected.Status != AgentStatusIdle {
		t.Fatalf("expected IDLE after reconnect from UNRESPONSIVE, got %q", reconnected.Status)
	}
}

func TestStaleTimerFireIgnored(t *testing.T) {
	var callbackFired bool
	setupTest(t, func(_, _ string) {
		callbackFired = true
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	ResetHeartbeat(agent1)

	RemoveAgent(agent1)

	time.Sleep(100 * time.Millisecond)
	if callbackFired {
		t.Fatalf("expected no callback after agent removal")
	}
}

func TestHeartbeatTimerResetPreventsTimeout(t *testing.T) {
	var callbackFired bool
	setupTest(t, func(_, _ string) {
		callbackFired = true
	})

	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	for i := 0; i < 5; i++ {
		ResetHeartbeat(agent1)
		time.Sleep(30 * time.Millisecond)
	}

	if callbackFired {
		t.Fatalf("expected no callback when timer is repeatedly reset")
	}

	agent := agents[agent1]
	if agent.Status != AgentStatusIdle {
		t.Fatalf("expected IDLE when timer is kept alive, got %q", agent.Status)
	}
}

func TestDisconnectOfUnresponsiveAgent(t *testing.T) {
	setupTest(t, nil)
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})

	mu.Lock()
	agent := agents[agent1]
	agent.Status = AgentStatusUnresponsive
	agents[agent1] = agent
	mu.Unlock()

	DisconnectAgent(agent1)
	disconnected := agents[agent1]
	if disconnected.Status != AgentStatusDisconnected {
		t.Fatalf("expected DISCONNECTED, got %q", disconnected.Status)
	}
	if disconnected.PreDisconnectStatus != AgentStatusIdle {
		t.Fatalf("expected PreDisconnectStatus IDLE for UNRESPONSIVE, got %q", disconnected.PreDisconnectStatus)
	}
}
