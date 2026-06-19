package agent_service

import (
	"testing"

	"github.com/google/uuid"
)

func TestRegisterAgentAndHeartbeat(t *testing.T) {
	service := NewService()

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
	service := NewService()
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

	// Simulate disconnect then reconnect
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
	service := NewService()
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
	service := NewService()
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
	service := NewService()
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
	service := NewService()
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

func TestDisconnectAndReconnect(t *testing.T) {
	service := NewService()
	agent1 := "pod-agent-0"
	tenantA := uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
	shard1 := "shard-001"

	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	service.HeartbeatAgent(HeartbeatAgentParams{AgentID: agent1, Status: "RUNNING", CurrentShardID: &shard1})

	// Disconnect
	service.DisconnectAgent(agent1)
	agent := service.agents[agent1]
	if agent.Status != "DISCONNECTED" {
		t.Fatalf("expected DISCONNECTED, got %q", agent.Status)
	}
	if agent.PreDisconnectStatus != "RUNNING" {
		t.Fatalf("expected PreDisconnectStatus RUNNING, got %q", agent.PreDisconnectStatus)
	}

	// Reconnect
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
	service := NewService()
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
	service := NewService()
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

	// Reconnect
	service.RegisterAgent(RegisterAgentParams{AgentID: agent1, TenantID: tenantA, BackendName: "backend-a"})
	agent = service.agents[agent1]
	if agent.Status != "IDLE" {
		t.Fatalf("expected IDLE after reconnect, got %q", agent.Status)
	}
}

func TestRegisterAgentRejectsDuplicateLiveConnection(t *testing.T) {
	service := NewService()
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
