# Agent Heartbeat Timeout and Shard Rollback

## Problem

The current shard timeout mechanism is a patchwork of independent systems:

- `TouchShardUpdatedAt` refreshes shard `updated_at` on every heartbeat (redundant — `activeAgentIDs` already protects active agents from the reaper)
- `ResetTimedOutShards` periodically scans all shards in DB, checking `updated_at` and `activeAgentIDs`
- `DisconnectAgent` / `HandleReconnectedAgent` / `ensureAgentCanBecomeIdle` handle reconnection recovery independently
- These mechanisms have overlapping responsibilities and unclear boundaries

## Design

Event-driven agent lifecycle. Agent is the source of truth for shard liveness. No periodic DB scanning.

### Agent State Machine

```
                          heartbeat timeout (1st)
    IDLE ──────────────────────────────────────→ UNRESPONSIVE
      ↑                                           │
      │ heartbeat (recover)              heartbeat timeout (2nd, consecutive)
      │                                           ↓
      │                                       TIMED_OUT
      │                                           │
    assign shard                            rollback shard → QUEUED
      │                                           │
      ↓                                           ↓
    RUNNING ←─────────────── assign shard ────────→ IDLE (reset)
      │
      ↓ (result ready)
    RESULT_READY
      │
      ↓ (result stored)
    IDLE
```

New states:
- **UNRESPONSIVE**: One heartbeat timeout. May be a network glitch. Recovers to previous state on next heartbeat.
- **TIMED_OUT**: Two consecutive heartbeat timeouts. Confirmed unresponsive. Triggers shard rollback.

Stream disconnect follows an independent path (does not go through UNRESPONSIVE):
- Stream disconnect → **DISCONNECTED** (existing) → start grace timer → grace expires → rollback shard

### Timer Mechanism

**Heartbeat Timer** (per agent):
- Started on agent registration, reset on each `HeartbeatAgent` call.
- On expiration:
  - If agent is already UNRESPONSIVE → transition to TIMED_OUT, rollback shard to QUEUED.
  - Otherwise → transition to UNRESPONSIVE, log warning, reset timer for one more round.

**Grace Timer** (per agent, started on stream disconnect):
- Started when gRPC stream dies (cancels heartbeat timer).
- Cancelled when agent reconnects within grace period.
- On expiration → rollback shard to QUEUED.

### Deletions

- `TouchShardUpdatedAt` in `handle_heartbeat.go` — no longer needed.
- `reset_timed_out_shards.go` (ORM) — periodic scan replaced by event-driven timers.
- `ResetTimedOutShards` control loop — replaced by timer per agent.
- `touch_updated_at.go` (ORM) — no longer needed.

### Server Startup Cleanup

On startup, one-time scan of DB for orphaned shards (shards in LEASED/RUNNING/RESULT_READY whose `assigned_agent_id` has no registered agent in memory). If shard `started_at` exceeds the orphan threshold, roll back to QUEUED.

This is a startup task, not a periodic goroutine. After startup, all shard lifecycle is event-driven.

### Timeout Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `heartbeat_timeout` | 90s | Single heartbeat timeout |
| `disconnect_grace` | 5min | Grace period after stream disconnect |
| `startup_orphan_threshold` | 10min | Max age for orphaned shards on startup |

### Implementation Changes

1. **`agent_service`**: Add UNRESPONSIVE and TIMED_OUT states. Add timer management (heartbeat timer, grace timer). Expose methods for timer start/stop/reset.
2. **`control/server.go`**: On `HeartbeatAgent`, reset heartbeat timer via agent_service. On stream disconnect (`DisconnectAgent`), cancel heartbeat timer and start grace timer.
3. **`control/handle_heartbeat.go`**: Remove `TouchShardUpdatedAt` call.
4. **`control/reset_timeout_shards.go`**: Replace with startup orphan cleanup function.
5. **`cmd/server/main.go`**: Replace `ResetTimedOutShards` goroutine with one-time startup cleanup call.

### Shard Rollback on Timeout

When a heartbeat timer or grace timer fires:
1. Find the agent's current shard (from `agent.CurrentShardID`).
2. Call `task_shard_orm.RollbackShardLease` to roll back the shard to QUEUED.
3. If the task was in WAITING_FOR_AGENTS, call `task_orm.RollbackTaskToWaiting`.
4. Remove the agent from agent_service (or mark as TIMED_OUT / cleaned up).

Existing recovery paths remain unchanged:
- Agent reconnects → `HandleReconnectedAgent` validates state against DB.
- Agent reports IDLE → `ensureAgentCanBecomeIdle` rolls back orphaned shards.

### Conflict Resolution

**1. `RegisterAgent` recovery logic**

Current `RegisterAgent` filters `PreDisconnectStatus` on reconnect:
```go
if recoveryStatus == "" || recoveryStatus == AgentStatusDisconnected || recoveryStatus == AgentStatusIdle {
    recoveryStatus = AgentStatusIdle
}
```

With TIMED_OUT, an agent that was timed out then disconnected would have `PreDisconnectStatus = TIMED_OUT`, which bypasses the filter and recovers to TIMED_OUT — wrong, since the shard was already rolled back.

**Fix**: Add TIMED_OUT (and UNRESPONSIVE) to the recovery filter, treating both as IDLE recovery:
```go
if recoveryStatus == "" || recoveryStatus == AgentStatusDisconnected ||
   recoveryStatus == AgentStatusIdle || recoveryStatus == AgentStatusTimedOut ||
   recoveryStatus == AgentStatusUnresponsive {
    recoveryStatus = AgentStatusIdle
}
```

**2. `GetActiveTenantIDs` status check**

UNRESPONSIVE agents may recover, so they should count as active (tenant still has capacity). TIMED_OUT agents should not count (shard was rolled back, agent is effectively gone).

**Fix**: Add UNRESPONSIVE to the active check:
```go
if a.Status == AgentStatusIdle || a.Status == AgentStatusRunning ||
   a.Status == AgentStatusResultReady || a.Status == AgentStatusUnresponsive {
```

**3. Heartbeat timer vs DisconnectAgent race**

The heartbeat timer callback runs in a separate goroutine. If the stream dies and `DisconnectAgent` runs concurrently with a timer firing:
- `DisconnectAgent` should cancel the heartbeat timer first, then start the grace timer.
- The timer callback should check agent status: if agent is already DISCONNECTED, skip the timeout action (grace timer will handle it).

**4. `NotifyCancelShard` interaction**

No conflict. `NotifyCancelShard` sends a cancel message to the agent via stream. If the agent is also timing out, the cancel takes priority (user-initiated action). The timer should be cancelled if the shard is cancelled, since the shard status would no longer be LEASED/RUNNING.

**5. `shouldTryAssign` and status checks**

No change needed. Only assigns to IDLE agents with no current shard. UNRESPONSIVE and TIMED_OUT agents are excluded naturally.
