# Cluster Idle Recycling Design

## Problem

Current cluster recycling logic is too aggressive: clusters with no active tasks are terminated immediately, regardless of billing cycle timing. This causes unnecessary churn when tasks arrive in bursts.

## New Behavior

Clusters should only be recycled when idle for a sustained period AND near a billing boundary.

### Rules

1. Active tasks exist → reset `idle_since` to NULL, renew billing as usual near boundary
2. No active tasks and `idle_since` is NULL → set `idle_since = now()`, start idle timer
3. No active tasks and `idle_since` is set → let it count (no reset on renewal)
4. Near billing boundary:
   - `idle_since != NULL` and `now - idle_since >= idle_threshold` → don't renew, terminate
   - Otherwise → renew normally (do NOT reset `idle_since`)

### DB Change

Add `idle_since` column to `clusters` table:

```sql
ALTER TABLE clusters ADD COLUMN idle_since TIMESTAMP WITH TIME ZONE;
```

Go model:

```go
type Cluster struct {
    ...
    IdleSince *time.Time `gorm:"column:idle_since"`
}
```

### Config

Add `IdleThresholdSeconds` to `ClusterConfig` (default: 600 = 10 minutes).

Env var: `CLUSTER_IDLE_THRESHOLD_SECONDS`

### Recycler Flow

```
every 30s:
  for each ACTIVE cluster:
    if has_active_tasks(tenant):
      idle_since = NULL
      if near_boundary: renew
    else:
      if idle_since is NULL: idle_since = now()
      if near_boundary:
        if idle_since != NULL && now - idle_since >= threshold:
          terminate cluster
        else:
          renew billing
```

## Files to Modify

- `internal/model/cluster.go` — add `IdleSince` field
- `internal/config/config.go` — add `IdleThresholdSeconds`
- `internal/service/cluster_service/billing.go` — rewrite `ShouldTerminate` to use idle timer
- `internal/service/cluster_service/recycler.go` — update `evaluateCluster` to manage `idle_since`
- `internal/service/cluster_service/service.go` — add `UpdateIdleSince` ORM function
- `internal/orm/cluster_orm/` — add `UpdateIdleSince` if not in service layer
