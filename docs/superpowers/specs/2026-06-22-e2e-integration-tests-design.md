# E2E Integration Tests Design

## Problem

Previous integration tests used SQLite in-memory database with mock gRPC streams, which doesn't validate real-world behavior (actual PostgreSQL queries, k8s StatefulSet provisioning, agent reconnection, cluster recycling). Need true end-to-end tests against real infrastructure.

## Approach

Tests in `test/e2e/` with `//go:build e2e` build tag. Manual run only via `go test -tags=e2e ./test/e2e/...`.

## Environment Variables

All existing config env vars are reused:

- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` — PostgreSQL connection
- `CLUSTER_AGENT_TOKEN` — agent authentication
- `CLUSTER_KUBERNETES_KUBECONFIG_PATH` — k8s cluster access
- `CLUSTER_AGENT_IMAGE` — agent container image
- `SERVER_GRPC_ADDR` — (optional) override gRPC listen address

No test-specific env vars. The orchestrator reads the same config as production.

## TestMain

`test/e2e/main_test.go`:

1. Load config from env (via `config.LoadFromEnv()`)
2. Connect to real PostgreSQL, run migrations
3. Start gRPC server on random port (like old integration tests)
4. Init `agent_service`
5. Start background goroutines: `PollPendingTasks`, `RecycleClusters`
6. Run test functions
7. Cleanup: stop server, stop goroutines, clean up test k8s resources

## Test Scenarios

### Task lifecycle
- Submit task → wait for all shards to complete → verify task status SUCCEEDED → verify result CSV exists
- Submit task → cancel → verify task status CANCELLED, shards CANCELLED

### Agent lifecycle
- Verify agent connects and receives shard assignment
- Verify agent reconnection after server restart scenario (if feasible)

### Cluster lifecycle
- Submit task → cluster is provisioned (StatefulSet created)
- After idle threshold, verify cluster is terminated (StatefulSet deleted)

## Test Helpers

`test/e2e/helpers.go`:

- `submitTask(t, tenantName, bundlePath, inputCSV) taskID` — submit via `task_service.SubmitTask`
- `waitForTaskStatus(t, taskID, status, timeout)` — poll `GetByID` until status matches
- `getTaskResultCSV(t, taskID) []byte` — fetch merged result CSV
- `createTestTenant(t, name, maxAgents) tenantID` — create tenant in DB
- `cleanupTenant(t, tenantID)` — delete tenant and all associated data
- `setupKubeconfig(t)` — verify KUBECONFIG_PATH is set

## Files

```
test/e2e/
  main_test.go         — TestMain: start server, DB, goroutines
  helpers.go           — shared test helpers
  task_lifecycle_test.go — submit, execute, cancel tests
  cluster_lifecycle_test.go — provision, recycle tests
```

## Test Data

Reuses existing `test-data/params.csv` for input. Test bundles can be minimal (a simple run.sh that echoes output).
