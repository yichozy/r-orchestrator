//go:build e2e

package e2e

import (
	"testing"
	"time"

)

func TestSubmitAndExecute(t *testing.T) {
	requireEnv(t, "DB_HOST", "CLUSTER_AGENT_TOKEN")

	tenantName := "e2e-task-lifecycle"
	tenantID := createTestTenant(t, tenantName, 1)
	defer cleanupTestTenant(t, tenantID)

	taskID := submitTestTask(t, tenantName,
		"test-data/e2e-bundle/bundle.zip",
		"test-data/params.csv",
	)

	// Wait for task to complete. Real k8s provisioning + agent execution can be slow.
	waitForTaskStatus(t, taskID, "SUCCEEDED", 10*time.Minute)

	// Verify shard count matches expected.
	shardCount := mustGetShardCount(t, taskID)
	if shardCount == 0 {
		t.Fatal("expected at least 1 shard")
	}
}

func TestSubmitAndCancel(t *testing.T) {
	requireEnv(t, "DB_HOST", "CLUSTER_AGENT_TOKEN")

	tenantName := "e2e-task-cancel"
	tenantID := createTestTenant(t, tenantName, 1)
	defer cleanupTestTenant(t, tenantID)

	taskID := submitTestTask(t, tenantName,
		"test-data/e2e-bundle/bundle.zip",
		"test-data/params.csv",
	)

	// Wait a bit for the task to be picked up by poll_pending_tasks.
	time.Sleep(5 * time.Second)

	cancelTestTask(t, tenantName, taskID)
	waitForTaskStatus(t, taskID, "CANCELLED", 2*time.Minute)
}
