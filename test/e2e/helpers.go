//go:build e2e

package e2e

import (
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
)

// requireEnv skips the test if any of the given env vars are not set.
func requireEnv(t *testing.T, keys ...string) {
	t.Helper()
	for _, key := range keys {
		if os.Getenv(key) == "" {
			t.Skipf("skipped: %s not set", key)
		}
	}
}

func createTestTenant(t *testing.T, name string, maxAgents int) uuid.UUID {
	t.Helper()
	db, err := orm.GetDB()
	if err != nil {
		t.Fatalf("get db: %v", err)
	}

	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	if _, err := tenant_orm.GetById(testCtx, db, tenantID); err == nil {
			// Tenant already exists from a previous run, delete it.
		cleanupTestTenant(t, tenantID)
	}

	tenant := model.Tenant{
		BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
		Name:               name,
		PrimaryBackendName: "kubernetes",
		MaxAgents:          maxAgents,
	}
	_, err = tenant_orm.Create(testCtx, db, tenant)
	if err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	return tenantID
}

func cleanupTestTenant(t *testing.T, tenantID uuid.UUID) {
	t.Helper()
	db, err := orm.GetDB()
	if err != nil {
		t.Logf("cleanup tenant: get db failed: %v", err)
		return
	}

	// Delete shards.
	var taskIDs []uuid.UUID
	db.WithContext(testCtx).Model(&model.Task{}).Where("tenant_id = ?", tenantID).Pluck("id", &taskIDs)
	if len(taskIDs) > 0 {
		db.WithContext(testCtx).Where("task_id IN ?", taskIDs).Delete(&model.TaskShard{})
	}
	// Delete tasks.
	db.WithContext(testCtx).Where("tenant_id = ?", tenantID).Delete(&model.Task{})
	// Delete cluster.
	db.WithContext(testCtx).Where("tenant_id = ?", tenantID).Delete(&model.Cluster{})
	// Delete tenant.
	db.WithContext(testCtx).Where("id = ?", tenantID).Delete(&model.Tenant{})
}

func submitTestTask(t *testing.T, tenantName string, zipPath, csvPath string) uuid.UUID {
	t.Helper()
	zipBytes, err := os.ReadFile(zipPath)
	if err != nil {
		t.Fatalf("read zip: %v", err)
	}
	csvBytes, err := os.ReadFile(csvPath)
	if err != nil {
		t.Fatalf("read csv: %v", err)
	}

	taskID, err := task_service.SubmitTask(testCtx, task_service.SubmitTaskParams{
		TenantName: tenantName,
		ZipBytes:   zipBytes,
		CSVBytes:   csvBytes,
	})
	if err != nil {
		t.Fatalf("submit task: %v", err)
	}
	return taskID
}

func waitForTaskStatus(t *testing.T, taskID uuid.UUID, status string, timeout time.Duration) {
	t.Helper()
	db, err := orm.GetDB()
	if err != nil {
		t.Fatalf("get db: %v", err)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task, err := task_orm.GetByID(testCtx, db, taskID)
		if err != nil {
			t.Fatalf("get task: %v", err)
		}
		if task.Status == status {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out waiting for task %s to be %s, last status: %s", taskID, status, mustGetTaskStatus(t, taskID))
}

func cancelTestTask(t *testing.T, tenantName string, taskID uuid.UUID) {
	t.Helper()
	if err := task_service.CancelTask(testCtx, tenantName, taskID); err != nil {
		t.Fatalf("cancel task: %v", err)
	}
}

func mustGetTaskStatus(t *testing.T, taskID uuid.UUID) string {
	t.Helper()
	db, err := orm.GetDB()
	if err != nil {
		t.Fatalf("get db: %v", err)
	}
	task, err := task_orm.GetByID(testCtx, db, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	return task.Status
}

func mustGetShardCount(t *testing.T, taskID uuid.UUID) int {
	t.Helper()
	db, err := orm.GetDB()
	if err != nil {
		t.Fatalf("get db: %v", err)
	}
	var count int64
	db.WithContext(testCtx).Model(&model.TaskShard{}).Where("task_id = ?", taskID).Count(&count)
	return int(count)
}

// requireK8s skips the test if k8s access is not configured.
func requireK8s(t *testing.T) {
	t.Helper()
	requireEnv(t, "CLUSTER_KUBERNETES_KUBECONFIG_PATH", "CLUSTER_AGENT_IMAGE", "CLUSTER_AGENT_TOKEN")
}
