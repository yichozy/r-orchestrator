package task_service

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
)

func TestRecomputeTaskStatusMarksTaskSucceededWhenAllShardsSucceeded(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)

	payload, err := RecomputeTaskStatus(ctx, db, taskID, "")
	if err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}
	if payload != nil && payload.CompletionHookURL != "" {
		t.Fatalf("unexpected completion hook payload when no hook URL set")
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
	if task.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want set")
	}
}

func TestRecomputeTaskStatusMarksTaskFailedWhenAnyShardFailed(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})
	failedShardError := "runtime error"
	if err := db.WithContext(ctx).Create(&model.TaskShard{
		BaseUUIDModel: model.BaseUUIDModel{ID: uuid.New()},
		TaskID:        taskID,
		Status:        model.ShardStatusFailed,
		LastError:     failedShardError,
	}).Error; err != nil {
		t.Fatalf("create failed shard: %v", err)
	}
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusFailed)
	}
	if task.LastError != failedShardError {
		t.Fatalf("LastError = %q, want %q", task.LastError, failedShardError)
	}
}

func TestRecomputeTaskStatusDoesNothingWhenActiveShardsRemain(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusRunning)

	payload, err := RecomputeTaskStatus(ctx, db, taskID, "")
	if err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}
	if payload != nil {
		t.Fatalf("expected nil payload when shards still active, got %v", payload)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusRunning)
	}
}

func TestRecomputeTaskStatusReturnsNilWhenTaskAlreadyTerminal(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusSucceeded,
		ShardCount:    1,
	})

	payload, err := RecomputeTaskStatus(ctx, db, taskID, "")
	if err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}
	if payload != nil {
		t.Fatalf("expected nil payload when task already succeeded, got %v", payload)
	}
}

func TestRecomputeTaskStatusCompetingSuccessIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() first call error = %v", err)
	}
	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() second call error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
}

func TestReportShardStatusTransitionsToSucceededWithOSSOutput(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	shardID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    1,
	})
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, model.ShardStatusRunning)

	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:      shardID,
		ShardStatus:  model.ShardStatusSucceeded,
		OutputOSSKey: "tasks/abc/shards/run1/output.zip",
		OutputSHA256: "sha256abc",
	}); err != nil {
		t.Fatalf("ReportShardStatus() error = %v", err)
	}

	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		t.Fatalf("load shard: %v", err)
	}
	if shard.Status != model.ShardStatusSucceeded {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusSucceeded)
	}
	if shard.OutputOSSKey != "tasks/abc/shards/run1/output.zip" {
		t.Fatalf("shard OutputOSSKey = %q, want %q", shard.OutputOSSKey, "tasks/abc/shards/run1/output.zip")
	}
	
	if shard.OutputSHA256 != "sha256abc" {
		t.Fatalf("shard OutputSHA256 = %q, want %q", shard.OutputSHA256, "sha256abc")
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
}

func TestReportShardStatusSkipsDuplicateSucceededWithOSSKey(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	shardID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusSucceeded,
		ShardCount:    1,
	})
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, model.ShardStatusSucceeded)
	if err := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("id = ?", shardID).
		Update("output_oss_key", "tasks/abc/shards/run1/output.zip").Error; err != nil {
		t.Fatalf("set output_oss_key: %v", err)
	}

	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:      shardID,
		ShardStatus:  model.ShardStatusSucceeded,
		OutputOSSKey: "tasks/abc/shards/run1/output2.zip",
	}); err != nil {
		t.Fatalf("ReportShardStatus() error = %v", err)
	}

	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		t.Fatalf("load shard: %v", err)
	}
	if shard.OutputOSSKey != "tasks/abc/shards/run1/output.zip" {
		t.Fatalf("shard OutputOSSKey = %q, want original %q", shard.OutputOSSKey, "tasks/abc/shards/run1/output.zip")
	}
}

func TestRecomputeTaskStatusPreservesFailedShardLastError(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})

	failedShardError := "runtime error"
	if err := db.WithContext(ctx).Create(&model.TaskShard{
		BaseUUIDModel: model.BaseUUIDModel{ID: uuid.New()},
		TaskID:        taskID,
		Status:        model.ShardStatusFailed,
		LastError:     failedShardError,
	}).Error; err != nil {
		t.Fatalf("create failed shard: %v", err)
	}
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusSucceeded)

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusFailed)
	}
	if task.LastError != failedShardError {
		t.Fatalf("LastError = %q, want %q", task.LastError, failedShardError)
	}
}

func TestRecomputeTaskStatusUsesDefaultErrorWhenShardHasNoErrorAndNoFallback(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    1,
	})
	mustCreateTaskShard(t, ctx, db, taskID, model.ShardStatusFailed)

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusFailed)
	}
	if !strings.Contains(task.LastError, "one or more shards failed") {
		t.Fatalf("LastError = %q, want default failure message", task.LastError)
	}
}
