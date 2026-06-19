package task_service

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func TestReportShardStatusLocksTaskBeforeUpdatingShard(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	shardID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusQueued,
		ShardCount:    1,
	})
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusLeased)

	hookCalled := false
	reportShardStatusAfterTaskLockHook = func(tx *gorm.DB, gotTaskID, gotShardID uuid.UUID) {
		hookCalled = true
		if gotTaskID != taskID {
			t.Errorf("hook taskID = %s, want %s", gotTaskID, taskID)
		}
		if gotShardID != shardID {
			t.Errorf("hook shardID = %s, want %s", gotShardID, shardID)
		}

		var task model.Task
		if err := tx.WithContext(ctx).
			Select("id", "status").
			Where("id = ?", taskID).
			First(&task).Error; err != nil {
			t.Errorf("hook load task: %v", err)
			return
		}
		if task.Status != model.TaskStatusQueued {
			t.Errorf("hook task status = %s, want %s", task.Status, model.TaskStatusQueued)
		}

		var shard model.TaskShard
		if err := tx.WithContext(ctx).
			Select("id", "status").
			Where("id = ?", shardID).
			First(&shard).Error; err != nil {
			t.Errorf("hook load shard: %v", err)
			return
		}
		if shard.Status != model.ShardStatusLeased {
			t.Errorf("hook shard status = %s, want %s", shard.Status, model.ShardStatusLeased)
		}
	}
	t.Cleanup(func() {
		reportShardStatusAfterTaskLockHook = nil
	})

	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusRunning,
	}); err != nil {
		t.Fatalf("ReportShardStatus() error = %v", err)
	}
	if !hookCalled {
		t.Fatal("reportShardStatusAfterTaskLockHook was not called")
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusRunning)
	}

	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		t.Fatalf("load shard: %v", err)
	}
	if shard.Status != model.ShardStatusRunning {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusRunning)
	}
}

func TestCancelTaskCancelsTaskBeforeShards(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	tenantName := "cancel-team"
	taskID := uuid.New()
	shardID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, tenantName)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    1,
	})
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning)

	hookCalled := false
	cancelTaskBeforeShardCancelHook = func(tx *gorm.DB, gotTaskID uuid.UUID) {
		hookCalled = true
		if gotTaskID != taskID {
			t.Errorf("hook taskID = %s, want %s", gotTaskID, taskID)
		}

		var task model.Task
		if err := tx.WithContext(ctx).
			Select("id", "status").
			Where("id = ?", taskID).
			First(&task).Error; err != nil {
			t.Errorf("hook load task: %v", err)
			return
		}
		if task.Status != model.TaskStatusCancelled {
			t.Errorf("hook task status = %s, want %s", task.Status, model.TaskStatusCancelled)
		}

		var shard model.TaskShard
		if err := tx.WithContext(ctx).
			Select("id", "status").
			Where("id = ?", shardID).
			First(&shard).Error; err != nil {
			t.Errorf("hook load shard: %v", err)
			return
		}
		if shard.Status != model.ShardStatusRunning {
			t.Errorf("hook shard status = %s, want %s", shard.Status, model.ShardStatusRunning)
		}
	}
	t.Cleanup(func() {
		cancelTaskBeforeShardCancelHook = nil
	})

	if err := CancelTask(ctx, tenantName, taskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}
	if !hookCalled {
		t.Fatal("cancelTaskBeforeShardCancelHook was not called")
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusCancelled {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusCancelled)
	}

	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		t.Fatalf("load shard: %v", err)
	}
	if shard.Status != model.ShardStatusCancelled {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusCancelled)
	}
}

func TestCancelTaskSetsFinishedAtAndClearsRuntimeFieldsForCancelledShards(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	tenantName := "cancel-runtime-team"
	taskID := uuid.New()
	shardID := uuid.New()
	startedAt := time.Now().UTC().Add(-time.Minute)
	mustCreateTenantWithName(t, ctx, db, tenantID, tenantName)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    1,
	})
	if err := db.WithContext(ctx).Create(&model.TaskShard{
		BaseUUIDModel:   model.BaseUUIDModel{ID: shardID},
		TaskID:          taskID,
		ShardIndex:      0,
		Status:          model.ShardStatusRunning,
		AssignedAgentID: "agent-1",
		StartedAt:       &startedAt,
		LastError:       "keep-me",
	}).Error; err != nil {
		t.Fatalf("create task shard: %v", err)
	}

	if err := CancelTask(ctx, tenantName, taskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}

	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		t.Fatalf("load shard: %v", err)
	}
	if shard.Status != model.ShardStatusCancelled {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusCancelled)
	}
	if shard.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want cancellation timestamp")
	}
	if shard.FinishedAt.Before(startedAt) {
		t.Fatalf("FinishedAt = %v, want after StartedAt %v", shard.FinishedAt, startedAt)
	}
	if shard.StartedAt != nil {
		t.Fatalf("StartedAt = %v, want nil", shard.StartedAt)
	}
	if shard.AssignedAgentID != "" {
		t.Fatalf("AssignedAgentID = %q, want empty", shard.AssignedAgentID)
	}
	if shard.LastError != "" {
		t.Fatalf("LastError = %q, want empty", shard.LastError)
	}
}

func TestCancelTaskIgnoresCallerContextAfterCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	tenantName := "cancel-post-commit-team"
	taskID := uuid.New()
	shardID := uuid.New()
	mustCreateTenantWithName(t, context.Background(), db, tenantID, tenantName)
	mustCreateTask(t, context.Background(), db, model.Task{
		BaseUUIDModel:     model.BaseUUIDModel{ID: taskID},
		TenantID:          tenantID,
		Status:            model.TaskStatusRunning,
		ShardCount:        1,
		CompletionHookURL: "http://example.test/hooks/task-cancelled",
	})
	if err := db.WithContext(context.Background()).Create(&model.TaskShard{
		BaseUUIDModel:   model.BaseUUIDModel{ID: shardID},
		TaskID:          taskID,
		ShardIndex:      0,
		Status:          model.ShardStatusRunning,
		AssignedAgentID: "agent-1",
	}).Error; err != nil {
		t.Fatalf("create task shard: %v", err)
	}

	restoreCancelTaskGlobals(t)

	hookCalled := false
	runHookDispatchAsync = func(fn func()) { fn() }
	hookDispatcher = func(hookCtx context.Context, payload CompletionHookPayload) error {
		hookCalled = true
		if payload.TaskID != taskID {
			t.Fatalf("payload.TaskID = %s, want %s", payload.TaskID, taskID)
		}
		if payload.Status != model.TaskStatusCancelled {
			t.Fatalf("payload.Status = %s, want %s", payload.Status, model.TaskStatusCancelled)
		}
		if payload.FinishedAt == nil {
			t.Fatal("payload.FinishedAt = nil, want cancellation timestamp")
		}
		if err := hookCtx.Err(); err != nil {
			t.Fatalf("hook ctx err = %v, want nil", err)
		}
		return nil
	}

	notifyCalled := false
	notifyCancelShard = func(notifyCtx context.Context, agentID string, gotShardID uuid.UUID) error {
		notifyCalled = true
		if agentID != "agent-1" {
			t.Fatalf("agentID = %s, want agent-1", agentID)
		}
		if gotShardID != shardID {
			t.Fatalf("shardID = %s, want %s", gotShardID, shardID)
		}
		if err := notifyCtx.Err(); err != nil {
			t.Fatalf("notify ctx err = %v, want nil", err)
		}
		return nil
	}

	cancelTaskAfterCommitHook = func(gotTaskID uuid.UUID) {
		if gotTaskID != taskID {
			t.Fatalf("hook taskID = %s, want %s", gotTaskID, taskID)
		}
		cancel()
	}

	if err := CancelTask(ctx, tenantName, taskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}
	if !hookCalled {
		t.Fatal("completion hook was not dispatched")
	}
	if !notifyCalled {
		t.Fatal("notifyCancelShard was not called")
	}

	var task model.Task
	if err := db.WithContext(context.Background()).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusCancelled {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusCancelled)
	}
	if task.FinishedAt == nil {
		t.Fatal("task.FinishedAt = nil, want cancellation timestamp")
	}
}

func restoreCancelTaskGlobals(t *testing.T) {
	t.Helper()

	prevBeforeShardHook := cancelTaskBeforeShardCancelHook
	prevAfterCommitHook := cancelTaskAfterCommitHook
	prevNotify := notifyCancelShard
	prevDispatcher := hookDispatcher
	prevAsync := runHookDispatchAsync
	prevPostCommitTimeout := cancelTaskPostCommitTimeout
	t.Cleanup(func() {
		cancelTaskBeforeShardCancelHook = prevBeforeShardHook
		cancelTaskAfterCommitHook = prevAfterCommitHook
		notifyCancelShard = prevNotify
		hookDispatcher = prevDispatcher
		runHookDispatchAsync = prevAsync
		cancelTaskPostCommitTimeout = prevPostCommitTimeout
	})
}
