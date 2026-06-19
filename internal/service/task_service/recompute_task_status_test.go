package task_service

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/util"
	"gorm.io/gorm"
)

func TestRecomputeTaskStatusAggregatesShardOutputsIntoTaskArtifact(t *testing.T) {
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
	mustCreateTaskShard(t, ctx, db, taskID, 0, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, 1, model.ShardStatusSucceeded)
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 0, []byte("id,value\n1,a\n"))
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 1, []byte("id,value\n2,b\n"))

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
	if task.ResultArtifactID == nil {
		t.Fatalf("ResultArtifactID = nil, want aggregated artifact")
	}

	artifact, err := artifact_orm.GetById(ctx, db, *task.ResultArtifactID)
	if err != nil {
		t.Fatalf("GetById() error = %v", err)
	}
	if artifact.ArtifactType != model.ArtifactTypeTaskOutput {
		t.Fatalf("ArtifactType = %s, want %s", artifact.ArtifactType, model.ArtifactTypeTaskOutput)
	}
	const wantCSV = "id,value\n1,a\n2,b\n"
	if string(artifact.ContentBytes) != wantCSV {
		t.Fatalf("ContentBytes = %q, want %q", string(artifact.ContentBytes), wantCSV)
	}
}

func TestRecomputeTaskStatusFailsWhenShardOutputArtifactsMissing(t *testing.T) {
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
	mustCreateTaskShard(t, ctx, db, taskID, 0, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, 1, model.ShardStatusSucceeded)
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 0, []byte("id,value\n1,a\n"))

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
	if !strings.Contains(task.LastError, "missing shard output artifact for shard_index") {
		t.Fatalf("LastError = %q, want missing shard output artifact for shard_index", task.LastError)
	}
	if task.ResultArtifactID != nil {
		t.Fatalf("ResultArtifactID = %v, want nil", *task.ResultArtifactID)
	}
}

func TestRecomputeTaskStatusCompetingSuccessCreatesSingleTaskArtifact(t *testing.T) {
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
	mustCreateTaskShard(t, ctx, db, taskID, 0, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, 1, model.ShardStatusSucceeded)
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 0, []byte("id,value\n1,a\n"))
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 1, []byte("id,value\n2,b\n"))

	var competingErr error
	injected := false
	recomputeTaskStatusBeforeTerminalUpdateHook = func(taskID uuid.UUID) {
		if injected {
			return
		}
		injected = true
		_, competingErr = RecomputeTaskStatus(ctx, db, taskID, "")
	}
	defer func() {
		recomputeTaskStatusBeforeTerminalUpdateHook = nil
	}()

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() outer error = %v", err)
	}
	if competingErr != nil {
		t.Fatalf("RecomputeTaskStatus() competing error = %v", competingErr)
	}

	artifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeTaskOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() error = %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("task output artifact count = %d, want 1", len(artifacts))
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
	if task.ResultArtifactID == nil {
		t.Fatal("ResultArtifactID = nil, want task output artifact")
	}
	if *task.ResultArtifactID != artifacts[0].ID {
		t.Fatalf("ResultArtifactID = %s, want %s", *task.ResultArtifactID, artifacts[0].ID)
	}
}

func TestRecomputeTaskStatusCreatesTaskArtifactBeforeMarkingTaskSucceeded(t *testing.T) {
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
	mustCreateTaskShard(t, ctx, db, taskID, 0, model.ShardStatusSucceeded)
	mustCreateTaskShard(t, ctx, db, taskID, 1, model.ShardStatusSucceeded)
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 0, []byte("id,value\n1,a\n"))
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 1, []byte("id,value\n2,b\n"))

	hookCalled := false
	recomputeTaskStatusBeforeTerminalUpdateHook = func(taskID uuid.UUID) {
		hookCalled = true

		var task model.Task
		if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
			t.Fatalf("load task in hook: %v", err)
		}
		if task.Status != model.TaskStatusRunning {
			t.Fatalf("task status in hook = %s, want %s", task.Status, model.TaskStatusRunning)
		}
		if task.ResultArtifactID != nil {
			t.Fatalf("ResultArtifactID in hook = %s, want nil", *task.ResultArtifactID)
		}

		artifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeTaskOutput)
		if err != nil {
			t.Fatalf("ListByTaskAndType() in hook error = %v", err)
		}
		if len(artifacts) != 1 {
			t.Fatalf("task output artifact count in hook = %d, want 1", len(artifacts))
		}
	}
	defer func() {
		recomputeTaskStatusBeforeTerminalUpdateHook = nil
	}()

	if _, err := RecomputeTaskStatus(ctx, db, taskID, ""); err != nil {
		t.Fatalf("RecomputeTaskStatus() error = %v", err)
	}
	if !hookCalled {
		t.Fatal("recomputeTaskStatusBeforeTerminalUpdateHook was not called")
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
	if task.ResultArtifactID == nil {
		t.Fatal("ResultArtifactID = nil, want task output artifact")
	}

	artifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeTaskOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() error = %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("task output artifact count = %d, want 1", len(artifacts))
	}
	if *task.ResultArtifactID != artifacts[0].ID {
		t.Fatalf("ResultArtifactID = %s, want %s", *task.ResultArtifactID, artifacts[0].ID)
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
		ShardIndex:    0,
		Status:        model.ShardStatusFailed,
		LastError:     failedShardError,
	}).Error; err != nil {
		t.Fatalf("create failed shard: %v", err)
	}
	mustCreateTaskShard(t, ctx, db, taskID, 1, model.ShardStatusSucceeded)

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

func TestReportShardStatusFailsTaskWhenAnySuccessfulShardOutputIsEmpty(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	firstShardID := uuid.New()
	secondShardID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})
	mustCreateTaskShardWithID(t, ctx, db, firstShardID, taskID, 0, model.ShardStatusRunning)
	mustCreateTaskShardWithID(t, ctx, db, secondShardID, taskID, 1, model.ShardStatusRunning)

	nonEmptyOutput := []byte("id,value\n1,a\n")
	mustReportShardResultReady(t, ctx, firstShardID)
	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     firstShardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   nonEmptyOutput,
	}); err != nil {
		t.Fatalf("ReportShardStatus() first shard error = %v", err)
	}
	mustReportShardResultReady(t, ctx, secondShardID)
	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     secondShardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   nil,
	}); err != nil {
		t.Fatalf("ReportShardStatus() empty-output shard error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusFailed)
	}
	if !strings.Contains(task.LastError, "empty shard output for shard_index 1") {
		t.Fatalf("LastError = %q, want empty shard output for shard_index 1", task.LastError)
	}
	if task.ResultArtifactID != nil {
		t.Fatalf("ResultArtifactID = %v, want nil", *task.ResultArtifactID)
	}

	shardArtifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeShardOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() shard outputs error = %v", err)
	}
	if len(shardArtifacts) != 2 {
		t.Fatalf("shard output artifact count = %d, want 2", len(shardArtifacts))
	}

	emptyArtifactCount := 0
	for _, artifact := range shardArtifacts {
		if len(artifact.ContentBytes) == 0 {
			emptyArtifactCount++
		}
	}
	if emptyArtifactCount != 1 {
		t.Fatalf("empty shard output artifact count = %d, want 1", emptyArtifactCount)
	}
}

func TestReportShardStatusUsesResultReadyBeforeSucceeded(t *testing.T) {
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
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning)

	mustReportShardResultReady(t, ctx, shardID)

	shard, err := task_shard_orm.GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() shard after result ready error = %v", err)
	}
	if shard.Status != model.ShardStatusResultReady {
		t.Fatalf("shard status after result ready = %s, want %s", shard.Status, model.ShardStatusResultReady)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task after result ready: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status after result ready = %s, want %s", task.Status, model.TaskStatusRunning)
	}

	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   []byte("id,value\n1,a\n"),
	}); err != nil {
		t.Fatalf("ReportShardStatus() succeeded error = %v", err)
	}

	shard, err = task_shard_orm.GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() shard after success error = %v", err)
	}
	if shard.Status != model.ShardStatusSucceeded {
		t.Fatalf("shard status after success = %s, want %s", shard.Status, model.ShardStatusSucceeded)
	}
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task after success: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status after success = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
}

func TestReportShardStatusFailsTaskWhenAllSuccessfulShardOutputsAreEmpty(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	firstShardID := uuid.New()
	secondShardID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusRunning,
		ShardCount:    2,
	})
	mustCreateTaskShardWithID(t, ctx, db, firstShardID, taskID, 0, model.ShardStatusRunning)
	mustCreateTaskShardWithID(t, ctx, db, secondShardID, taskID, 1, model.ShardStatusRunning)

	mustReportShardResultReady(t, ctx, firstShardID)
	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     firstShardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   nil,
	}); err != nil {
		t.Fatalf("ReportShardStatus() first empty-output shard error = %v", err)
	}
	mustReportShardResultReady(t, ctx, secondShardID)
	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     secondShardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   []byte{},
	}); err != nil {
		t.Fatalf("ReportShardStatus() second empty-output shard error = %v", err)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusFailed)
	}
	if !strings.Contains(task.LastError, "empty shard output for shard_index 0") {
		t.Fatalf("LastError = %q, want empty shard output for shard_index 0", task.LastError)
	}
	if task.ResultArtifactID != nil {
		t.Fatalf("ResultArtifactID = %v, want nil", *task.ResultArtifactID)
	}
}

func TestReportShardStatusRollsBackShardOutputArtifactOnStorageFailure(t *testing.T) {
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
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning)

	outputCSV := []byte("id,value\n1,a\n")
	storeErr := errors.New("injected shard output failure")
	mustReportShardResultReady(t, ctx, shardID)
	err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   outputCSV,
		StoreShardOutputFn: func(ctx context.Context, tx *gorm.DB, task model.Task, shard model.TaskShard, outputCSV []byte) error {
			shardIndex := shard.ShardIndex
			if err := artifact_orm.Create(ctx, tx, model.Artifact{
				BaseUUIDModel: model.BaseUUIDModel{ID: uuid.New()},
				TenantID:      task.TenantID,
				TaskID:        task.ID,
				ArtifactType:  model.ArtifactTypeShardOutput,
				ContentBytes:  append([]byte(nil), outputCSV...),
				ContentSize:   int64(len(outputCSV)),
				SHA256:        util.SumSHA256(outputCSV),
				ShardIndex:    &shardIndex,
			}); err != nil {
				return err
			}
			return storeErr
		},
	})
	if err == nil || !strings.Contains(err.Error(), "store shard output") || !strings.Contains(err.Error(), storeErr.Error()) {
		t.Fatalf("ReportShardStatus() error = %v, want wrapped store shard output failure", err)
	}

	shard, err := task_shard_orm.GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() shard error = %v", err)
	}
	if shard.Status != model.ShardStatusResultReady {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusResultReady)
	}

	var task model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&task).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusRunning)
	}
	if task.ResultArtifactID != nil {
		t.Fatalf("ResultArtifactID = %s, want nil", *task.ResultArtifactID)
	}

	shardArtifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeShardOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() shard outputs error = %v", err)
	}
	if len(shardArtifacts) != 0 {
		t.Fatalf("shard output artifact count = %d, want 0", len(shardArtifacts))
	}
	taskArtifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeTaskOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() task outputs error = %v", err)
	}
	if len(taskArtifacts) != 0 {
		t.Fatalf("task output artifact count = %d, want 0", len(taskArtifacts))
	}
}

func TestReportShardStatusAllowsDuplicateSucceededWhenShardOutputExists(t *testing.T) {
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
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusSucceeded)
	mustCreateShardOutputArtifact(t, ctx, db, tenantID, taskID, 0, []byte("id,value\n1,a\n"))

	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   []byte("id,value\n1,a\n"),
	}); err != nil {
		t.Fatalf("ReportShardStatus() duplicate succeeded error = %v", err)
	}

	shard, err := task_shard_orm.GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() shard error = %v", err)
	}
	if shard.Status != model.ShardStatusSucceeded {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusSucceeded)
	}

	shardArtifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeShardOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() shard outputs error = %v", err)
	}
	if len(shardArtifacts) != 1 {
		t.Fatalf("shard output artifact count = %d, want 1", len(shardArtifacts))
	}
}

func mustCreateTaskShard(t *testing.T, ctx context.Context, db *gorm.DB, taskID uuid.UUID, shardIndex int, status string) {
	t.Helper()
	mustCreateTaskShardWithID(t, ctx, db, uuid.New(), taskID, shardIndex, status)
}

func mustReportShardResultReady(t *testing.T, ctx context.Context, shardID uuid.UUID) {
	t.Helper()

	if err := ReportShardStatus(ctx, ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusResultReady,
	}); err != nil {
		t.Fatalf("ReportShardStatus() result ready error = %v", err)
	}
}

func mustCreateTaskShardWithID(t *testing.T, ctx context.Context, db *gorm.DB, shardID, taskID uuid.UUID, shardIndex int, status string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&model.TaskShard{
		BaseUUIDModel: model.BaseUUIDModel{ID: shardID},
		TaskID:        taskID,
		ShardIndex:    shardIndex,
		Status:        status,
	}).Error; err != nil {
		t.Fatalf("create task shard: %v", err)
	}
}

func mustCreateShardOutputArtifact(t *testing.T, ctx context.Context, db *gorm.DB, tenantID, taskID uuid.UUID, shardIndex int, content []byte) {
	t.Helper()

	contentCopy := append([]byte(nil), content...)
	mustCreateArtifact(t, ctx, db, model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: uuid.New()},
		TenantID:      tenantID,
		TaskID:        taskID,
		ArtifactType:  model.ArtifactTypeShardOutput,
		ContentBytes:  contentCopy,
		ContentSize:   int64(len(contentCopy)),
		SHA256:        util.SumSHA256(contentCopy),
		ShardIndex:    &shardIndex,
	})
}
