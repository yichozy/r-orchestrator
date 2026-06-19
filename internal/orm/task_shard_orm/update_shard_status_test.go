package task_shard_orm

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestUpdateShardStatusAllowsResultReadyTransition(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, model.TaskStatusRunning)
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning)

	now := time.Now().UTC()
	err := UpdateShardStatus(ctx, db, UpdateShardStatusParams{
		ShardID:         shardID,
		Status:          model.ShardStatusResultReady,
		CurrentStatuses: []string{model.ShardStatusRunning},
		FinishedAt:      &now,
	})
	if err != nil {
		t.Fatalf("UpdateShardStatus() error = %v", err)
	}

	shard, err := GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if shard.Status != model.ShardStatusResultReady {
		t.Fatalf("shard.Status = %s, want %s", shard.Status, model.ShardStatusResultReady)
	}
}

func TestUpdateShardStatusAllowsTerminalTransitionDuringMigration(t *testing.T) {
	tests := []struct {
		name          string
		initialStatus string
	}{
		{
			name:          "from running",
			initialStatus: model.ShardStatusRunning,
		},
		{
			name:          "from result ready",
			initialStatus: model.ShardStatusResultReady,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			db := newTestDB(t)
			taskID := uuid.Must(uuid.NewV7())
			shardID := uuid.Must(uuid.NewV7())

			mustCreateTask(t, ctx, db, taskID, model.TaskStatusRunning)
			mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, tt.initialStatus)

			now := time.Now().UTC()
			err := UpdateShardStatus(ctx, db, UpdateShardStatusParams{
				ShardID:    shardID,
				Status:     model.ShardStatusSucceeded,
				FinishedAt: &now,
			})
			if err != nil {
				t.Fatalf("UpdateShardStatus() error = %v", err)
			}

			shard, err := GetByID(ctx, db, shardID)
			if err != nil {
				t.Fatalf("GetByID() error = %v", err)
			}
			if shard.Status != model.ShardStatusSucceeded {
				t.Fatalf("shard.Status = %s, want %s", shard.Status, model.ShardStatusSucceeded)
			}
		})
	}
}

func TestRollbackStaleShardsClearsExecutionTimestampsForResultReadyShard(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, model.TaskStatusRunning)
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, 0, model.ShardStatusResultReady)

	startedAt := time.Now().UTC().Add(-10 * time.Minute)
	finishedAt := time.Now().UTC().Add(-5 * time.Minute)
	if err := db.WithContext(ctx).Model(&model.TaskShard{}).
		Where("id = ?", shardID).
		Updates(map[string]any{
			"assigned_agent_id": "agent-1",
			"started_at":        startedAt,
			"finished_at":       finishedAt,
			"last_error":        "stale result",
			"updated_at":        time.Now().UTC().Add(-2 * time.Hour),
		}).Error; err != nil {
		t.Fatalf("prime stale result-ready shard: %v", err)
	}

	rolled, err := RollbackStaleShards(ctx, db, nil, time.Now().UTC().Add(-30*time.Minute))
	if err != nil {
		t.Fatalf("RollbackStaleShards() error = %v", err)
	}
	if rolled != 1 {
		t.Fatalf("RollbackStaleShards() rolled = %d, want 1", rolled)
	}

	shard, err := GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if shard.Status != model.ShardStatusQueued {
		t.Fatalf("shard status = %s, want %s", shard.Status, model.ShardStatusQueued)
	}
	if shard.AssignedAgentID != "" {
		t.Fatalf("AssignedAgentID = %q, want empty", shard.AssignedAgentID)
	}
	if shard.StartedAt != nil {
		t.Fatalf("StartedAt = %v, want nil", shard.StartedAt)
	}
	if shard.FinishedAt != nil {
		t.Fatalf("FinishedAt = %v, want nil", shard.FinishedAt)
	}
	if shard.LastError != "" {
		t.Fatalf("LastError = %q, want empty", shard.LastError)
	}
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_busy_timeout=5000", uuid.NewString())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	if err := orm.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB() error = %v", err)
	}
	sqlDB.SetMaxOpenConns(10)
	sqlDB.SetMaxIdleConns(10)

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	return db
}

func mustCreateTask(t *testing.T, ctx context.Context, db *gorm.DB, taskID uuid.UUID, status string) {
	t.Helper()

	tenantID := uuid.New()
	if err := db.WithContext(ctx).Create(&model.Tenant{
		BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
		PrimaryBackendName: "ray",
		MaxAgents:          1,
	}).Error; err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	if err := db.WithContext(ctx).Create(&model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        status,
		ShardCount:    1,
	}).Error; err != nil {
		t.Fatalf("create task: %v", err)
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
