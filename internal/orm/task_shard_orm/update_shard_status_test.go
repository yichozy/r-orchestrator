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

	now := time.Now()
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

			now := time.Now()
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
