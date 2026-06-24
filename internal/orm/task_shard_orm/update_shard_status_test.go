package task_shard_orm

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestMarkSucceededFromRunning(t *testing.T) {
	ctx := context.Background()
	db := newTestDB(t)
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, model.TaskStatusRunning)
	mustCreateTaskShardWithID(t, ctx, db, shardID, taskID, model.ShardStatusRunning)

	if err := MarkSucceeded(ctx, db, shardID); err != nil {
		t.Fatalf("MarkSucceeded() error = %v", err)
	}

	shard, err := GetByID(ctx, db, shardID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if shard.Status != model.ShardStatusSucceeded {
		t.Fatalf("shard.Status = %s, want %s", shard.Status, model.ShardStatusSucceeded)
	}
	if shard.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want set")
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

func mustCreateTaskShardWithID(t *testing.T, ctx context.Context, db *gorm.DB, shardID, taskID uuid.UUID, status string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&model.TaskShard{
		BaseUUIDModel: model.BaseUUIDModel{ID: shardID},
		TaskID:        taskID,
		Status:        status,
	}).Error; err != nil {
		t.Fatalf("create task shard: %v", err)
	}
}
