package task_service

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

func setupTaskServiceTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", uuid.NewString())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	if err := orm.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	orm.SetTestDB(db)

	t.Cleanup(func() {
		orm.SetTestDB(nil)
		sqlDB, err := db.DB()
		if err == nil {
			_ = sqlDB.Close()
		}
	})

	return db
}

func mustCreateTenant(t *testing.T, ctx context.Context, db *gorm.DB, tenantID uuid.UUID) {
	t.Helper()
	mustCreateTenantWithName(t, ctx, db, tenantID, "test-tenant-"+tenantID.String()[:8])
}

func mustCreateTenantWithName(t *testing.T, ctx context.Context, db *gorm.DB, tenantID uuid.UUID, name string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&model.Tenant{
		BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
		Name:               name,
		PrimaryBackendName: "ray",
		MaxAgents:          2,
	}).Error; err != nil {
		t.Fatalf("create tenant: %v", err)
	}
}

func mustCreateTask(t *testing.T, ctx context.Context, db *gorm.DB, task model.Task) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&task).Error; err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func mustCreateTaskShard(t *testing.T, ctx context.Context, db *gorm.DB, taskID uuid.UUID, status string) {
	t.Helper()
	mustCreateTaskShardWithID(t, ctx, db, uuid.New(), taskID, status)
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
