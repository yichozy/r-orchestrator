package task_service

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestGetTaskReturnsErrTaskNotFoundForWrongTenant(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	taskTenantID := uuid.New()
	callerTenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, taskTenantID, "task-team")
	mustCreateTenantWithName(t, ctx, db, callerTenantID, "caller-team")

	taskID := uuid.New()
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      taskTenantID,
		Status:        model.TaskStatusPending,
		ShardCount:    1,
	})

	_, err := GetTask(ctx, "caller-team", taskID)
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetTask() error = %v, want ErrTaskNotFound", err)
	}
}

func TestGetTaskReturnsErrTenantNotFoundForUnknownTenantName(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	taskTenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, taskTenantID, "task-team")

	taskID := uuid.New()
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      taskTenantID,
		Status:        model.TaskStatusPending,
		ShardCount:    1,
	})

	_, err := GetTask(ctx, "missing-team", taskID)
	if !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("GetTask() error = %v, want ErrTenantNotFound", err)
	}
}

func TestGetTaskResultCSVReturnsErrTaskNotFoundForMissingTask(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, "team-alpha")

	_, err := GetTaskResultCSV(ctx, "team-alpha", uuid.New())
	if !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("GetTaskResultCSV() error = %v, want ErrTaskNotFound", err)
	}
}

func TestGetTaskResultCSVReturnsErrTaskNotSucceededForPendingTask(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, "team-alpha")

	taskID := uuid.New()
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusPending,
		ShardCount:    1,
	})

	_, err := GetTaskResultCSV(ctx, "team-alpha", taskID)
	if !errors.Is(err, ErrTaskNotSucceeded) {
		t.Fatalf("GetTaskResultCSV() error = %v, want ErrTaskNotSucceeded", err)
	}
}

func TestGetTaskResultCSVReturnsErrTaskResultNotFoundWhenArtifactMissing(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, "team-alpha")

	missingArtifactID := uuid.New()
	taskID := uuid.New()
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel:    model.BaseUUIDModel{ID: taskID},
		TenantID:         tenantID,
		Status:           model.TaskStatusSucceeded,
		ResultArtifactID: &missingArtifactID,
		ShardCount:       1,
	})

	_, err := GetTaskResultCSV(ctx, "team-alpha", taskID)
	if !errors.Is(err, ErrTaskResultNotFound) {
		t.Fatalf("GetTaskResultCSV() error = %v, want ErrTaskResultNotFound", err)
	}
}

func TestGetTaskResultCSVReturnsErrTaskResultNotFoundForInvalidArtifactGuard(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, "team-alpha")

	artifactID := uuid.New()
	taskID := uuid.New()
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel:    model.BaseUUIDModel{ID: taskID},
		TenantID:         tenantID,
		Status:           model.TaskStatusSucceeded,
		ResultArtifactID: &artifactID,
		ShardCount:       1,
	})
	mustCreateArtifact(t, ctx, db, model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: artifactID},
		TenantID:      tenantID,
		TaskID:        taskID,
		ArtifactType:  model.ArtifactTypeShardOutput,
		ContentBytes:  []byte("id,value\n1,a\n"),
		ContentSize:   int64(len("id,value\n1,a\n")),
		SHA256:        "sha",
	})

	_, err := GetTaskResultCSV(ctx, "team-alpha", taskID)
	if !errors.Is(err, ErrTaskResultNotFound) {
		t.Fatalf("GetTaskResultCSV() error = %v, want ErrTaskResultNotFound", err)
	}
}

func TestGetTaskResultCSVReturnsErrTenantNotFoundForUnknownTenantName(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, "team-alpha")

	taskID := uuid.New()
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        model.TaskStatusSucceeded,
		ShardCount:    1,
	})

	_, err := GetTaskResultCSV(ctx, "missing-team", taskID)
	if !errors.Is(err, ErrTenantNotFound) {
		t.Fatalf("GetTaskResultCSV() error = %v, want ErrTenantNotFound", err)
	}
}

func setupTaskServiceTestDB(t *testing.T) *gorm.DB {
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
	orm.SetTestDB(db)

	t.Cleanup(func() {
		orm.SetTestDB(nil)
		_ = sqlDB.Close()
	})

	return db
}

func mustCreateTenant(t *testing.T, ctx context.Context, db *gorm.DB, tenantID uuid.UUID) {
	t.Helper()

	mustCreateTenantWithName(t, ctx, db, tenantID, tenantID.String())
}

func mustCreateTenantWithName(t *testing.T, ctx context.Context, db *gorm.DB, tenantID uuid.UUID, tenantName string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&model.Tenant{
		BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
		Name:               tenantName,
		PrimaryBackendName: "ray",
		MaxAgents:          1,
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

func mustCreateArtifact(t *testing.T, ctx context.Context, db *gorm.DB, artifact model.Artifact) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&artifact).Error; err != nil {
		t.Fatalf("create artifact: %v", err)
	}
}

func TestTenantORMCreateNormalizesNameToLowercase(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenant, err := tenant_orm.Create(ctx, db, model.Tenant{
		Name:               " Team-Alpha ",
		PrimaryBackendName: "ray",
		MaxAgents:          2,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if tenant.Name != "team-alpha" {
		t.Fatalf("tenant.Name = %q, want %q", tenant.Name, "team-alpha")
	}
}

func TestTenantORMGetByNameMatchesCaseInsensitiveInput(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	created, err := tenant_orm.Create(ctx, db, model.Tenant{
		Name:               "team-alpha",
		PrimaryBackendName: "ray",
		MaxAgents:          2,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := tenant_orm.GetByName(ctx, db, " Team-Alpha ")
	if err != nil {
		t.Fatalf("GetByName() error = %v", err)
	}
	if got.ID != created.ID {
		t.Fatalf("got.ID = %s, want %s", got.ID, created.ID)
	}
}
