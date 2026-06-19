package graph

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	"github.com/google/uuid"
	gqlmodel "github.com/yichozy/r-orchestrator/graph/model"
	imodel "github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestQueryResolverGetTaskResultCSVMapsFields(t *testing.T) {
	ctx := context.Background()
	db := setupGraphResolverTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	artifactID := uuid.New()
	mustCreateGraphTenant(t, ctx, db, tenantID, "team-alpha")
	mustCreateGraphTask(t, ctx, db, imodel.Task{
		BaseUUIDModel:    imodel.BaseUUIDModel{ID: taskID},
		TenantID:         tenantID,
		Status:           imodel.TaskStatusSucceeded,
		ResultArtifactID: &artifactID,
		ShardCount:       1,
	})
	mustCreateGraphArtifact(t, ctx, db, imodel.Artifact{
		BaseUUIDModel: imodel.BaseUUIDModel{ID: artifactID},
		TenantID:      tenantID,
		TaskID:        taskID,
		ArtifactType:  imodel.ArtifactTypeTaskOutput,
		ContentBytes:  []byte("id,value\n1,a\n"),
		ContentSize:   int64(len("id,value\n1,a\n")),
		SHA256:        "sha",
	})

	got, err := (&queryResolver{&Resolver{}}).GetTaskResultCSV(ctx, "team-alpha", taskID)
	if err != nil {
		t.Fatalf("GetTaskResultCSV() error = %v", err)
	}

	want := &gqlmodel.TaskResultCSV{
		TaskID:      taskID,
		Filename:    fmt.Sprintf("task-%s-result.csv", taskID),
		ContentType: "text/csv; charset=utf-8",
		CSVContent:  "id,value\n1,a\n",
	}
	if *got != *want {
		t.Fatalf("GetTaskResultCSV() = %#v, want %#v", got, want)
	}
}

func TestQueryResolverGetTaskByIDPropagatesTaskNotFound(t *testing.T) {
	ctx := context.Background()
	db := setupGraphResolverTestDB(t)

	taskTenantID := uuid.New()
	callerTenantID := uuid.New()
	taskID := uuid.New()
	mustCreateGraphTenant(t, ctx, db, taskTenantID, "task-team")
	mustCreateGraphTenant(t, ctx, db, callerTenantID, "caller-team")
	mustCreateGraphTask(t, ctx, db, imodel.Task{
		BaseUUIDModel: imodel.BaseUUIDModel{ID: taskID},
		TenantID:      taskTenantID,
		Status:        imodel.TaskStatusPending,
		ShardCount:    1,
	})

	_, err := (&queryResolver{&Resolver{}}).GetTaskByID(ctx, "caller-team", taskID)
	if !errors.Is(err, task_service.ErrTaskNotFound) {
		t.Fatalf("GetTaskByID() error = %v, want ErrTaskNotFound", err)
	}
}

func TestQueryResolverGetTaskListFiltersByTenantName(t *testing.T) {
	ctx := context.Background()
	db := setupGraphResolverTestDB(t)

	alphaTenantID := uuid.New()
	betaTenantID := uuid.New()
	alphaRunningTaskID := uuid.New()
	alphaPendingTaskID := uuid.New()
	betaRunningTaskID := uuid.New()

	mustCreateGraphTenant(t, ctx, db, alphaTenantID, "team-alpha")
	mustCreateGraphTenant(t, ctx, db, betaTenantID, "team-beta")
	mustCreateGraphTask(t, ctx, db, imodel.Task{
		BaseUUIDModel: imodel.BaseUUIDModel{ID: alphaPendingTaskID},
		TenantID:      alphaTenantID,
		Status:        imodel.TaskStatusPending,
		ShardCount:    1,
	})
	mustCreateGraphTask(t, ctx, db, imodel.Task{
		BaseUUIDModel: imodel.BaseUUIDModel{ID: alphaRunningTaskID},
		TenantID:      alphaTenantID,
		Status:        imodel.TaskStatusRunning,
		ShardCount:    2,
	})
	mustCreateGraphTask(t, ctx, db, imodel.Task{
		BaseUUIDModel: imodel.BaseUUIDModel{ID: betaRunningTaskID},
		TenantID:      betaTenantID,
		Status:        imodel.TaskStatusRunning,
		ShardCount:    3,
	})

	status := imodel.TaskStatusRunning
	got, err := (&queryResolver{&Resolver{}}).GetTaskList(ctx, "team-alpha", &status)
	if err != nil {
		t.Fatalf("GetTaskList() error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(GetTaskList()) = %d, want %d", len(got), 1)
	}
	if got[0].ID != alphaRunningTaskID {
		t.Fatalf("GetTaskList()[0].ID = %s, want %s", got[0].ID, alphaRunningTaskID)
	}
	if got[0].TenantName != "team-alpha" {
		t.Fatalf("GetTaskList()[0].TenantName = %q, want %q", got[0].TenantName, "team-alpha")
	}
}

func TestMutationResolverCancelTaskUsesTenantNamePath(t *testing.T) {
	ctx := context.Background()
	db := setupGraphResolverTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateGraphTenant(t, ctx, db, tenantID, "team-alpha")
	mustCreateGraphTask(t, ctx, db, imodel.Task{
		BaseUUIDModel: imodel.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        imodel.TaskStatusPending,
		ShardCount:    1,
	})

	got, err := (&mutationResolver{&Resolver{}}).CancelTask(ctx, "team-alpha", taskID)
	if err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}
	if got.TaskID != taskID {
		t.Fatalf("CancelTask().TaskID = %s, want %s", got.TaskID, taskID)
	}
	if got.Status != imodel.TaskStatusCancelled {
		t.Fatalf("CancelTask().Status = %q, want %q", got.Status, imodel.TaskStatusCancelled)
	}

	var task imodel.Task
	if err := db.WithContext(ctx).First(&task, "id = ?", taskID).Error; err != nil {
		t.Fatalf("query cancelled task: %v", err)
	}
	if task.Status != imodel.TaskStatusCancelled {
		t.Fatalf("stored task.Status = %q, want %q", task.Status, imodel.TaskStatusCancelled)
	}
	if task.FinishedAt == nil {
		t.Fatal("stored task.FinishedAt = nil, want non-nil")
	}
}

func TestMutationResolverSubmitTaskUsesTenantNamePath(t *testing.T) {
	ctx := context.Background()
	db := setupGraphResolverTestDB(t)

	tenantID := uuid.New()
	if err := db.WithContext(ctx).Create(&imodel.Tenant{
		BaseUUIDModel:      imodel.BaseUUIDModel{ID: tenantID},
		Name:               "team-alpha",
		PrimaryBackendName: "ray",
		MaxAgents:          2,
	}).Error; err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	got, err := (&mutationResolver{&Resolver{}}).SubmitTask(ctx, gqlmodel.SubmitTaskInput{
		TenantName:        "team-alpha",
		RZipFile:          newGraphUpload("bundle.zip", []byte("fake-zip-bytes"), "application/zip"),
		ParametersCSVFile: newGraphUpload("params.csv", []byte("id,value\n1,a\n2,b\n"), "text/csv"),
	})
	if err != nil {
		t.Fatalf("SubmitTask() error = %v", err)
	}
	if got.TaskID == uuid.Nil {
		t.Fatal("SubmitTask().TaskID = uuid.Nil, want generated task ID")
	}

	var task imodel.Task
	if err := db.WithContext(ctx).First(&task, "id = ?", got.TaskID).Error; err != nil {
		t.Fatalf("query submitted task: %v", err)
	}
	if task.TenantID != tenantID {
		t.Fatalf("stored task.TenantID = %s, want %s", task.TenantID, tenantID)
	}
	if task.Status != imodel.TaskStatusPending {
		t.Fatalf("stored task.Status = %q, want %q", task.Status, imodel.TaskStatusPending)
	}
	if task.ShardCount != 2 {
		t.Fatalf("stored task.ShardCount = %d, want %d", task.ShardCount, 2)
	}
}

func TestCreateTenantStoresNormalizedName(t *testing.T) {
	ctx := context.Background()
	setupGraphResolverTestDB(t)

	got, err := (&mutationResolver{&Resolver{}}).CreateTenant(ctx, gqlmodel.CreateTenantInput{
		Name:               " Team-Alpha ",
		PrimaryBackendName: "ray",
		MaxAgents:          3,
	})
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if got.Name != "team-alpha" {
		t.Fatalf("got.Name = %q, want %q", got.Name, "team-alpha")
	}
}

func TestTaskGraphQLSchemaRemovesTenantIDArgsAndFields(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller(0) failed")
	}

	schemaPath := filepath.Join(filepath.Dir(thisFile), "schema", "task.graphqls")
	content, err := os.ReadFile(schemaPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", schemaPath, err)
	}

	schema := string(content)
	requiredSnippets := []string{
		"GetTaskByID(tenant_name: String!, task_id: UUID!): Task!",
		"GetTaskList(tenant_name: String!, status: String): [Task!]!",
		"GetTaskResultCSV(tenant_name: String!, task_id: UUID!): TaskResultCSV!",
		"CancelTask(tenant_name: String!, task_id: UUID!): CancelTaskPayload!",
		"type Task {\n  id: UUID!\n  tenant_name: String!",
		"input SubmitTaskInput {\n  tenant_name: String!",
	}
	for _, snippet := range requiredSnippets {
		if !strings.Contains(schema, snippet) {
			t.Fatalf("task.graphqls missing required snippet %q", snippet)
		}
	}
	if strings.Contains(schema, "tenant_id") {
		t.Fatalf("task.graphqls = %q, want no tenant_id references", schema)
	}
}

func setupGraphResolverTestDB(t *testing.T) *gorm.DB {
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

func mustCreateGraphTenant(t *testing.T, ctx context.Context, db *gorm.DB, tenantID uuid.UUID, tenantName string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&imodel.Tenant{
		BaseUUIDModel:      imodel.BaseUUIDModel{ID: tenantID},
		Name:               tenantName,
		PrimaryBackendName: "ray",
		MaxAgents:          1,
	}).Error; err != nil {
		t.Fatalf("create tenant: %v", err)
	}
}

func mustCreateGraphTask(t *testing.T, ctx context.Context, db *gorm.DB, task imodel.Task) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&task).Error; err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func mustCreateGraphArtifact(t *testing.T, ctx context.Context, db *gorm.DB, artifact imodel.Artifact) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&artifact).Error; err != nil {
		t.Fatalf("create artifact: %v", err)
	}
}

func newGraphUpload(filename string, content []byte, contentType string) graphql.Upload {
	return graphql.Upload{
		File:        graphUploadFile{Reader: bytes.NewReader(content)},
		Filename:    filename,
		Size:        int64(len(content)),
		ContentType: contentType,
	}
}

type graphUploadFile struct {
	*bytes.Reader
}

func (graphUploadFile) Close() error { return nil }
