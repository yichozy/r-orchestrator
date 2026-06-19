# Tenant Name External API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace public `tenant_id` usage with `tenant_name` across tenant and task GraphQL APIs while keeping UUID as the internal tenant key.

**Architecture:** Add a lowercase unique `tenants.name` field, resolve `tenant_name -> tenant.id` at the service boundary, and keep all internal relationships and control-plane contracts UUID-based. GraphQL schemas and resolvers become name-first, while ORM and task services preserve existing internal behavior after tenant resolution.

**Tech Stack:** Go, gqlgen GraphQL, GORM, SQLite test DB, PostgreSQL-compatible schema via AutoMigrate

---

## File Map

### Model / ORM

- Modify: `internal/model/tenant.go`
  - Add `Name string` with unique, non-null persistence metadata.
- Modify: `internal/orm/tenant_orm/create.go`
  - Normalize tenant names before create.
- Modify: `internal/orm/tenant_orm/get_by_id.go`
  - Keep existing UUID lookup unchanged.
- Create: `internal/orm/tenant_orm/get_by_name.go`
  - Add normalized lookup by tenant name.
- Create: `internal/orm/tenant_orm/normalize_name.go`
  - Centralize trim + lowercase validation.

### GraphQL Tenant Boundary

- Modify: `graph/schema/tenant.graphqls`
  - Add `name` to `CreateTenantInput` and `Tenant`.
- Modify: `graph/tenant.resolvers.go`
  - Pass tenant name into ORM create and return `Tenant.name`.

### Task Service Boundary

- Modify: `internal/service/task_service/service.go`
  - Replace `TenantID uuid.UUID` in public DTOs/params with `TenantName string` where applicable.
- Create: `internal/service/task_service/resolve_tenant.go`
  - Add helper to normalize and resolve tenant names to tenant records.
- Modify: `internal/service/task_service/submit_task.go`
  - Resolve tenant by name, then continue existing internal task creation using `tenant.ID`.
- Modify: `internal/service/task_service/get_task.go`
  - Resolve tenant by name before enforcing task ownership.
- Modify: `internal/service/task_service/list_tasks.go`
  - Resolve tenant by name before listing tasks.
- Modify: `internal/service/task_service/get_task_result_csv.go`
  - Resolve tenant by name before result lookup.
- Modify: `internal/service/task_service/cancel_task.go`
  - Resolve tenant by name before cancellation.

### GraphQL Task Boundary

- Modify: `graph/schema/task.graphqls`
  - Replace all `tenant_id` arguments/fields with `tenant_name`.
- Modify: `graph/task.resolvers.go`
  - Accept `tenant_name`, pass it into task services, and return `tenant_name`.

### Tests

- Modify: `graph/task_resolvers_test.go`
  - Update fixtures and resolver calls to use tenant names.
- Modify: `internal/service/task_service/get_task_result_csv_test.go`
  - Create tenants with names and update service calls to use names.
- Modify: `internal/service/task_service/dispatch_completion_hook_test.go`
  - Update helper-created tenants and any `GetTask(...)` calls.
- Modify: `test/integration/task_completion_result_test.go`
  - Update external task service calls to use tenant names.
- Modify: `test/integration/helpers.go`
  - Seed tenants with names.

## Task 1: Add Tenant Name Model and ORM Support

**Files:**
- Create: `internal/orm/tenant_orm/get_by_name.go`
- Create: `internal/orm/tenant_orm/normalize_name.go`
- Modify: `internal/model/tenant.go`
- Modify: `internal/orm/tenant_orm/create.go`
- Test: `internal/service/task_service/get_task_result_csv_test.go`

- [ ] **Step 1: Write failing ORM-facing tests using existing SQLite helper**

Add these tests near the bottom of `internal/service/task_service/get_task_result_csv_test.go`:

```go
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
```

- [ ] **Step 2: Run only the new tenant tests and confirm compile/test failure**

Run:

```bash
/usr/local/go/bin/go test ./internal/service/task_service -run 'TestTenantORMCreateNormalizesNameToLowercase|TestTenantORMGetByNameMatchesCaseInsensitiveInput' -v
```

Expected:

- compile failure because `model.Tenant` has no `Name`
- or missing `tenant_orm.GetByName`

- [ ] **Step 3: Add the `name` column to the tenant model**

Update `internal/model/tenant.go` to:

```go
package model

type Tenant struct {
	BaseUUIDModel
	Name               string `gorm:"column:name;not null;uniqueIndex"`
	PrimaryBackendName string `gorm:"column:primary_backend_name;not null"`
	MaxAgents          int    `gorm:"column:max_agents;not null"`
}

func (Tenant) TableName() string { return "tenants" }
```

- [ ] **Step 4: Add normalization helper**

Create `internal/orm/tenant_orm/normalize_name.go`:

```go
package tenant_orm

import (
	"fmt"
	"strings"
)

func NormalizeName(raw string) (string, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	if normalized == "" {
		return "", fmt.Errorf("tenant name is required")
	}
	return normalized, nil
}
```

- [ ] **Step 5: Normalize names on create**

Update `internal/orm/tenant_orm/create.go` to:

```go
package tenant_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, tenant model.Tenant) (model.Tenant, error) {
	name, err := NormalizeName(tenant.Name)
	if err != nil {
		return model.Tenant{}, err
	}
	tenant.Name = name
	if err := db.WithContext(ctx).Create(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
```

- [ ] **Step 6: Add name lookup**

Create `internal/orm/tenant_orm/get_by_name.go`:

```go
package tenant_orm

import (
	"context"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetByName(ctx context.Context, db *gorm.DB, name string) (model.Tenant, error) {
	normalized, err := NormalizeName(name)
	if err != nil {
		return model.Tenant{}, err
	}

	var tenant model.Tenant
	if err := db.WithContext(ctx).Where("name = ?", normalized).First(&tenant).Error; err != nil {
		return model.Tenant{}, err
	}
	return tenant, nil
}
```

- [ ] **Step 7: Re-run the tenant ORM tests**

Run:

```bash
/usr/local/go/bin/go test ./internal/service/task_service -run 'TestTenantORMCreateNormalizesNameToLowercase|TestTenantORMGetByNameMatchesCaseInsensitiveInput' -v
```

Expected:

- both tests `PASS`

- [ ] **Step 8: Commit the model/ORM change**

```bash
git add internal/model/tenant.go internal/orm/tenant_orm/create.go internal/orm/tenant_orm/get_by_name.go internal/orm/tenant_orm/normalize_name.go internal/service/task_service/get_task_result_csv_test.go
git commit -m "feat: add normalized tenant name support"
```

## Task 2: Switch Tenant GraphQL Create API to Name

**Files:**
- Modify: `graph/schema/tenant.graphqls`
- Modify: `graph/tenant.resolvers.go`
- Modify: `graph/task_resolvers_test.go`

- [ ] **Step 1: Write a failing tenant GraphQL resolver test**

Append this test to `graph/task_resolvers_test.go`:

```go
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
```

- [ ] **Step 2: Run the single resolver test and confirm failure**

Run:

```bash
/usr/local/go/bin/go test ./graph -run TestCreateTenantStoresNormalizedName -v
```

Expected:

- compile failure because GraphQL model/schema do not expose `Name`

- [ ] **Step 3: Update tenant GraphQL schema**

Modify `graph/schema/tenant.graphqls` to:

```graphql
scalar Upload

input CreateTenantInput {
  name: String!
  primary_backend_name: String!
  max_agents: Int!
}

type Tenant {
  id: UUID!
  name: String!
  primary_backend_name: String!
  max_agents: Int!
}

extend type Mutation {
  CreateTenant(input: CreateTenantInput!): Tenant!
}
```

- [ ] **Step 4: Regenerate gqlgen artifacts**

Run:

```bash
/usr/local/go/bin/go run github.com/99designs/gqlgen generate
```

Expected:

- `graph/generated/generated.go` and `graph/model/models_gen.go` update successfully

- [ ] **Step 5: Update tenant resolver**

Modify `graph/tenant.resolvers.go` so `CreateTenant` passes `input.Name` and returns `Tenant.Name`:

```go
tenant, err := tenant_orm.Create(ctx, db, model.Tenant{
	Name:               input.Name,
	PrimaryBackendName: input.PrimaryBackendName,
	MaxAgents:          input.MaxAgents,
})
```

and return:

```go
return &gqlmodel.Tenant{
	ID:                 tenant.ID,
	Name:               tenant.Name,
	PrimaryBackendName: tenant.PrimaryBackendName,
	MaxAgents:          tenant.MaxAgents,
}, nil
```

- [ ] **Step 6: Re-run the tenant resolver test**

Run:

```bash
/usr/local/go/bin/go test ./graph -run TestCreateTenantStoresNormalizedName -v
```

Expected:

- test `PASS`

- [ ] **Step 7: Commit the tenant GraphQL change**

```bash
git add graph/schema/tenant.graphqls graph/tenant.resolvers.go graph/generated/generated.go graph/model/models_gen.go graph/task_resolvers_test.go
git commit -m "feat: switch tenant create api to tenant name"
```

## Task 3: Switch Task Services to Resolve Tenant by Name

**Files:**
- Create: `internal/service/task_service/resolve_tenant.go`
- Modify: `internal/service/task_service/service.go`
- Modify: `internal/service/task_service/submit_task.go`
- Modify: `internal/service/task_service/get_task.go`
- Modify: `internal/service/task_service/list_tasks.go`
- Modify: `internal/service/task_service/get_task_result_csv.go`
- Modify: `internal/service/task_service/cancel_task.go`
- Modify: `internal/service/task_service/get_task_result_csv_test.go`
- Modify: `internal/service/task_service/dispatch_completion_hook_test.go`

- [ ] **Step 1: Update existing task service tests to expect tenant name inputs**

In `internal/service/task_service/get_task_result_csv_test.go`, change calls like:

```go
_, err := GetTask(ctx, callerTenantID, taskID)
```

to:

```go
_, err := GetTask(ctx, "caller-team", taskID)
```

and update tenant fixtures to include names:

```go
if err := db.WithContext(ctx).Create(&model.Tenant{
	BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
	Name:               "team-alpha",
	PrimaryBackendName: "ray",
	MaxAgents:          1,
}).Error; err != nil {
	t.Fatalf("create tenant: %v", err)
}
```

Also update any `GetTask(...)` call in `internal/service/task_service/dispatch_completion_hook_test.go` to use the seeded name.

- [ ] **Step 2: Run targeted task service tests and confirm failure**

Run:

```bash
/usr/local/go/bin/go test ./internal/service/task_service -run 'TestDispatchCompletionHookAsyncRecordsDeliverySuccess|TestGetTaskResultCSVReturnsStoredContent' -v
```

Expected:

- compile failure due to function signatures still taking `uuid.UUID`

- [ ] **Step 3: Add tenant resolution helper**

Create `internal/service/task_service/resolve_tenant.go`:

```go
package task_service

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"gorm.io/gorm"
)

func resolveTenantByName(ctx context.Context, db *gorm.DB, tenantName string) (model.Tenant, error) {
	tenant, err := tenant_orm.GetByName(ctx, db, tenantName)
	if err != nil {
		return model.Tenant{}, fmt.Errorf("query tenant: %w", err)
	}
	return tenant, nil
}
```

- [ ] **Step 4: Update public task service DTOs**

Modify `internal/service/task_service/service.go`:

```go
type SubmitTaskParams struct {
	TenantName         string
	ZipBytes           []byte
	CSVBytes           []byte
	CompletionHookURL  string
}

type TaskView struct {
	ID         uuid.UUID
	TenantName string
	Status     string
	CreatedAt  time.Time
	StartedAt  *time.Time
	FinishedAt *time.Time
	ShardCount int
	LastError  string
}
```

- [ ] **Step 5: Switch `SubmitTask` to resolve by name**

Update `internal/service/task_service/submit_task.go` so it resolves tenant first:

```go
tenant, err := resolveTenantByName(ctx, db, params.TenantName)
if err != nil {
	return uuid.Nil, err
}
```

and replace all persisted `params.TenantID` writes with `tenant.ID`.

- [ ] **Step 6: Switch read/list/result/cancel service entry points**

Update signatures:

```go
func GetTask(ctx context.Context, tenantName string, taskID uuid.UUID) (TaskView, error)
func ListTasks(ctx context.Context, tenantName string, status string) ([]TaskView, error)
func GetTaskResultCSV(ctx context.Context, tenantName string, taskID uuid.UUID) (TaskResultCSVView, error)
func CancelTask(ctx context.Context, tenantName string, taskID uuid.UUID) error
```

Each function should start with:

```go
tenant, err := resolveTenantByName(ctx, db, tenantName)
if err != nil {
	return ..., err
}
```

Then continue enforcing ownership using `tenant.ID`.

- [ ] **Step 7: Return `TenantName` in `TaskView`**

When building `TaskView` in `get_task.go` and `list_tasks.go`, set:

```go
TenantName: tenant.Name,
```

- [ ] **Step 8: Re-run focused task service tests**

Run:

```bash
/usr/local/go/bin/go test ./internal/service/task_service -run 'TestDispatchCompletionHookAsyncRecordsDeliverySuccess|TestGetTaskResultCSVReturnsStoredContent|TestGetTaskResultCSVRejectsNonSucceededTask' -v
```

Expected:

- tests `PASS`

- [ ] **Step 9: Commit the task service boundary change**

```bash
git add internal/service/task_service/service.go internal/service/task_service/resolve_tenant.go internal/service/task_service/submit_task.go internal/service/task_service/get_task.go internal/service/task_service/list_tasks.go internal/service/task_service/get_task_result_csv.go internal/service/task_service/cancel_task.go internal/service/task_service/get_task_result_csv_test.go internal/service/task_service/dispatch_completion_hook_test.go
git commit -m "feat: resolve external task apis by tenant name"
```

## Task 4: Switch Task GraphQL Schema and Resolvers to Tenant Name

**Files:**
- Modify: `graph/schema/task.graphqls`
- Modify: `graph/task.resolvers.go`
- Modify: `graph/task_resolvers_test.go`

- [ ] **Step 1: Update existing graph task tests to use tenant names**

In `graph/task_resolvers_test.go`, update tenant fixtures:

```go
if err := db.WithContext(ctx).Create(&imodel.Tenant{
	BaseUUIDModel:      imodel.BaseUUIDModel{ID: tenantID},
	Name:               "team-alpha",
	PrimaryBackendName: "ray",
	MaxAgents:          1,
}).Error; err != nil {
	t.Fatalf("create tenant: %v", err)
}
```

Update resolver calls:

```go
got, err := (&queryResolver{&Resolver{}}).GetTaskResultCSV(ctx, "team-alpha", taskID)
_, err := (&queryResolver{&Resolver{}}).GetTaskByID(ctx, "caller-team", taskID)
```

- [ ] **Step 2: Run graph task tests and confirm failure**

Run:

```bash
/usr/local/go/bin/go test ./graph -run 'TestQueryResolverGetTaskResultCSVMapsFields|TestQueryResolverGetTaskByIDPropagatesTaskNotFound' -v
```

Expected:

- compile failure because resolver signatures and DTOs still use `tenant_id`

- [ ] **Step 3: Update task GraphQL schema**

Modify `graph/schema/task.graphqls` to:

```graphql
scalar UUID
scalar Time

type Query {
  GetTaskByID(tenant_name: String!, task_id: UUID!): Task!
  GetTaskList(tenant_name: String!, status: String): [Task!]!
  GetTaskResultCSV(tenant_name: String!, task_id: UUID!): TaskResultCSV!
}

type Mutation {
  SubmitTask(input: SubmitTaskInput!): SubmitTaskResponse!
  CancelTask(tenant_name: String!, task_id: UUID!): CancelTaskPayload!
}

type Task {
  id: UUID!
  tenant_name: String!
  status: String!
  last_error: String!
  created_at: Time!
  started_at: Time
  finished_at: Time
  shard_count: Int!
}

input SubmitTaskInput {
  tenant_name: String!
  r_zip_file: Upload!
  parameters_csv_file: Upload!
  completion_hook_url: String
}
```

- [ ] **Step 4: Regenerate gqlgen artifacts**

Run:

```bash
/usr/local/go/bin/go run github.com/99designs/gqlgen generate
```

Expected:

- generated files update cleanly

- [ ] **Step 5: Update task resolvers**

Modify `graph/task.resolvers.go` signatures and mappings:

```go
func (r *mutationResolver) CancelTask(ctx context.Context, tenantName string, taskID uuid.UUID) (*model.CancelTaskPayload, error)
func (r *queryResolver) GetTaskByID(ctx context.Context, tenantName string, taskID uuid.UUID) (*model.Task, error)
func (r *queryResolver) GetTaskList(ctx context.Context, tenantName string, status *string) ([]*model.Task, error)
func (r *queryResolver) GetTaskResultCSV(ctx context.Context, tenantName string, taskID uuid.UUID) (*model.TaskResultCSV, error)
```

and pass:

```go
task, err := task_service.GetTask(ctx, tenantName, taskID)
```

For `SubmitTask`:

```go
taskID, err := task_service.SubmitTask(ctx, task_service.SubmitTaskParams{
	TenantName:        input.TenantName,
	ZipBytes:          zipBytes,
	CSVBytes:          csvBytes,
	CompletionHookURL: completionHookURL,
})
```

When returning `Task`, map:

```go
TenantName: task.TenantName,
```

- [ ] **Step 6: Re-run graph tests**

Run:

```bash
/usr/local/go/bin/go test ./graph -run 'TestCreateTenantStoresNormalizedName|TestQueryResolverGetTaskResultCSVMapsFields|TestQueryResolverGetTaskByIDPropagatesTaskNotFound' -v
```

Expected:

- tests `PASS`

- [ ] **Step 7: Commit the GraphQL task boundary change**

```bash
git add graph/schema/task.graphqls graph/task.resolvers.go graph/generated/generated.go graph/model/models_gen.go graph/task_resolvers_test.go
git commit -m "feat: switch task graphql apis to tenant name"
```

## Task 5: Update Integration Fixtures and Run Full Regression

**Files:**
- Modify: `test/integration/helpers.go`
- Modify: `test/integration/task_completion_result_test.go`
- Modify: `graph/task_resolvers_test.go`
- Modify: `internal/service/task_service/get_task_result_csv_test.go`

- [ ] **Step 1: Seed tenant names in integration helpers**

Update `test/integration/helpers.go` tenant creation helpers so inserted tenants include `Name`, for example:

```go
_, err := tenant_orm.Create(ctx, testDB, model.Tenant{
	BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
	Name:               "integration-team",
	PrimaryBackendName: "ray",
	MaxAgents:          maxAgents,
})
```

If helper uniqueness matters across tests, use deterministic suffixed names, for example `fmt.Sprintf("integration-team-%d", maxAgents)`.

- [ ] **Step 2: Update integration service calls to use tenant names**

In `test/integration/task_completion_result_test.go`, change:

```go
task_service.SubmitTask(ctx, task_service.SubmitTaskParams{TenantID: tenantID, ...})
```

to:

```go
task_service.SubmitTask(ctx, task_service.SubmitTaskParams{TenantName: "integration-team-2", ...})
```

and similarly update:

```go
task_service.GetTask(ctx, "integration-team-1", taskID)
task_service.GetTaskResultCSV(ctx, "integration-team-1", taskID)
task_service.CancelTask(ctx, "integration-team-1", taskID)
```

- [ ] **Step 3: Run focused integration tests**

Run:

```bash
/usr/local/go/bin/go test -tags=integration ./test/integration -run 'TestSubmitTaskPersistsCompletionHookURL|TestGetTaskResultCSVReturnsStoredContent|TestGetTaskResultCSVReturnsAggregatedContent|TestCancelTaskDispatchesCompletionHookAndRecordsDelivery' -v
```

Expected:

- all selected integration tests `PASS`

- [ ] **Step 4: Run full Go regression**

Run:

```bash
/usr/local/go/bin/go test ./...
```

Expected:

- full Go test suite `PASS`

- [ ] **Step 5: Commit the regression and fixture updates**

```bash
git add test/integration/helpers.go test/integration/task_completion_result_test.go graph/task_resolvers_test.go internal/service/task_service/get_task_result_csv_test.go
git commit -m "test: update fixtures for tenant name external api"
```

## Spec Coverage Check

- Add stable lowercase unique tenant name: covered by Task 1.
- Make tenant GraphQL create API name-based: covered by Task 2.
- Make all public task services resolve by tenant name: covered by Task 3.
- Make task GraphQL schema/resolvers tenant-name based: covered by Task 4.
- Keep internal UUID relationships/runtime unchanged: preserved throughout Tasks 3-5 by resolving names only at service boundaries.
- Regression coverage across ORM/service/GraphQL/integration: covered by Tasks 1-5.

## Placeholder Scan

- No `TODO` / `TBD` placeholders remain.
- Each code-changing step includes concrete code or exact schema content.
- Each verification step includes an exact command and expected outcome.

## Type Consistency Check

- Public task service signatures consistently switch to `tenantName string`.
- GraphQL schema consistently uses `tenant_name`.
- Internal persisted references continue using `tenant.ID uuid.UUID`.
- `TaskView` consistently returns `TenantName string`.

