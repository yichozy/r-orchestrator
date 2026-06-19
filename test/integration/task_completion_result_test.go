//go:build integration

package integration

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"github.com/yichozy/r-orchestrator/internal/util"
)

func TestSubmitTaskPersistsCompletionHookURL(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 2)
	tenantName := integrationTenantName(2)
	defer cleanupTenant(ctx, tenantID)

	taskID, err := task_service.SubmitTask(ctx, task_service.SubmitTaskParams{
		TenantName:        tenantName,
		ZipBytes:          []byte("fake-zip"),
		CSVBytes:          []byte("a,b\n1,2\n3,4\n"),
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})
	if err != nil {
		t.Fatalf("SubmitTask() error = %v", err)
	}

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if task.CompletionHookURL != "http://example.test/hooks/task-finished" {
		t.Fatalf("CompletionHookURL = %q", task.CompletionHookURL)
	}
	if task.ShardCount != 2 {
		t.Fatalf("ShardCount = %d, want 2", task.ShardCount)
	}
}

func TestSubmitTaskRejectsInvalidCompletionHookURL(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	defer cleanupTenant(ctx, tenantID)

	_, err := task_service.SubmitTask(ctx, task_service.SubmitTaskParams{
		TenantName:        tenantName,
		ZipBytes:          []byte("fake-zip"),
		CSVBytes:          []byte("a,b\n1,2\n"),
		CompletionHookURL: "not-a-valid-url",
	})
	if err == nil || !strings.Contains(err.Error(), "completion hook url") {
		t.Fatalf("SubmitTask() error = %v, want invalid completion hook url", err)
	}
}

func TestGetTaskReturnsLifecycleFields(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	defer cleanupTenant(ctx, tenantID)

	taskID, err := task_service.SubmitTask(ctx, task_service.SubmitTaskParams{
		TenantName: tenantName,
		ZipBytes:   []byte("fake-zip"),
		CSVBytes:   []byte("a,b\n1,2\n"),
	})
	if err != nil {
		t.Fatalf("SubmitTask() error = %v", err)
	}

	taskView, err := task_service.GetTask(ctx, tenantName, taskID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if taskView.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt was zero")
	}
	if taskView.ShardCount != 1 {
		t.Fatalf("ShardCount = %d, want 1", taskView.ShardCount)
	}
	if taskView.StartedAt != nil {
		t.Fatalf("StartedAt = %v, want nil", taskView.StartedAt)
	}
	if taskView.FinishedAt != nil {
		t.Fatalf("FinishedAt = %v, want nil", taskView.FinishedAt)
	}
}

func TestGetTaskResultCSVRejectsNonSucceededTask(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	defer cleanupTenant(ctx, tenantID)

	taskID, err := task_service.SubmitTask(ctx, task_service.SubmitTaskParams{
		TenantName: tenantName,
		ZipBytes:   []byte("fake-zip"),
		CSVBytes:   []byte("a,b\n1,2\n"),
	})
	if err != nil {
		t.Fatalf("SubmitTask() error = %v", err)
	}

	_, err = task_service.GetTaskResultCSV(ctx, tenantName, taskID)
	if !errors.Is(err, task_service.ErrTaskNotSucceeded) {
		t.Fatalf("GetTaskResultCSV() error = %v, want ErrTaskNotSucceeded", err)
	}
}

func TestGetTaskResultCSVReturnsStoredContent(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	defer cleanupTenant(ctx, tenantID)

	taskID, err := task_service.SubmitTask(ctx, task_service.SubmitTaskParams{
		TenantName: tenantName,
		ZipBytes:   []byte("fake-zip"),
		CSVBytes:   []byte("a,b\n1,2\n"),
	})
	if err != nil {
		t.Fatalf("SubmitTask() error = %v", err)
	}

	const wantCSV = "id,value\n1,a\n2,b\n"
	markTaskSucceededWithResult(t, ctx, tenantID, taskID, []byte(wantCSV))

	result, err := task_service.GetTaskResultCSV(ctx, tenantName, taskID)
	if err != nil {
		t.Fatalf("GetTaskResultCSV() error = %v", err)
	}
	if result.TaskID != taskID {
		t.Fatalf("TaskID = %s, want %s", result.TaskID, taskID)
	}
	if result.Filename != "task-"+taskID.String()+"-result.csv" {
		t.Fatalf("Filename = %q", result.Filename)
	}
	if result.ContentType != "text/csv; charset=utf-8" {
		t.Fatalf("ContentType = %q", result.ContentType)
	}
	if result.CSVContent != wantCSV {
		t.Fatalf("CSVContent = %q, want %q", result.CSVContent, wantCSV)
	}
}

func TestGetTaskResultCSVReturnsAggregatedContent(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	defer cleanupTenant(ctx, tenantID)

	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	agentID := "10000000-0000-0000-0000-000000000011"
	if err := sendRegister(stream, agentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg1, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 1: %v", err)
	}
	assign1 := msg1.GetAssignShard()
	if err := sendShardStarted(stream, assign1.GetShardId()); err != nil {
		t.Fatalf("shard started 1: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign1.GetShardId(), []byte("id,value\n1,a\n2,b\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 1 via result-ready: %v", err)
	}

	msg2, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 2: %v", err)
	}
	assign2 := msg2.GetAssignShard()
	if err := sendShardStarted(stream, assign2.GetShardId()); err != nil {
		t.Fatalf("shard started 2: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign2.GetShardId(), []byte("id,value\n3,c\n4,d\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 2 via result-ready: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
	if task.ResultArtifactID == nil {
		t.Fatalf("ResultArtifactID = nil, want aggregated artifact")
	}

	result, err := task_service.GetTaskResultCSV(ctx, tenantName, taskID)
	if err != nil {
		t.Fatalf("GetTaskResultCSV() error = %v", err)
	}
	const wantCSV = "id,value\n1,a\n2,b\n3,c\n4,d\n"
	if result.CSVContent != wantCSV {
		t.Fatalf("CSVContent = %q, want %q", result.CSVContent, wantCSV)
	}
}

func TestTaskFailsWhenAnySuccessfulShardOutputIsEmpty(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	defer cleanupTenant(ctx, tenantID)

	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	agentID := "10000000-0000-0000-0000-000000000012"
	if err := sendRegister(stream, agentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg1, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 1: %v", err)
	}
	assign1 := msg1.GetAssignShard()
	if err := sendShardStarted(stream, assign1.GetShardId()); err != nil {
		t.Fatalf("shard started 1: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign1.GetShardId(), []byte("id,value\n1,a\n2,b\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 1 via result-ready: %v", err)
	}

	msg2, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 2: %v", err)
	}
	assign2 := msg2.GetAssignShard()
	if err := sendShardStarted(stream, assign2.GetShardId()); err != nil {
		t.Fatalf("shard started 2: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign2.GetShardId(), nil, 5*time.Second); err != nil {
		t.Fatalf("complete shard 2 via result-ready: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
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
}

func TestTaskSuccessDispatchesCompletionHookAndRecordsDelivery(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	defer cleanupTenant(ctx, tenantID)

	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	bodyCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		bodyCh <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	if err := testDB.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", taskID).
		Update("completion_hook_url", ts.URL).Error; err != nil {
		t.Fatalf("set completion hook url: %v", err)
	}

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	agentID := "10000000-0000-0000-0000-000000000013"
	if err := sendRegister(stream, agentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg1, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 1: %v", err)
	}
	assign1 := msg1.GetAssignShard()
	if err := sendShardStarted(stream, assign1.GetShardId()); err != nil {
		t.Fatalf("shard started 1: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign1.GetShardId(), []byte("id,value\n1,a\n2,b\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 1 via result-ready: %v", err)
	}

	msg2, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 2: %v", err)
	}
	assign2 := msg2.GetAssignShard()
	if err := sendShardStarted(stream, assign2.GetShardId()); err != nil {
		t.Fatalf("shard started 2: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign2.GetShardId(), []byte("id,value\n3,c\n4,d\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 2 via result-ready: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	var hookBody string
	select {
	case hookBody = <-bodyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for completion hook")
	}

	task := waitForTaskHookRecorded(t, ctx, taskID, func(task model.Task) bool {
		return task.HookDeliveredAt != nil && task.HookLastError == ""
	})
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}
	if task.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want terminal timestamp")
	}
	if task.HookDeliveredAt == nil {
		t.Fatal("HookDeliveredAt = nil, want delivery timestamp")
	}
	if task.HookLastError != "" {
		t.Fatalf("HookLastError = %q, want empty", task.HookLastError)
	}
	if !strings.Contains(hookBody, `"status":"SUCCEEDED"`) {
		t.Fatalf("hook body = %s, want succeeded status", hookBody)
	}
	if !strings.Contains(hookBody, `"result_available":true`) {
		t.Fatalf("hook body = %s, want result_available true", hookBody)
	}
}

func TestTaskFailureDispatchesCompletionHookAndRecordsDelivery(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	defer cleanupTenant(ctx, tenantID)

	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	bodyCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		bodyCh <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	if err := testDB.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", taskID).
		Update("completion_hook_url", ts.URL).Error; err != nil {
		t.Fatalf("set completion hook url: %v", err)
	}

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	agentID := "10000000-0000-0000-0000-000000000014"
	if err := sendRegister(stream, agentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg1, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 1: %v", err)
	}
	assign1 := msg1.GetAssignShard()
	if err := sendShardStarted(stream, assign1.GetShardId()); err != nil {
		t.Fatalf("shard started 1: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign1.GetShardId(), []byte("id,value\n1,a\n2,b\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 1 via result-ready: %v", err)
	}

	msg2, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 2: %v", err)
	}
	assign2 := msg2.GetAssignShard()
	if err := sendShardStarted(stream, assign2.GetShardId()); err != nil {
		t.Fatalf("shard started 2: %v", err)
	}
	if err := sendShardFailed(stream, assign2.GetShardId(), "runtime error"); err != nil {
		t.Fatalf("shard failed 2: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	var hookBody string
	select {
	case hookBody = <-bodyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for failed completion hook")
	}

	task := waitForTaskHookRecorded(t, ctx, taskID, func(task model.Task) bool {
		return task.HookDeliveredAt != nil && task.HookLastError == ""
	})
	if task.Status != model.TaskStatusFailed {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusFailed)
	}
	if task.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want terminal timestamp")
	}
	if task.HookDeliveredAt == nil {
		t.Fatal("HookDeliveredAt = nil, want delivery timestamp")
	}
	if task.HookLastError != "" {
		t.Fatalf("HookLastError = %q, want empty", task.HookLastError)
	}
	if task.LastError != "runtime error" {
		t.Fatalf("LastError = %q, want %q", task.LastError, "runtime error")
	}
	if !strings.Contains(hookBody, `"status":"FAILED"`) {
		t.Fatalf("hook body = %s, want failed status", hookBody)
	}
	if !strings.Contains(hookBody, `"result_available":false`) {
		t.Fatalf("hook body = %s, want result_available false", hookBody)
	}
	if !strings.Contains(hookBody, `"last_error":"runtime error"`) {
		t.Fatalf("hook body = %s, want runtime error", hookBody)
	}
}

func TestCancelTaskDispatchesCompletionHookAndRecordsDelivery(t *testing.T) {
	ctx := context.Background()
	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	defer cleanupTenant(ctx, tenantID)

	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	bodyCh := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		bodyCh <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	if err := testDB.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", taskID).
		Update("completion_hook_url", ts.URL).Error; err != nil {
		t.Fatalf("set completion hook url: %v", err)
	}

	if err := task_service.CancelTask(ctx, tenantName, taskID); err != nil {
		t.Fatalf("CancelTask() error = %v", err)
	}

	var hookBody string
	select {
	case hookBody = <-bodyCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancellation hook")
	}

	task := waitForTaskHookRecorded(t, ctx, taskID, func(task model.Task) bool {
		return task.HookDeliveredAt != nil && task.HookLastError == ""
	})
	if task.Status != model.TaskStatusCancelled {
		t.Fatalf("task status = %s, want %s", task.Status, model.TaskStatusCancelled)
	}
	if task.FinishedAt == nil {
		t.Fatal("FinishedAt = nil, want terminal timestamp")
	}
	if task.HookDeliveredAt == nil {
		t.Fatal("HookDeliveredAt = nil, want delivery timestamp")
	}
	if task.HookLastError != "" {
		t.Fatalf("HookLastError = %q, want empty", task.HookLastError)
	}
	if !strings.Contains(hookBody, `"status":"CANCELLED"`) {
		t.Fatalf("hook body = %s, want cancelled status", hookBody)
	}
	if !strings.Contains(hookBody, `"result_available":false`) {
		t.Fatalf("hook body = %s, want result_available false", hookBody)
	}
}

func markTaskSucceededWithResult(t *testing.T, ctx context.Context, tenantID uuid.UUID, taskID uuid.UUID, resultBytes []byte) {
	t.Helper()

	artifactID, err := uuid.NewV7()
	if err != nil {
		t.Fatalf("uuid.NewV7() error = %v", err)
	}

	artifact := model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: artifactID},
		TenantID:      tenantID,
		TaskID:        taskID,
		ArtifactType:  model.ArtifactTypeTaskOutput,
		ContentBytes:  append([]byte(nil), resultBytes...),
		ContentSize:   int64(len(resultBytes)),
		SHA256:        util.SumSHA256(resultBytes),
	}
	if err := artifact_orm.Create(ctx, testDB, artifact); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	finishedAt := time.Now().UTC()
	if err := testDB.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", taskID).
		Updates(map[string]any{
			"status":             model.TaskStatusSucceeded,
			"result_artifact_id": artifactID,
			"finished_at":        finishedAt,
			"last_error":         "",
		}).Error; err != nil {
		t.Fatalf("mark task succeeded: %v", err)
	}
}

func waitForTaskHookRecorded(t *testing.T, ctx context.Context, taskID uuid.UUID, done func(model.Task) bool) model.Task {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		task, err := task_orm.GetByID(ctx, testDB, taskID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}
		if done(task) {
			return task
		}
		time.Sleep(50 * time.Millisecond)
	}

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("GetByID() final error = %v", err)
	}
	t.Fatalf("timed out waiting for hook bookkeeping, task = %+v", task)
	return model.Task{}
}
