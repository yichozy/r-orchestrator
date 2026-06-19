package task_service

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
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestDispatchCompletionHookPostsJSON(t *testing.T) {
	var gotMethod string
	var gotContentType string
	var gotBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("ReadAll() error = %v", err)
		}
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotBody = append([]byte(nil), body...)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	finishedAt := time.Date(2026, 6, 18, 12, 34, 56, 0, time.UTC)
	payload := CompletionHookPayload{
		TaskID:            uuid.MustParse("00000000-0000-0000-0000-000000000010"),
		TenantID:          uuid.MustParse("00000000-0000-0000-0000-000000000011"),
		Status:            model.TaskStatusSucceeded,
		LastError:         "",
		FinishedAt:        &finishedAt,
		ResultAvailable:   true,
		CompletionHookURL: ts.URL,
	}

	if err := dispatchCompletionHook(context.Background(), ts.Client(), payload); err != nil {
		t.Fatalf("dispatchCompletionHook() error = %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("method = %s, want %s", gotMethod, http.MethodPost)
	}
	if gotContentType != "application/json" {
		t.Fatalf("content-type = %q, want application/json", gotContentType)
	}
	body := string(gotBody)
	if !strings.Contains(body, `"status":"SUCCEEDED"`) {
		t.Fatalf("hook body = %s, want succeeded status", body)
	}
	if !strings.Contains(body, `"result_available":true`) {
		t.Fatalf("hook body = %s, want result_available true", body)
	}
}

func TestDispatchCompletionHookAsyncRecordsDeliverySuccess(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	tenantName := "completion-team"
	taskID := uuid.New()
	mustCreateTenantWithName(t, ctx, db, tenantID, tenantName)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel:     model.BaseUUIDModel{ID: taskID},
		TenantID:          tenantID,
		Status:            model.TaskStatusSucceeded,
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})

	restoreHookDispatchGlobals(t)
	runHookDispatchAsync = func(fn func()) { fn() }
	logs, observedLogs := observer.New(zapcore.InfoLevel)
	prevLogger := zap.L()
	zap.ReplaceGlobals(zap.New(logs))
	t.Cleanup(func() {
		zap.ReplaceGlobals(prevLogger)
	})
	hookDispatcher = func(ctx context.Context, payload CompletionHookPayload) error {
		if payload.TaskID != taskID {
			t.Fatalf("payload.TaskID = %s, want %s", payload.TaskID, taskID)
		}
		if payload.Status != model.TaskStatusSucceeded {
			t.Fatalf("payload.Status = %s, want %s", payload.Status, model.TaskStatusSucceeded)
		}
		return nil
	}

	dispatchCompletionHookAsync(CompletionHookPayload{
		TaskID:            taskID,
		TenantID:          tenantID,
		Status:            model.TaskStatusSucceeded,
		ResultAvailable:   true,
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})

	task, err := GetTask(ctx, tenantName, taskID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Fatalf("task.Status = %s, want %s", task.Status, model.TaskStatusSucceeded)
	}

	var stored model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&stored).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if stored.HookDeliveredAt == nil {
		t.Fatal("HookDeliveredAt = nil, want recorded delivery time")
	}
	if stored.HookLastError != "" {
		t.Fatalf("HookLastError = %q, want empty", stored.HookLastError)
	}
	successLogs := observedLogs.FilterMessage("completion hook dispatched").All()
	if len(successLogs) != 1 {
		t.Fatalf("success log count = %d, want 1", len(successLogs))
	}
	fields := successLogs[0].ContextMap()
	if got := fields["task_id"]; got != taskID.String() {
		t.Fatalf("success log task_id = %v, want %s", got, taskID)
	}
	if got := fields["hook_url"]; got != "http://example.test/hooks/task-finished" {
		t.Fatalf("success log hook_url = %v", got)
	}
	if got := fields["status"]; got != model.TaskStatusSucceeded {
		t.Fatalf("success log status = %v, want %s", got, model.TaskStatusSucceeded)
	}
	if got := fields["result_available"]; got != true {
		t.Fatalf("success log result_available = %v, want true", got)
	}
}

func TestDispatchCompletionHookAsyncRecordsDeliveryFailure(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel:     model.BaseUUIDModel{ID: taskID},
		TenantID:          tenantID,
		Status:            model.TaskStatusFailed,
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})

	restoreHookDispatchGlobals(t)
	runHookDispatchAsync = func(fn func()) { fn() }
	hookDispatcher = func(ctx context.Context, payload CompletionHookPayload) error {
		return errors.New("hook server unavailable")
	}

	dispatchCompletionHookAsync(CompletionHookPayload{
		TaskID:            taskID,
		TenantID:          tenantID,
		Status:            model.TaskStatusFailed,
		LastError:         "task failed",
		ResultAvailable:   false,
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})

	var stored model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&stored).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if stored.HookDeliveredAt != nil {
		t.Fatalf("HookDeliveredAt = %v, want nil", stored.HookDeliveredAt)
	}
	if !strings.Contains(stored.HookLastError, "hook server unavailable") {
		t.Fatalf("HookLastError = %q, want dispatcher error", stored.HookLastError)
	}
}

func TestDispatchCompletionHookAsyncRecordsDeliveryWhenDispatchContextExpires(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel:     model.BaseUUIDModel{ID: taskID},
		TenantID:          tenantID,
		Status:            model.TaskStatusFailed,
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})

	restoreHookDispatchGlobals(t)
	runHookDispatchAsync = func(fn func()) { fn() }
	completionHookDispatchTimeout = time.Millisecond
	completionHookRecordTimeout = time.Second
	hookDispatcher = func(ctx context.Context, payload CompletionHookPayload) error {
		<-ctx.Done()
		return ctx.Err()
	}

	dispatchCompletionHookAsync(CompletionHookPayload{
		TaskID:            taskID,
		TenantID:          tenantID,
		Status:            model.TaskStatusFailed,
		LastError:         "task failed",
		ResultAvailable:   false,
		CompletionHookURL: "http://example.test/hooks/task-finished",
	})

	var stored model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&stored).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if stored.HookDeliveredAt != nil {
		t.Fatalf("HookDeliveredAt = %v, want nil", stored.HookDeliveredAt)
	}
	if !strings.Contains(stored.HookLastError, "context deadline exceeded") {
		t.Fatalf("HookLastError = %q, want dispatch timeout error", stored.HookLastError)
	}
}

func TestRecordCompletionHookResultDoesNotOverwriteDeliveredTask(t *testing.T) {
	ctx := context.Background()
	db := setupTaskServiceTestDB(t)

	tenantID := uuid.New()
	taskID := uuid.New()
	deliveredAt := time.Now().Add(-time.Minute)
	mustCreateTenant(t, ctx, db, tenantID)
	mustCreateTask(t, ctx, db, model.Task{
		BaseUUIDModel:     model.BaseUUIDModel{ID: taskID},
		TenantID:          tenantID,
		Status:            model.TaskStatusSucceeded,
		CompletionHookURL: "http://example.test/hooks/task-finished",
		HookDeliveredAt:   &deliveredAt,
		HookLastError:     "",
	})

	if err := recordCompletionHookResult(ctx, taskID, errors.New("late retry failed")); err != nil {
		t.Fatalf("recordCompletionHookResult() error = %v", err)
	}

	var stored model.Task
	if err := db.WithContext(ctx).Where("id = ?", taskID).First(&stored).Error; err != nil {
		t.Fatalf("load task: %v", err)
	}
	if stored.HookDeliveredAt == nil {
		t.Fatal("HookDeliveredAt = nil, want original delivery time preserved")
	}
	if !stored.HookDeliveredAt.Equal(deliveredAt) {
		t.Fatalf("HookDeliveredAt = %v, want %v", stored.HookDeliveredAt, deliveredAt)
	}
	if stored.HookLastError != "" {
		t.Fatalf("HookLastError = %q, want unchanged empty value", stored.HookLastError)
	}
}

func restoreHookDispatchGlobals(t *testing.T) {
	t.Helper()

	prevDispatcher := hookDispatcher
	prevAsync := runHookDispatchAsync
	prevDispatchTimeout := completionHookDispatchTimeout
	prevRecordTimeout := completionHookRecordTimeout
	t.Cleanup(func() {
		hookDispatcher = prevDispatcher
		runHookDispatchAsync = prevAsync
		completionHookDispatchTimeout = prevDispatchTimeout
		completionHookRecordTimeout = prevRecordTimeout
	})
}
