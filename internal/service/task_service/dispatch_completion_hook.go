package task_service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"go.uber.org/zap"
)

var defaultHookHTTPClient = &http.Client{Timeout: 10 * time.Second}
var completionHookDispatchTimeout = 15 * time.Second
var completionHookRecordTimeout = 5 * time.Second

func dispatchCompletionHook(ctx context.Context, client *http.Client, payload CompletionHookPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal completion hook payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, payload.CompletionHookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build completion hook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("send completion hook request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("completion hook returned status %s", resp.Status)
	}

	return nil
}

func dispatchCompletionHookAsync(payload CompletionHookPayload) {
	if payload.CompletionHookURL == "" {
		return
	}

	dispatcher := hookDispatcher
	if dispatcher == nil {
		dispatcher = func(ctx context.Context, payload CompletionHookPayload) error {
			return dispatchCompletionHook(ctx, defaultHookHTTPClient, payload)
		}
	}

	runHookDispatchAsync(func() {
		dispatchCtx, dispatchCancel := context.WithTimeout(context.Background(), completionHookDispatchTimeout)
		err := dispatcher(dispatchCtx, payload)
		dispatchCancel()

		recordCtx, recordCancel := context.WithTimeout(context.Background(), completionHookRecordTimeout)
		recordErr := recordCompletionHookResult(recordCtx, payload.TaskID, err)
		recordCancel()
		if recordErr != nil {
			zap.L().Named("task_service").Warn("record completion hook result failed",
				zap.Stringer("task_id", payload.TaskID),
				zap.Error(recordErr),
			)
		}
		if err != nil {
			zap.L().Named("task_service").Warn("dispatch completion hook failed",
				zap.Stringer("task_id", payload.TaskID),
				zap.String("hook_url", payload.CompletionHookURL),
				zap.Error(err),
			)
			return
		}
		if recordErr == nil {
			zap.L().Named("task_service").Info("completion hook dispatched",
				zap.Stringer("task_id", payload.TaskID),
				zap.String("hook_url", payload.CompletionHookURL),
				zap.String("status", payload.Status),
				zap.Bool("result_available", payload.ResultAvailable),
			)
		}
	})
}

func recordCompletionHookResult(ctx context.Context, taskID uuid.UUID, dispatchErr error) error {
	db, err := orm.GetDB()
	if err != nil {
		return err
	}

	updates := map[string]any{}
	query := db.WithContext(ctx).
		Model(&model.Task{}).
		Where("id = ?", taskID).
		Where("hook_delivered_at IS NULL")
	if dispatchErr != nil {
		updates["hook_delivered_at"] = nil
		updates["hook_last_error"] = dispatchErr.Error()
	} else {
		deliveredAt := time.Now().UTC()
		updates["hook_delivered_at"] = deliveredAt
		updates["hook_last_error"] = ""
	}

	if err := query.Updates(updates).Error; err != nil {
		return fmt.Errorf("update completion hook bookkeeping: %w", err)
	}

	return nil
}

func newCompletionHookPayload(task model.Task, status, lastError string, finishedAt *time.Time, resultAvailable bool) *CompletionHookPayload {
	if task.CompletionHookURL == "" {
		return nil
	}

	return &CompletionHookPayload{
		TaskID:            task.ID,
		TenantID:          task.TenantID,
		Status:            status,
		LastError:         lastError,
		FinishedAt:        cloneTimePtr(finishedAt),
		ResultAvailable:   resultAvailable,
		CompletionHookURL: task.CompletionHookURL,
	}
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}

	cloned := value.UTC()
	return &cloned
}
