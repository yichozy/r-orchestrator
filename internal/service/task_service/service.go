package task_service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

// notifyCancelShard 由控制面在启动时注入，取消任务时通知 agent 取消正在运行的 shard。
var notifyCancelShard func(ctx context.Context, agentID string, shardID uuid.UUID) error

var ErrTaskNotFound = errors.New("task not found")
var ErrTenantNotFound = errors.New("tenant not found")

type SubmitTaskParams struct {
	TenantName        string
	ZipBytes          []byte
	CompletionHookURL string
}

type CompletionHookPayload struct {
	TaskID            uuid.UUID  `json:"task_id"`
	TenantID          uuid.UUID  `json:"tenant_id"`
	Status            string     `json:"status"`
	LastError         string     `json:"last_error"`
	FinishedAt        *time.Time `json:"finished_at"`
	ResultAvailable   bool       `json:"result_available"`
	CompletionHookURL string     `json:"-"`
}

var hookDispatcher func(context.Context, CompletionHookPayload) error

var runHookDispatchAsync = func(fn func()) {
	go fn()
}

// SetNotifyCancelShard 设置取消通知回调。
func SetNotifyCancelShard(f func(ctx context.Context, agentID string, shardID uuid.UUID) error) {
	notifyCancelShard = f
}
