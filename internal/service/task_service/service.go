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
var ErrTaskNotSucceeded = errors.New("task is not succeeded")
var ErrTaskResultNotFound = errors.New("task result not found")

type SubmitTaskParams struct {
	TenantName        string
	ZipBytes          []byte
	CSVBytes          []byte
	CompletionHookURL string
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

type TaskResultCSVView struct {
	TaskID      uuid.UUID
	Filename    string
	ContentType string
	CSVContent  string
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

// SetHookDispatcher 设置 completion hook 派发回调，测试中可注入 stub。
func SetHookDispatcher(f func(context.Context, CompletionHookPayload) error) {
	hookDispatcher = f
}
