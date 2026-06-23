package model

import (
	"time"

	"github.com/google/uuid"
)

const (
	TaskStatusPending          = "PENDING"
	TaskStatusWaitingForAgents = "WAITING_FOR_AGENTS"
	TaskStatusQueued           = "QUEUED"
	TaskStatusRunning          = "RUNNING"
	TaskStatusSucceeded        = "SUCCEEDED"
	TaskStatusFailed           = "FAILED"
	TaskStatusCancelled        = "CANCELLED"
)

type Task struct {
	BaseUUIDModel
	TenantID          uuid.UUID  `json:"tenant_id" gorm:"column:tenant_id;not null;type:uuid"`
	Status            string     `json:"status" gorm:"column:status;not null"`
	CompletionHookURL string     `json:"completion_hook_url" gorm:"column:completion_hook_url;not null;default:''"`
	ShardCount        int        `json:"shard_count" gorm:"column:shard_count;not null;default:0"`
	StartedAt         *time.Time `json:"started_at" gorm:"column:started_at"`
	FinishedAt        *time.Time `json:"finished_at" gorm:"column:finished_at"`
	HookDeliveredAt   *time.Time `json:"hook_delivered_at" gorm:"column:hook_delivered_at"`
	HookLastError     string     `json:"hook_last_error" gorm:"column:hook_last_error;not null;default:''"`
	LastError         string     `json:"last_error" gorm:"column:last_error;not null;default:''"`
	Shards            []TaskShard
}

func (Task) TableName() string { return "tasks" }
