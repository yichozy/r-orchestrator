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
	TenantID           uuid.UUID  `gorm:"column:tenant_id;not null;type:uuid"`
	Status             string     `gorm:"column:status;not null"`
	BundleArtifactID   uuid.UUID  `gorm:"column:bundle_artifact_id;type:uuid"`
	InputCSVArtifactID uuid.UUID  `gorm:"column:input_csv_artifact_id;type:uuid"`
	ResultArtifactID   *uuid.UUID `gorm:"column:result_artifact_id;type:uuid"`
	CompletionHookURL  string     `gorm:"column:completion_hook_url;not null;default:''"`
	ShardCount         int        `gorm:"column:shard_count;not null;default:0"`
	StartedAt          *time.Time `gorm:"column:started_at"`
	FinishedAt         *time.Time `gorm:"column:finished_at"`
	HookDeliveredAt    *time.Time `gorm:"column:hook_delivered_at"`
	HookLastError      string     `gorm:"column:hook_last_error;not null;default:''"`
	LastError          string     `gorm:"column:last_error;not null;default:''"`
}

func (Task) TableName() string { return "tasks" }
