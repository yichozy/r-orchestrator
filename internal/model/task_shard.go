package model

import (
	"time"

	"github.com/google/uuid"
)

const (
	ShardStatusQueued      = "QUEUED"
	ShardStatusLeased      = "LEASED"
	ShardStatusRunning     = "RUNNING"
	ShardStatusResultReady = "RESULT_READY"
	ShardStatusSucceeded   = "SUCCEEDED"
	ShardStatusFailed      = "FAILED"
	ShardStatusCancelled   = "CANCELLED"
)

type TaskShard struct {
	BaseUUIDModel
	TaskID          uuid.UUID  `gorm:"column:task_id;not null;type:uuid"`
	ShardIndex      int        `gorm:"column:shard_index;not null"`
	Status          string     `gorm:"column:status;not null"`
	AssignedAgentID string     `gorm:"column:assigned_agent_id;type:varchar(255)"`
	StartedAt       *time.Time `gorm:"column:started_at"`
	FinishedAt      *time.Time `gorm:"column:finished_at"`
	LastError       string     `gorm:"column:last_error;not null;default:''"`
}

func (TaskShard) TableName() string { return "task_shards" }
