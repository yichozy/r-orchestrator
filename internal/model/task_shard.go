package model

import (
	"time"

	"github.com/google/uuid"
)

const (
	ShardStatusQueued    = "QUEUED"
	ShardStatusLeased    = "LEASED"
	ShardStatusRunning   = "RUNNING"
	ShardStatusSucceeded = "SUCCEEDED"
	ShardStatusFailed    = "FAILED"
	ShardStatusCancelled = "CANCELLED"
)

type TaskShard struct {
	BaseUUIDModel
	TaskID          uuid.UUID  `json:"task_id" gorm:"column:task_id;not null;type:uuid"`
	ScriptName      string     `json:"script_name" gorm:"column:script_name;not null;default:''"`
	Status          string     `json:"status" gorm:"column:status;not null"`
	AssignedAgentID string     `json:"assigned_agent_id" gorm:"column:assigned_agent_id;type:varchar(255)"`
	OutputOSSKey    string     `json:"output_oss_key" gorm:"column:output_oss_key;not null;default:''"`
	OutputSHA256    string     `json:"output_sha256" gorm:"column:output_sha256;not null;default:''"`
	StartedAt       *time.Time `json:"started_at" gorm:"column:started_at"`
	FinishedAt      *time.Time `json:"finished_at" gorm:"column:finished_at"`
	LastError       string     `json:"last_error" gorm:"column:last_error;not null;default:''"`
}

func (TaskShard) TableName() string { return "task_shards" }
