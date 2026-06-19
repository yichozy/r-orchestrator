package model

import (
	"github.com/google/uuid"
)

const (
	ArtifactTypeBundle      = "BUNDLE_ZIP"
	ArtifactTypeInputCSV    = "INPUT_CSV"
	ArtifactTypeShardOutput = "SHARD_OUTPUT"
	ArtifactTypeTaskOutput  = "TASK_OUTPUT_CSV"
)

type Artifact struct {
	BaseUUIDModel
	TenantID     uuid.UUID `gorm:"column:tenant_id;not null;type:uuid"`
	TaskID       uuid.UUID `gorm:"column:task_id;not null;type:uuid"`
	ArtifactType string    `gorm:"column:artifact_type;not null"`
	ContentBytes []byte    `gorm:"column:content_bytes;type:bytea;not null"`
	ContentSize  int64     `gorm:"column:content_size;not null"`
	SHA256       string    `gorm:"column:sha256;not null"`
	ShardIndex   *int      `gorm:"column:shard_index"`
}

func (Artifact) TableName() string { return "artifacts" }
