package artifact_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func ListByTaskAndType(ctx context.Context, db *gorm.DB, taskID uuid.UUID, artifactType string) ([]model.Artifact, error) {
	var artifacts []model.Artifact
	if err := db.WithContext(ctx).
		Where("task_id = ? AND artifact_type = ?", taskID, artifactType).
		Order("shard_index asc").
		Find(&artifacts).Error; err != nil {
		return nil, fmt.Errorf("list artifacts by task and type: %w", err)
	}

	return artifacts, nil
}
