package artifact_orm

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// ExistsShardOutput checks whether a shard output artifact exists for the
// given task and shard index.
func ExistsShardOutput(ctx context.Context, db *gorm.DB, taskID uuid.UUID, shardIndex int) (bool, error) {
	var count int64
	err := db.WithContext(ctx).
		Model(&model.Artifact{}).
		Where("task_id = ?", taskID).
		Where("artifact_type = ?", model.ArtifactTypeShardOutput).
		Where("shard_index = ?", shardIndex).
		Count(&count).Error
	return count > 0, err
}
