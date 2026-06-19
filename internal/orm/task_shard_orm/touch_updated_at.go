package task_shard_orm

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// TouchShardUpdatedAt refreshes the updated_at timestamp of a shard whose
// status is one of the given active statuses. Returns false if the shard
// was not found or is no longer in an active state.
func TouchShardUpdatedAt(ctx context.Context, db *gorm.DB, shardID uuid.UUID, activeStatuses []string) (bool, error) {
	result := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("id = ?", shardID).
		Where("status IN ?", activeStatuses).
		Update("updated_at", time.Now().UTC())
	if result.Error != nil {
		return false, fmt.Errorf("touch shard updated_at: %w", result.Error)
	}
	return result.RowsAffected > 0, nil
}
