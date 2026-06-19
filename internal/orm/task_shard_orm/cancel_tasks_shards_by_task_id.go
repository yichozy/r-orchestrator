package task_shard_orm

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func CancelTaskShardsByTaskId(ctx context.Context, db *gorm.DB, task_id uuid.UUID) error {
	now := time.Now()
	result := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("task_id = ?", task_id).
		Where("status NOT IN ?", []string{model.ShardStatusSucceeded, model.ShardStatusFailed, model.ShardStatusCancelled}).
		Updates(map[string]any{
			"status":            model.ShardStatusCancelled,
			"finished_at":       now,
			"assigned_agent_id": nil,
			"started_at":        nil,
			"last_error":        "",
		})
	if result.Error != nil {
		return fmt.Errorf("cancel shards: %w", result.Error)
	}
	return nil
}
