package task_shard_orm

import (
	"context"
	"fmt"
	"time"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

// ResetTimedOutShards resets LEASED/RUNNING/RESULT_READY shards whose updated_at is before
// threshold and whose assigned agent is not in activeAgentIDs back to QUEUED.
func ResetTimedOutShards(ctx context.Context, db *gorm.DB, activeAgentIDs []string, threshold time.Time) (int, error) {
	query := db.WithContext(ctx).
		Model(&model.TaskShard{}).
		Where("status IN ?", []string{model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady}).
		Where("updated_at < ?", threshold)

	if len(activeAgentIDs) > 0 {
		query = query.Where("assigned_agent_id NOT IN ?", activeAgentIDs)
	}

	result := query.Updates(map[string]any{
		"status":            model.ShardStatusQueued,
		"assigned_agent_id": nil,
		"started_at":        nil,
		"finished_at":       nil,
		"last_error":        "",
	})
	if result.Error != nil {
		return 0, fmt.Errorf("reset timed out shards: %w", result.Error)
	}

	return int(result.RowsAffected), nil
}
