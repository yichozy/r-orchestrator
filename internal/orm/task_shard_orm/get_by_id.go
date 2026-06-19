package task_shard_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetByID(ctx context.Context, db *gorm.DB, shardID uuid.UUID) (model.TaskShard, error) {
	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		return model.TaskShard{}, fmt.Errorf("get shard: %w", err)
	}
	return shard, nil
}
