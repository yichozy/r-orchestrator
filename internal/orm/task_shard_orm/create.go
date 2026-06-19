package task_shard_orm

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, shard model.TaskShard) error {
	if err := db.WithContext(ctx).Create(&shard).Error; err != nil {
		return fmt.Errorf("create shard: %w", err)
	}

	return nil
}
