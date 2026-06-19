package shard_orm

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, shard model.TaskShard) error {
	return task_shard_orm.Create(ctx, db, shard)
}

func GetByID(ctx context.Context, db *gorm.DB, shardID uuid.UUID) (model.TaskShard, error) {
	return task_shard_orm.GetByID(ctx, db, shardID)
}

func CancelShards(ctx context.Context, db *gorm.DB, taskID uuid.UUID) error {
	return task_shard_orm.CancelTaskShardsByTaskId(ctx, db, taskID)
}
