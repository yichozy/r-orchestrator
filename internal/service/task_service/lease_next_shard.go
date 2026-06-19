package task_service

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"gorm.io/gorm"
)

func LeaseNextShard(ctx context.Context, tenantID uuid.UUID, backendName string, agentID string) (model.Task, model.TaskShard, error) {
	db, err := orm.GetDB()
	if err != nil {
		return model.Task{}, model.TaskShard{}, err
	}

	var leasedTask model.Task
	var leasedShard model.TaskShard

	shardQueryStatuses := []string{
		model.TaskStatusWaitingForAgents,
		model.TaskStatusQueued,
		model.TaskStatusRunning,
	}

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		const maxRetries = 100
		for i := 0; i < maxRetries; i++ {
			var shard model.TaskShard
			if err := tx.Model(&model.TaskShard{}).
				Joins("JOIN tasks ON tasks.id = task_shards.task_id").
				Joins("JOIN tenants ON tenants.id = tasks.tenant_id").
				Where("tasks.tenant_id = ?", tenantID).
				Where("tasks.status IN ?", shardQueryStatuses).
				Where("tenants.primary_backend_name = ?", backendName).
				Where("task_shards.status = ?", model.ShardStatusQueued).
				Order("tasks.created_at asc").
				Order("task_shards.shard_index asc").
				Take(&shard).Error; err != nil {
				return err
			}

			var task model.Task
			if err := tx.WithContext(ctx).
				Where("tenant_id = ? AND id = ?", tenantID, shard.TaskID).
				First(&task).Error; err != nil {
				return fmt.Errorf("load shard task: %w", err)
			}

			if task.Status == model.TaskStatusWaitingForAgents {
				if err := tx.Model(&model.Task{}).
					Where("id = ? AND status = ?", task.ID, model.TaskStatusWaitingForAgents).
					Update("status", model.TaskStatusQueued).Error; err != nil {
					return fmt.Errorf("promote task to queued: %w", err)
				}
				task.Status = model.TaskStatusQueued
			}

			// Re-check task status before leasing: the task may have been
			// cancelled between the initial SELECT and this UPDATE.
			taskActive := false
			for _, s := range shardQueryStatuses {
				if task.Status == s {
					taskActive = true
					break
				}
			}
			if !taskActive {
				// Task was cancelled; retry to find a different shard.
				continue
			}

			result := tx.Model(&model.TaskShard{}).
				Where("id = ? AND status = ?", shard.ID, model.ShardStatusQueued).
				Updates(map[string]any{
					"status":            model.ShardStatusLeased,
					"assigned_agent_id": agentID,
				})
			if result.Error != nil {
				return fmt.Errorf("lease shard: %w", result.Error)
			}
			if result.RowsAffected == 0 {
				// Another agent leased this shard between SELECT and UPDATE.
				// Retry within the same transaction to find the next queued shard.
				continue
			}

			shard.Status = model.ShardStatusLeased
			shard.AssignedAgentID = agentID
			leasedTask = task
			leasedShard = shard
			return nil
		}
		return fmt.Errorf("lease next shard: exhausted %d retry attempts", maxRetries)
	})
	if err != nil {
		return model.Task{}, model.TaskShard{}, fmt.Errorf("lease next shard: %w", err)
	}

	return leasedTask, leasedShard, nil
}
