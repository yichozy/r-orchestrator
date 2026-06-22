package task_service

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetTaskScripts(ctx context.Context, db *gorm.DB, taskID uuid.UUID) ([]TaskScriptView, error) {
	var shards []model.TaskShard
	if err := db.WithContext(ctx).
		Where("task_id = ?", taskID).
		Order("script_name ASC").
		Find(&shards).Error; err != nil {
		return nil, err
	}

	result := make([]TaskScriptView, len(shards))
	for i, s := range shards {
		result[i] = TaskScriptView{
			ScriptName:    s.ScriptName,
			Status:        s.Status,
			OutputOSSKey:  s.OutputOSSKey,
			OutputSHA256:  s.OutputSHA256,
			ErrorMessage:  s.LastError,
			StartedAt:     s.StartedAt,
			FinishedAt:    s.FinishedAt,
		}
	}
	return result, nil
}
