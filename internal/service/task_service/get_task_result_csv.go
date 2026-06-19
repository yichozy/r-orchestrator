package task_service

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"gorm.io/gorm"
)

func GetTaskResultCSV(ctx context.Context, tenantName string, taskID uuid.UUID) (TaskResultCSVView, error) {
	db, err := orm.GetDB()
	if err != nil {
		return TaskResultCSVView{}, err
	}

	tenant, err := tenant_orm.GetByName(ctx, db, tenantName)
	if err != nil {
		return TaskResultCSVView{}, fmt.Errorf("%w: %s", ErrTenantNotFound, tenantName)
	}

	task, err := task_orm.GetByID(ctx, db, taskID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return TaskResultCSVView{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
		}
		return TaskResultCSVView{}, err
	}
	if task.TenantID != tenant.ID {
		return TaskResultCSVView{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if task.Status != model.TaskStatusSucceeded {
		return TaskResultCSVView{}, fmt.Errorf("%w: task %s status %s", ErrTaskNotSucceeded, task.ID, task.Status)
	}
	if task.ResultArtifactID == nil {
		return TaskResultCSVView{}, fmt.Errorf("%w: task %s", ErrTaskResultNotFound, task.ID)
	}

	artifact, err := artifact_orm.GetById(ctx, db, *task.ResultArtifactID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return TaskResultCSVView{}, fmt.Errorf("%w: task %s", ErrTaskResultNotFound, task.ID)
		}
		return TaskResultCSVView{}, err
	}
	if artifact.TaskID != task.ID || artifact.TenantID != tenant.ID || artifact.ArtifactType != model.ArtifactTypeTaskOutput {
		return TaskResultCSVView{}, fmt.Errorf("%w: task %s", ErrTaskResultNotFound, task.ID)
	}

	return TaskResultCSVView{
		TaskID:      task.ID,
		Filename:    fmt.Sprintf("task-%s-result.csv", task.ID),
		ContentType: "text/csv; charset=utf-8",
		CSVContent:  string(artifact.ContentBytes),
	}, nil
}
