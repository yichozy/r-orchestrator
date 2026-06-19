package task_service

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"github.com/yichozy/r-orchestrator/internal/util"
	"gorm.io/gorm"
)

func SubmitTask(ctx context.Context, params SubmitTaskParams) (uuid.UUID, error) {
	db, err := orm.GetDB()
	if err != nil {
		return uuid.Nil, err
	}

	hook := strings.TrimSpace(params.CompletionHookURL)
	if hook != "" {
		parsed, err := url.Parse(hook)
		if err != nil {
			return uuid.Nil, fmt.Errorf("invalid completion hook url: %w", err)
		}
		if !parsed.IsAbs() || parsed.Host == "" {
			return uuid.Nil, fmt.Errorf("invalid completion hook url: must be an absolute http(s) url")
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return uuid.Nil, fmt.Errorf("invalid completion hook url: unsupported scheme %q", parsed.Scheme)
		}
		params.CompletionHookURL = parsed.String()
	} else {
		params.CompletionHookURL = ""
	}

	tenant, err := tenant_orm.GetByName(ctx, db, params.TenantName)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: %s", ErrTenantNotFound, params.TenantName)
	}

	shard_csv_rows, err := util.SplitCSVRows(params.CSVBytes, tenant.MaxAgents)
	if err != nil {
		return uuid.Nil, fmt.Errorf("split csv: %w", err)
	}

	task_id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate task id: %w", err)
	}
	bundle_artifact_id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate bundle artifact id: %w", err)
	}
	input_csv_artifact_id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate input csv artifact id: %w", err)
	}

	task := model.Task{
		BaseUUIDModel:      model.BaseUUIDModel{ID: task_id},
		TenantID:           tenant.ID,
		Status:             model.TaskStatusPending,
		BundleArtifactID:   bundle_artifact_id,
		InputCSVArtifactID: input_csv_artifact_id,
		CompletionHookURL:  params.CompletionHookURL,
		ShardCount:         len(shard_csv_rows),
	}
	bundle_artifact := model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: bundle_artifact_id},
		TenantID:      tenant.ID,
		TaskID:        task_id,
		ArtifactType:  model.ArtifactTypeBundle,
		ContentBytes:  append([]byte(nil), params.ZipBytes...),
		ContentSize:   int64(len(params.ZipBytes)),
		SHA256:        util.SumSHA256(params.ZipBytes),
	}
	input_artifact := model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: input_csv_artifact_id},
		TenantID:      tenant.ID,
		TaskID:        task_id,
		ArtifactType:  model.ArtifactTypeInputCSV,
		ContentBytes:  append([]byte(nil), params.CSVBytes...),
		ContentSize:   int64(len(params.CSVBytes)),
		SHA256:        util.SumSHA256(params.CSVBytes),
	}

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := task_orm.Create(ctx, tx, task); err != nil {
			return err
		}
		if err := artifact_orm.Create(ctx, tx, bundle_artifact); err != nil {
			return err
		}
		if err := artifact_orm.Create(ctx, tx, input_artifact); err != nil {
			return err
		}

		for shard_index := range shard_csv_rows {
			if err := task_shard_orm.Create(ctx, tx, model.TaskShard{
				TaskID:     task_id,
				ShardIndex: shard_index,
				Status:     model.ShardStatusQueued,
			}); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return uuid.Nil, err
	}

	return task_id, nil
}
