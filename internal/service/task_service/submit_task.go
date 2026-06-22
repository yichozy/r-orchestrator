package task_service

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"sort"
	"strings"

	"github.com/google/uuid"
	aliyun "github.com/yichozy/hopebox/aliyun"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
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

	// Extract and validate zip contents.
	scripts, err := extractScriptsFromZip(params.ZipBytes)
	if err != nil {
		return uuid.Nil, err
	}

	taskID, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, fmt.Errorf("generate task id: %w", err)
	}

	// Upload bundle to OSS.
	ossClient, err := aliyun.NewOss()
	if err != nil {
		return uuid.Nil, fmt.Errorf("init oss: %w", err)
	}
	bundleKey := fmt.Sprintf("r-orchestrator/tasks/%s/bundle.zip", taskID)
	if err := ossClient.UploadBytes(ctx, bundleKey, params.ZipBytes); err != nil {
		return uuid.Nil, fmt.Errorf("upload bundle to oss: %w", err)
	}

	task := model.Task{
		BaseUUIDModel:    model.BaseUUIDModel{ID: taskID},
		TenantID:         tenant.ID,
		Status:           model.TaskStatusPending,
		CompletionHookURL: params.CompletionHookURL,
		ShardCount:       len(scripts),
	}

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := task_orm.Create(ctx, tx, task); err != nil {
			return err
		}

		for _, scriptName := range scripts {
			if err := task_shard_orm.Create(ctx, tx, model.TaskShard{
				TaskID:     taskID,
				ScriptName: scriptName,
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

	return taskID, nil
}

// extractScriptsFromZip opens the zip and extracts script names from cmd/*.sh.
// Also validates that install.sh exists at the root.
func extractScriptsFromZip(zipBytes []byte) ([]string, error) {
	reader, err := zip.NewReader(bytes.NewReader(zipBytes), int64(len(zipBytes)))
	if err != nil {
		return nil, fmt.Errorf("invalid zip: %w", err)
	}

	var hasInstall bool
	var scripts []string

	for _, f := range reader.File {
		if f.FileInfo().IsDir() {
			continue
		}

		switch path.Clean(f.Name) {
		case "install.sh":
			hasInstall = true
		}

		dir, name := path.Split(f.Name)
		if path.Clean(dir) == "cmd" && strings.HasSuffix(name, ".sh") {
			scripts = append(scripts, name)
		}
	}

	if !hasInstall {
		return nil, fmt.Errorf("bundle must contain install.sh at root")
	}
	if len(scripts) == 0 {
		return nil, fmt.Errorf("bundle must contain at least one .sh script in cmd/ directory")
	}

	sort.Strings(scripts)
	return scripts, nil
}
