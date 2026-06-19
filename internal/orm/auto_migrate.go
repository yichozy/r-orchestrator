package orm

import (
	"fmt"

	"gorm.io/gorm"

	"github.com/yichozy/r-orchestrator/internal/model"
)

func AutoMigrate(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("gorm db is required")
	}

	if err := db.AutoMigrate(
		&model.Tenant{},
		&model.Task{},
		&model.Artifact{},
		&model.TaskShard{},
		&model.Cluster{},
	); err != nil {
		return fmt.Errorf("auto migrate gorm models: %w", err)
	}

	return nil
}
