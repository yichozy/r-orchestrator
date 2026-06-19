package artifact_orm

import (
	"context"
	"fmt"

	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func Create(ctx context.Context, db *gorm.DB, artifact model.Artifact) error {
	if err := db.WithContext(ctx).Create(&artifact).Error; err != nil {
		return fmt.Errorf("create artifact: %w", err)
	}

	return nil
}

func CreateArtifact(ctx context.Context, db *gorm.DB, artifact model.Artifact) error {
	return Create(ctx, db, artifact)
}
