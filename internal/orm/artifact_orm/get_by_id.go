package artifact_orm

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"gorm.io/gorm"
)

func GetById(ctx context.Context, db *gorm.DB, id uuid.UUID) (model.Artifact, error) {
	var artifact model.Artifact
	if err := db.WithContext(ctx).Where("id = ?", id).First(&artifact).Error; err != nil {
		return model.Artifact{}, fmt.Errorf("get artifact: %w", err)
	}

	return artifact, nil
}
