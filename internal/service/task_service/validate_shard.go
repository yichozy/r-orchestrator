package task_service

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

// LoadValidatedShard loads a shard by ID and validates that the shard exists,
// is assigned to the given agent, and its parent task belongs to the given
// tenant and backend. Returns a gRPC-status error on any validation failure.
func LoadValidatedShard(
	ctx context.Context,
	shardID uuid.UUID,
	agentID string,
	tenantID uuid.UUID,
	backendName string,
) (model.TaskShard, error) {
	db, err := orm.GetDB()
	if err != nil {
		return model.TaskShard{}, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	if shardID == uuid.Nil {
		return model.TaskShard{}, status.Error(codes.InvalidArgument, "shard_id is required")
	}

	shard, err := task_shard_orm.GetByID(ctx, db, shardID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.TaskShard{}, status.Errorf(codes.NotFound, "shard %s not found", shardID)
		}
		return model.TaskShard{}, status.Errorf(codes.Internal, "load shard %s: %v", shardID, err)
	}
	if shard.AssignedAgentID != agentID {
		switch shard.Status {
		case model.ShardStatusCancelled, model.ShardStatusFailed, model.ShardStatusSucceeded:
		default:
			return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s is assigned to agent %s, not %s", shardID, shard.AssignedAgentID, agentID)
		}
	}

	owner, err := task_shard_orm.GetShardTaskOwner(ctx, db, shard.TaskID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return model.TaskShard{}, status.Errorf(codes.NotFound, "task %s not found", shard.TaskID)
		}
		return model.TaskShard{}, status.Errorf(codes.Internal, "load shard task %s: %v", shard.TaskID, err)
	}
	if owner.TenantID != tenantID {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s belongs to tenant %s, not %s", shardID, owner.TenantID, tenantID)
	}
	if owner.PrimaryBackendName != backendName {
		return model.TaskShard{}, status.Errorf(codes.PermissionDenied, "shard %s belongs to backend %s, not %s", shardID, owner.PrimaryBackendName, backendName)
	}

	return shard, nil
}

// ValidateShardReport loads and validates a shard, then checks that its status
// matches the required status for the given action.
func ValidateShardReport(
	ctx context.Context,
	shardID uuid.UUID,
	agentID string,
	tenantID uuid.UUID,
	backendName, requiredStatus, action string,
) error {
	shard, err := LoadValidatedShard(ctx, shardID, agentID, tenantID, backendName)
	if err != nil {
		return err
	}
	if shard.Status != requiredStatus {
		return status.Errorf(codes.FailedPrecondition, "%s requires shard %s to be %s, got %s", action, shardID, requiredStatus, shard.Status)
	}
	return nil
}

// RestoreResultReadyShard finds a RESULT_READY shard assigned to the given agent.
// Returns the shard ID if found, or nil (no error) if no such shard exists.
func RestoreResultReadyShard(ctx context.Context, agentID string) (*uuid.UUID, error) {
	db, err := orm.GetDB()
	if err != nil {
		return nil, err
	}

	shard, err := task_shard_orm.GetResultReadyTaskShardByAgent(ctx, db, agentID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	shardID := shard.ID
	return &shardID, nil
}
