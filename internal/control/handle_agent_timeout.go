package control

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

// HandleAgentTimeout is the callback invoked by agent_service when a heartbeat
// or grace timer fires. It rolls back the agent's shard in the DB.
func (server *Server) HandleAgentTimeout(agentID string, reason string) {
	if server.db == nil {
		return
	}

	agent, err := server.agentService.GetAgent(agentID)
	if err != nil {
		return // agent already removed
	}

	ctx := context.Background()
	if agent.CurrentShardID != nil && *agent.CurrentShardID != "" {
		shardID, parseErr := uuid.Parse(*agent.CurrentShardID)
		if parseErr == nil {
			var task model.Task
			if err := server.db.WithContext(ctx).
				Model(&model.Task{}).
				Select("id", "status").
				Joins("JOIN task_shards ON task_shards.task_id = tasks.id").
				Where("task_shards.id = ?", shardID).
				First(&task).Error; err != nil {
				server.logger.Error("load task for timed-out shard rollback",
					zap.Stringer("shard_id", shardID), zap.Error(err))
			} else if err := server.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				if err := task_shard_orm.UpdateShardStatus(ctx, tx, task_shard_orm.UpdateShardStatusParams{
					ShardID:         shardID,
					Status:          model.ShardStatusQueued,
					CurrentStatuses: []string{model.ShardStatusLeased, model.ShardStatusRunning, model.ShardStatusResultReady},
					ClearAgent:      true,
				}); err != nil {
					return err
				}
				if task.Status == model.TaskStatusWaitingForAgents {
					if err := task_orm.RollbackTaskToWaiting(ctx, tx, task.ID); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
						return err
					}
				}
				return nil
			}); err != nil {
				server.logger.Error("rollback timed-out shard failed",
					zap.String("agent_id", agentID),
					zap.Stringer("shard_id", shardID),
					zap.Error(err))
			} else {
				server.logger.Warn("rolled back timed-out shard",
					zap.String("agent_id", agentID),
					zap.Stringer("shard_id", shardID),
					zap.String("reason", reason),
				)
			}
		}
	}

	// For grace_expired, the agent is still DISCONNECTED. Mark as TIMED_OUT
	// so it's excluded from active tenant counts.
	if reason == "grace_expired" {
		if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
			AgentID:        agentID,
			Status:         agent_service.AgentStatusTimedOut,
			CurrentShardID: nil,
		}); err != nil {
			server.logger.Warn("mark grace-expired agent as timed out",
				zap.String("agent_id", agentID),
				zap.Error(err),
			)
		}
	}
}
