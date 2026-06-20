package control

import (
	"errors"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_shard_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

// HandleReconnectedAgent validates a reconnecting agent's in-memory state against
// the database, then drives the post-registration state machine. Agents have no
// PVC so state is lost on restart. If the agent is RESULT_READY with a valid shard,
// send FetchShardResult. If IDLE, scan for orphaned RESULT_READY shards first, then
// try to assign new work.
func (server *Server) HandleReconnectedAgent(
	streamRef *agentStream,
	agent agent_service.Agent,
	agentID string,
	tenantID uuid.UUID,
	backendName string,
) (agent_service.Agent, error) {
	ctx := streamRef.Context()

	// Validate RUNNING/RESULT_READY state against DB.
	if agent.Status == agent_service.AgentStatusRunning || agent.Status == agent_service.AgentStatusResultReady {
		if agent.CurrentShardID == nil || *agent.CurrentShardID == "" {
			agent, _ = server.ResetRegisteredAgentToIdle(agent.ID)
		} else {
			shardID, err := uuid.Parse(*agent.CurrentShardID)
			if err != nil {
				agent, _ = server.ResetRegisteredAgentToIdle(agent.ID)
			} else {
				shard, err := server.loadValidatedShard(ctx, shardID, agent.ID, agent.TenantID, agent.BackendName)
				if err != nil {
					switch status.Code(err) {
					case codes.NotFound, codes.PermissionDenied:
						agent, _ = server.ResetRegisteredAgentToIdle(agent.ID)
					default:
						return agent_service.Agent{}, err
					}
				} else {
					switch agent.Status {
					case agent_service.AgentStatusResultReady:
						if shard.Status == model.ShardStatusResultReady {
							// Valid, continue to post-validation
						} else if shard.Status == model.ShardStatusSucceeded {
							hasOutput, err := artifact_orm.ExistsShardOutput(ctx, server.db, shard.TaskID, shard.ShardIndex)
							if err != nil {
								return agent_service.Agent{}, status.Errorf(codes.Internal, "check shard output artifact: %v", err)
							}
							if hasOutput {
								// Valid, continue to post-validation
							} else {
								agent, _ = server.ResetRegisteredAgentToIdle(agent.ID)
							}
						} else {
							agent, _ = server.ResetRegisteredAgentToIdle(agent.ID)
						}
					case agent_service.AgentStatusRunning:
						if shard.Status != model.ShardStatusLeased && shard.Status != model.ShardStatusRunning {
							agent, _ = server.ResetRegisteredAgentToIdle(agent.ID)
						}
					}
				}
			}
		}
	}

	// Post-validation state machine: fetch results or assign work.
	if agent.Status == agent_service.AgentStatusResultReady && agent.CurrentShardID != nil {
		return agent, nil
	}
	if !(agent.Status == agent_service.AgentStatusIdle && agent.CurrentShardID == nil) {
		return agent, nil
	}

	// The agent may have crashed between ReportShardStatus(RESULT_READY) and
	// HeartbeatAgent, leaving a RESULT_READY shard in DB with no agent record.
	if server.db != nil {
		shard, err := task_shard_orm.GetResultReadyTaskShardByAgent(ctx, server.db, agentID)
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return agent_service.Agent{}, status.Errorf(codes.Internal, "restore result-ready shard: %v", err)
		}
		if err == nil {
			shardID := shard.ID.String()
			if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
				AgentID:        agentID,
				Status:         agent_service.AgentStatusResultReady,
				CurrentShardID: &shardID,
			}); err != nil {
				return agent_service.Agent{}, status.Errorf(codes.Internal, "restore result-ready agent heartbeat: %v", err)
			}
			server.logger.Info("restored result-ready agent state from DB",
				zap.String("agent_id", agentID),
				zap.Stringer("shard_id", shard.ID),
			)
			agent, err = server.agentService.GetAgent(agentID)
			if err != nil {
				return agent_service.Agent{}, status.Errorf(codes.Internal, "get restored agent: %v", err)
			}
			if agent.Status == agent_service.AgentStatusResultReady && agent.CurrentShardID != nil {
				return agent, nil
			}
		}
	}
	if agent.Status == agent_service.AgentStatusIdle && agent.CurrentShardID == nil {
		if err := server.tryAssignShard(streamRef, agentID, tenantID, backendName); err != nil {
			return agent_service.Agent{}, err
		}
	}
	return agent, nil
}

func (server *Server) ResetRegisteredAgentToIdle(agentID string) (agent_service.Agent, error) {
	if err := server.agentService.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "reset stale agent state: %v", err)
	}

	agent, err := server.agentService.GetAgent(agentID)
	if err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "get reset agent: %v", err)
	}

	return agent, nil
}
