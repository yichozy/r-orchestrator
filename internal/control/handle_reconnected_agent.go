package control

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandleReconnectedAgent validates a reconnecting agent's in-memory state against
// the database, then drives the post-registration state machine. Agents have no
// PVC so state is lost on restart. If the agent is RESULT_READY with a valid shard,
// send FetchShardResult. If IDLE, scan for orphaned RESULT_READY shards first, then
// try to assign new work.
func (server *Server) HandleReconnectedAgent(sess *agentSession, agent agent_service.Agent) (agent_service.Agent, error) {
	ctx := sess.Context()

	// Validate RUNNING/RESULT_READY state against DB, reset to IDLE if stale.
	if agent.Status == agent_service.AgentStatusRunning || agent.Status == agent_service.AgentStatusResultReady {
		var err error
		if agent.CurrentShardID != nil && *agent.CurrentShardID != "" {
			agent, err = server.resolveAgentShardState(ctx, agent)
		} else {
			agent, err = server.resetRegisteredAgentToIdle(agent.ID)
		}
		if err != nil {
			return agent_service.Agent{}, err
		}
	}

	// Agent has work to resume — caller will send FetchShardResult.
	if agent.Status == agent_service.AgentStatusResultReady {
		return agent, nil
	}
	if agent.Status != agent_service.AgentStatusIdle {
		return agent, nil
	}

	// Agent crashed between ReportShardStatus(RESULT_READY) and HeartbeatAgent,
	// leaving a RESULT_READY shard in DB with no in-memory agent record.
	shardIDPtr, err := task_service.RestoreResultReadyShard(ctx, sess.agentID)
	if err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "restore result-ready shard: %v", err)
	}
	if shardIDPtr != nil {
		shardIDStr := (*shardIDPtr).String()
		if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
			AgentID:        sess.agentID,
			Status:         agent_service.AgentStatusResultReady,
			CurrentShardID: &shardIDStr,
		}); err != nil {
			return agent_service.Agent{}, status.Errorf(codes.Internal, "restore result-ready state: %v", err)
		}
		server.logger.Info("restored result-ready agent state from DB",
			zap.String("agent_id", sess.agentID),
			zap.Stringer("shard_id", *shardIDPtr),
		)
		agent, _ = agent_service.GetAgent(sess.agentID)
		return agent, nil
	}

	// IDLE with no orphaned work — try to assign new shard.
	return agent, server.TryAssignShard(sess)
}

// resolveAgentShardState checks a RUNNING/RESULT_READY agent's shard against the DB.
// Returns the agent unchanged if consistent, or reset to IDLE if inconsistent.
// Returns an error only on unexpected DB failures.
func (server *Server) resolveAgentShardState(ctx context.Context, agent agent_service.Agent) (agent_service.Agent, error) {
	shardID, err := uuid.Parse(*agent.CurrentShardID)
	if err != nil || shardID == uuid.Nil {
		return server.resetRegisteredAgentToIdle(agent.ID)
	}

	shard, err := task_service.LoadValidatedShard(ctx, shardID, agent.ID, agent.TenantID, agent.BackendName)
	if err != nil {
		code := status.Code(err)
		if code == codes.NotFound || code == codes.PermissionDenied {
			return server.resetRegisteredAgentToIdle(agent.ID)
		}
		return agent, err
	}

	switch agent.Status {
	case agent_service.AgentStatusResultReady:
		if shard.Status == model.ShardStatusResultReady {
			return agent, nil
		}
		if shard.Status == model.ShardStatusSucceeded {
			hasOutput, err := task_service.IsShardResultStored(ctx, shard.TaskID, shard.ShardIndex)
			if err == nil && hasOutput {
				return agent, nil
			}
		}
	case agent_service.AgentStatusRunning:
		if shard.Status == model.ShardStatusLeased || shard.Status == model.ShardStatusRunning {
			return agent, nil
		}
	}

	return server.resetRegisteredAgentToIdle(agent.ID)
}

func (server *Server) resetRegisteredAgentToIdle(agentID string) (agent_service.Agent, error) {
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "reset stale agent state: %v", err)
	}

	agent, err := agent_service.GetAgent(agentID)
	if err != nil {
		return agent_service.Agent{}, status.Errorf(codes.Internal, "get reset agent: %v", err)
	}

	return agent, nil
}
