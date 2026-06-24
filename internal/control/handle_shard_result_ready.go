package control

import (
	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Server) HandleShardResultReady(sess *agentSession, shard_ready *controlv1.ShardResultReady) error {
	shardID, err := uuid.Parse(shard_ready.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}

	shard, err := task_service.LoadValidatedShard(sess.Context(), shardID, sess.agentID, sess.tenantID, sess.backend)
	if err != nil {
		return err
	}

	ack := &controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_ShardResultStored{
			ShardResultStored: &controlv1.ShardResultStored{ShardId: shardID.String()},
		},
	}

	// Normal path: shard is RUNNING, mark it SUCCEEDED.
	if shard.Status == model.ShardStatusRunning {
		if err := task_service.ReportShardStatus(sess.Context(), task_service.ReportShardStatusParams{
			ShardID:      shardID,
			ShardStatus:  model.ShardStatusSucceeded,
			OutputOSSKey: shard_ready.GetOutputOssKey(),
			OutputSHA256: shard_ready.GetSha256(),
		}); err != nil {
			return status.Errorf(codes.Internal, "mark shard succeeded: %v", err)
		}
		server.logger.Info("shard succeeded",
			zap.String("agent_id", sess.agentID),
			zap.Stringer("shard_id", shardID),
			zap.String("output_oss_key", shard_ready.GetOutputOssKey()),
		)
		if err := sess.Send(ack); err != nil {
			return err
		}
		return server.resetAgentAndAssign(sess)
	}

	// Shard is not RUNNING — already SUCCEEDED, CANCELLED, or rolled back to
	// QUEUED/LEASED during reconnect. Send the ack so the agent can clear its
	// pending state, but only reset + assign if the agent is still waiting on
	// this shard. If the agent already moved on (e.g. reconnected and got a
	// new assignment), just send the ack without disrupting current work.
	server.logger.Info("shard result ready for non-running shard, sending ack",
		zap.String("agent_id", sess.agentID),
		zap.Stringer("shard_id", shardID),
		zap.String("shard_status", shard.Status),
	)

	agent, agentErr := agent_service.GetAgent(sess.agentID)
	if agentErr != nil || agent.CurrentShardID == nil || *agent.CurrentShardID != shardID.String() {
		return sess.Send(ack)
	}
	if err := sess.Send(ack); err != nil {
		return err
	}
	return server.resetAgentAndAssign(sess)
}

// resetAgentAndAssign resets the agent to IDLE and tries to assign the next shard.
func (server *Server) resetAgentAndAssign(sess *agentSession) error {
	if err := agent_service.HeartbeatAgent(sess.agentID, agent_service.AgentStatusIdle, nil); err != nil {
		return status.Errorf(codes.Internal, "reset agent idle: %v", err)
	}
	return server.TryAssignShard(sess)
}
