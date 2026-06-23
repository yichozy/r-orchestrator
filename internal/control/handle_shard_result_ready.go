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

	// Idempotent: shard already SUCCEEDED means the original ShardResultReady
	// was processed but the ack was lost (e.g. agent reconnected). Re-send the
	// ack and reset to IDLE instead of rejecting and terminating the stream.
	if shard.Status == model.ShardStatusSucceeded {
		server.logger.Info("shard already succeeded, re-sending ack",
			zap.String("agent_id", sess.agentID),
			zap.Stringer("shard_id", shardID),
		)
		return server.ackShardResultAndReset(sess, shardID)
	}

	if shard.Status != model.ShardStatusRunning {
		return status.Errorf(codes.FailedPrecondition, "mark shard result ready requires shard %s to be %s, got %s", shardID, model.ShardStatusRunning, shard.Status)
	}

	// Agent uploaded output to OSS. Record OSS key and mark shard SUCCEEDED.
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

	return server.ackShardResultAndReset(sess, shardID)
}

// ackShardResultAndReset sends the ShardResultStored ack, resets the agent to
// IDLE, and tries to assign the next shard.
func (server *Server) ackShardResultAndReset(sess *agentSession, shardID uuid.UUID) error {
	if err := sess.Send(&controlv1.ServerMessage{Payload: &controlv1.ServerMessage_ShardResultStored{ShardResultStored: &controlv1.ShardResultStored{ShardId: shardID.String()}}}); err != nil {
		return err
	}
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        sess.agentID,
		Status:         agent_service.AgentStatusIdle,
		CurrentShardID: nil,
	}); err != nil {
		return status.Errorf(codes.Internal, "reset agent idle: %v", err)
	}

	return server.TryAssignShard(sess)
}
