package control

import (
	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (server *Server) HandleShardResultData(sess *agentSession, shard_data *controlv1.ShardResultData) error {
	shardID, err := uuid.Parse(shard_data.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}
	duplicateAck, err := task_service.ValidateShardResultDataReport(sess.Context(), shardID, sess.agentID, sess.tenantID, sess.backend)
	if err != nil {
		return err
	}
	if duplicateAck {
		return server.completeCurrentWorkAndReassign(sess, shardID.String())
	}
	if err := task_service.ReportShardStatus(sess.Context(), task_service.ReportShardStatusParams{
		ShardID:     shardID,
		ShardStatus: model.ShardStatusSucceeded,
		OutputCSV:   shard_data.GetOutputCsv(),
	}); err != nil {
		return status.Errorf(codes.Internal, "store shard result data: %v", err)
	}
	server.logger.Info("shard result stored",
		zap.String("agent_id", sess.agentID),
		zap.Stringer("shard_id", shardID),
	)
	return server.completeCurrentWorkAndReassign(sess, shardID.String())
}
