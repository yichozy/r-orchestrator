package control

import (
	"context"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func (server *Server) HandleShardResultData(
	streamRef *agentStream,
	stream grpc.BidiStreamingServer[controlv1.AgentMessage, controlv1.ServerMessage],
	shard_data *controlv1.ShardResultData,
	current_agent_id string,
	current_tenant_id uuid.UUID,
	current_backend_name string,
) error {
	shardID, err := uuid.Parse(shard_data.GetShardId())
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid shard_id: %v", err)
	}
	duplicateAck, err := server.validateShardResultDataReport(stream.Context(), shardID, current_agent_id, current_tenant_id, current_backend_name)
	if err != nil {
		return err
	}
	if duplicateAck {
		return server.completeCurrentWorkAndReassign(streamRef, current_agent_id, current_tenant_id, current_backend_name, shardID.String())
	}
	var storeShardOutputFn task_service.StoreShardOutputFunc
	if server.storeOutputFn != nil {
		storeShardOutputFn = func(ctx context.Context, tx *gorm.DB, task model.Task, shard model.TaskShard, outputCSV []byte) error {
			return server.storeOutputFn(ctx, tx, task.TenantID, shard.ID, outputCSV)
		}
	}
	if err := task_service.ReportShardStatus(stream.Context(), task_service.ReportShardStatusParams{
		ShardID:            shardID,
		ShardStatus:        model.ShardStatusSucceeded,
		OutputCSV:          shard_data.GetOutputCsv(),
		StoreShardOutputFn: storeShardOutputFn,
	}); err != nil {
		return status.Errorf(codes.Internal, "store shard result data: %v", err)
	}
	server.logger.Info("shard result stored",
		zap.String("agent_id", current_agent_id),
		zap.Stringer("shard_id", shardID),
	)
	return server.completeCurrentWorkAndReassign(streamRef, current_agent_id, current_tenant_id, current_backend_name, shardID.String())
}
