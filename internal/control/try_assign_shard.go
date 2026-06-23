package control

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
	aliyun "github.com/yichozy/hopebox/aliyun"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	ossSDK "github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

func (server *Server) TryAssignShard(sess *agentSession) (ret_err error) {
	if server.db == nil || sess.agentID == "" || sess.tenantID == uuid.Nil || sess.backend == "" {
		return nil
	}

	task, shard, err := task_service.LeaseNextShard(sess.Context(), sess.tenantID, sess.backend, sess.agentID)
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			server.logger.Debug("no queued shard available for assignment",
				zap.String("agent_id", sess.agentID),
				zap.Stringer("tenant_id", sess.tenantID),
				zap.String("backend_name", sess.backend),
			)
			return nil
		}
		return status.Errorf(codes.Internal, "lease next shard: %v", err)
	}
	should_rollback := true
	defer func() {
		if !should_rollback {
			return
		}
		if rollback_err := server.rollbackAssignedShard(sess.Context(), task, shard, sess.agentID); rollback_err != nil {
			if ret_err == nil {
				ret_err = status.Errorf(codes.Internal, "rollback assigned shard: %v", rollback_err)
				return
			}
			ret_err = status.Errorf(codes.Internal, "%v (rollback failed: %v)", ret_err, rollback_err)
		}
	}()

	shardIDStr := shard.ID.String()
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        sess.agentID,
		Status:         agent_service.AgentStatusRunning,
		CurrentShardID: &shardIDStr,
	}); err != nil {
		return status.Errorf(codes.Internal, "mark agent busy: %v", err)
	}

	// Generate pre-signed URLs for bundle download and output upload.
	ossClient, err := aliyun.NewOss()
	if err != nil {
		return status.Errorf(codes.Internal, "init oss client: %v", err)
	}

	bundleKey := fmt.Sprintf("r-orchestrator/tasks/%s/bundle.zip", task.ID)
	bundleURL, err := ossClient.GetObjectURL(sess.Context(), bundleKey)
	if err != nil {
		return status.Errorf(codes.Internal, "sign bundle url: %v", err)
	}

	outputKey := fmt.Sprintf("r-orchestrator/tasks/%s/output/%s-output.zip", task.ID, shard.ScriptName)
	outputURL, err := ossClient.Bucket.SignURL(outputKey, ossSDK.HTTPPut, 3600)
	if err != nil {
		return status.Errorf(codes.Internal, "sign output url: %v", err)
	}

	if err := sess.Send(&controlv1.ServerMessage{
		Payload: &controlv1.ServerMessage_AssignShard{
			AssignShard: &controlv1.AssignShard{
				ShardId:           shard.ID.String(),
				TaskId:            task.ID.String(),
				ScriptName:        shard.ScriptName,
				BundleDownloadUrl: bundleURL,
				OutputUploadUrl:    outputURL,
				OutputOssKey:      outputKey,
				TotalShards:       int32(task.ShardCount),
			},
		},
	}); err != nil {
		return status.Errorf(codes.Internal, "send assign shard: %v", err)
	}

	server.logger.Info("shard assigned",
		zap.String("agent_id", sess.agentID),
		zap.Stringer("task_id", task.ID),
		zap.Stringer("shard_id", shard.ID),
	)

	should_rollback = false
	return nil
}
