//go:build integration

package integration

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/shard_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/tenant_orm"
	"github.com/yichozy/r-orchestrator/internal/util"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- DB setup helpers ---

func setupTenant(ctx context.Context, maxAgents int) uuid.UUID {
	tenantID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	_, err := tenant_orm.Create(ctx, testDB, model.Tenant{
		BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
		Name:               integrationTenantName(maxAgents),
		PrimaryBackendName: "ray",
		MaxAgents:          maxAgents,
	})
	if err != nil {
		panic(fmt.Sprintf("setup tenant: %v", err))
	}
	return tenantID
}

func integrationTenantName(maxAgents int) string {
	return fmt.Sprintf("integration-team-%d", maxAgents)
}

func cleanupTenant(ctx context.Context, tenantID uuid.UUID) {
	testDB.WithContext(ctx).Unscoped().
		Where("task_id IN (?)", testDB.Model(&model.Task{}).Select("id").Where("tenant_id = ?", tenantID)).
		Delete(&model.TaskShard{})
	testDB.WithContext(ctx).Unscoped().Where("tenant_id = ?", tenantID).Delete(&model.Artifact{})
	testDB.WithContext(ctx).Unscoped().Where("tenant_id = ?", tenantID).Delete(&model.Task{})
	testDB.WithContext(ctx).Unscoped().Where("id = ?", tenantID).Delete(&model.Tenant{})
}

// setupRunningTask creates a RUNNING task with QUEUED shards and their artifacts.
func setupRunningTask(ctx context.Context, tenantID uuid.UUID, shardCount int) (uuid.UUID, []uuid.UUID, uuid.UUID, uuid.UUID) {
	taskID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	bundleArtifactID := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	inputCSVArtifactID := uuid.MustParse("00000000-0000-0000-0000-000000000004")

	if err := task_orm.Create(ctx, testDB, model.Task{
		BaseUUIDModel:      model.BaseUUIDModel{ID: taskID},
		TenantID:           tenantID,
		Status:             model.TaskStatusRunning,
		BundleArtifactID:   bundleArtifactID,
		InputCSVArtifactID: inputCSVArtifactID,
		ShardCount:         shardCount,
	}); err != nil {
		panic(fmt.Sprintf("setup task: %v", err))
	}

	bundleBytes := []byte("fake-bundle")
	inputCSVBytes := []byte("col1,col2\nval1,val2\n")

	artifact_orm.CreateArtifact(ctx, testDB, model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: bundleArtifactID},
		TenantID:      tenantID,
		TaskID:        taskID,
		ArtifactType:  model.ArtifactTypeBundle,
		ContentBytes:  bundleBytes,
		ContentSize:   int64(len(bundleBytes)),
		SHA256:        util.SumSHA256(bundleBytes),
	})
	artifact_orm.CreateArtifact(ctx, testDB, model.Artifact{
		BaseUUIDModel: model.BaseUUIDModel{ID: inputCSVArtifactID},
		TenantID:      tenantID,
		TaskID:        taskID,
		ArtifactType:  model.ArtifactTypeInputCSV,
		ContentBytes:  inputCSVBytes,
		ContentSize:   int64(len(inputCSVBytes)),
		SHA256:        util.SumSHA256(inputCSVBytes),
	})

	shardIDs := make([]uuid.UUID, shardCount)
	for i := 0; i < shardCount; i++ {
		shardID, _ := uuid.NewV7()
		shardIDs[i] = shardID
		if err := shard_orm.Create(ctx, testDB, model.TaskShard{
			BaseUUIDModel: model.BaseUUIDModel{ID: shardID},
			TaskID:        taskID,
			ShardIndex:    i,
			Status:        model.ShardStatusQueued,
		}); err != nil {
			panic(fmt.Sprintf("setup shard %d: %v", i, err))
		}
	}

	return taskID, shardIDs, bundleArtifactID, inputCSVArtifactID
}

// cleanupTask removes task and all associated artifacts/shards.
func cleanupTask(ctx context.Context, taskID uuid.UUID) {
	testDB.WithContext(ctx).Unscoped().Where("task_id = ?", taskID).Delete(&model.TaskShard{})
	testDB.WithContext(ctx).Unscoped().Where("task_id = ?", taskID).Delete(&model.Artifact{})
	testDB.WithContext(ctx).Unscoped().Where("id = ?", taskID).Delete(&model.Task{})
}

// --- gRPC client helpers ---

func dialGrpc(ctx context.Context) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, testGrpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func openControlStream(ctx context.Context, conn *grpc.ClientConn) (controlv1.ControlService_OpenControlStreamClient, error) {
	client := controlv1.NewControlServiceClient(conn)
	return client.OpenControlStream(ctx)
}

// --- Message send helpers ---

func sendRegister(stream controlv1.ControlService_OpenControlStreamClient, agentID, tenantID, backendName, token string) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_Register{
			Register: &controlv1.Register{
				AgentId:     agentID,
				TenantId:    tenantID,
				BackendName: backendName,
				Token:       token,
			},
		},
	})
}

func sendHeartbeat(stream controlv1.ControlService_OpenControlStreamClient, agentID, status, currentShardID string) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_Heartbeat{
			Heartbeat: &controlv1.Heartbeat{
				AgentId:        agentID,
				Status:         status,
				CurrentShardId: currentShardID,
			},
		},
	})
}

func sendShardStarted(stream controlv1.ControlService_OpenControlStreamClient, shardID string) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardStarted{
			ShardStarted: &controlv1.ShardStarted{
				ShardId: shardID,
			},
		},
	})
}

func sendShardCompleted(stream controlv1.ControlService_OpenControlStreamClient, shardID string, outputCSV []byte) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardCompleted{
			ShardCompleted: &controlv1.ShardCompleted{
				ShardId:   shardID,
				OutputCsv: outputCSV,
			},
		},
	})
}

func sendShardResultReady(stream controlv1.ControlService_OpenControlStreamClient, shardID string, outputCSV []byte) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardResultReady{
			ShardResultReady: &controlv1.ShardResultReady{
				ShardId:    shardID,
				OutputSize: int64(len(outputCSV)),
				Sha256:     util.SumSHA256(outputCSV),
			},
		},
	})
}

func sendShardResultData(stream controlv1.ControlService_OpenControlStreamClient, shardID string, outputCSV []byte) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardResultData{
			ShardResultData: &controlv1.ShardResultData{
				ShardId:   shardID,
				OutputCsv: outputCSV,
			},
		},
	})
}

func completeShardViaResultReady(stream controlv1.ControlService_OpenControlStreamClient, shardID string, outputCSV []byte, timeout time.Duration) error {
	if err := sendShardResultReady(stream, shardID, outputCSV); err != nil {
		return err
	}

	fetchMsg, err := recvServerMessage(stream, timeout)
	if err != nil {
		return err
	}
	fetch := fetchMsg.GetFetchShardResult()
	if fetch == nil {
		return fmt.Errorf("expected FetchShardResult, got %T", fetchMsg.Payload)
	}
	if fetch.GetShardId() != shardID {
		return fmt.Errorf("fetch shard_id: want %s, got %s", shardID, fetch.GetShardId())
	}

	if err := sendShardResultData(stream, shardID, outputCSV); err != nil {
		return err
	}

	storedMsg, err := recvServerMessage(stream, timeout)
	if err != nil {
		return err
	}
	stored := storedMsg.GetShardResultStored()
	if stored == nil {
		return fmt.Errorf("expected ShardResultStored, got %T", storedMsg.Payload)
	}
	if stored.GetShardId() != shardID {
		return fmt.Errorf("stored shard_id: want %s, got %s", shardID, stored.GetShardId())
	}

	return nil
}

func sendShardFailed(stream controlv1.ControlService_OpenControlStreamClient, shardID, errorMessage string) error {
	return stream.Send(&controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardFailed{
			ShardFailed: &controlv1.ShardFailed{
				ShardId:      shardID,
				ErrorMessage: errorMessage,
			},
		},
	})
}

// recvServerMessage receives a server message with a timeout.
func recvServerMessage(stream controlv1.ControlService_OpenControlStreamClient, timeout time.Duration) (*controlv1.ServerMessage, error) {
	ctx := stream.Context()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	done := make(chan struct{})
	var msg *controlv1.ServerMessage
	var recvErr error
	go func() {
		msg, recvErr = stream.Recv()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	if recvErr != nil {
		return nil, recvErr
	}
	return msg, nil
}

// recvServerMessageEOF receives until EOF, returns nil on EOF.
func recvServerMessageEOF(stream controlv1.ControlService_OpenControlStreamClient) error {
	_, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	return err
}

// --- UUID generation for consistent test IDs ---

func mustUUID(s string) uuid.UUID {
	return uuid.MustParse(s)
}
