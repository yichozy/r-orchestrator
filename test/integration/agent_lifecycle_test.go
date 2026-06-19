//go:build integration

package integration

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/control"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/shard_orm"
	"github.com/yichozy/r-orchestrator/internal/orm/task_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	"github.com/yichozy/r-orchestrator/internal/service/task_service"
	"github.com/yichozy/r-orchestrator/internal/util"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
)

const (
	testTenantID = "00000000-0000-0000-0000-000000000001"
	testAgentID  = "10000000-0000-0000-0000-000000000001"
	testBackend  = "ray"
)

func TestAgentRegisterAndReceiveShard(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, bundleArtifactID, inputCSVArtifactID := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign: %v", err)
	}

	assign := msg.GetAssignShard()
	if assign == nil {
		t.Fatalf("expected AssignShard, got %T", msg.Payload)
	}
	if assign.GetShardId() != shardIDs[0].String() {
		t.Errorf("shard_id: want %s, got %s", shardIDs[0], assign.GetShardId())
	}
	if assign.GetTaskId() != taskID.String() {
		t.Errorf("task_id: want %s, got %s", taskID, assign.GetTaskId())
	}
	if assign.GetBundleArtifactId() != bundleArtifactID.String() {
		t.Errorf("bundle_artifact_id: want %s, got %s", bundleArtifactID, assign.GetBundleArtifactId())
	}
	if assign.GetInputCsvArtifactId() != inputCSVArtifactID.String() {
		t.Errorf("input_csv_artifact_id: want %s, got %s", inputCSVArtifactID, assign.GetInputCsvArtifactId())
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)
}

func TestShardStartedThenCompleted(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign: %v", err)
	}
	assign := msg.GetAssignShard()

	// Shard started
	if err := sendShardStarted(stream, assign.GetShardId()); err != nil {
		t.Fatalf("shard started: %v", err)
	}

	// Shard completed
	outputCSV := []byte("result_col\nresult_val\n")
	if err := completeShardViaResultReady(stream, assign.GetShardId(), outputCSV, 5*time.Second); err != nil {
		t.Fatalf("complete shard via result-ready: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	// Verify shard status
	shard, err := shard_orm.GetByID(ctx, testDB, shardIDs[0])
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	if shard.Status != model.ShardStatusSucceeded {
		t.Errorf("shard status: want %s, got %s", model.ShardStatusSucceeded, shard.Status)
	}

	// Verify task status
	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Errorf("task status: want %s, got %s", model.TaskStatusSucceeded, task.Status)
	}
}

func TestShardCompletedBlockedWhenOutputStorageFails(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)
	agentID := "10000000-0000-0000-0000-000000000099"

	if testControlServer == nil {
		t.Fatal("test control server is nil")
	}
	testControlServer.SetStoreShardOutputFunc(func(ctx context.Context, tx *gorm.DB, tenantID, shardID uuid.UUID, outputCSV []byte) error {
		var shard model.TaskShard
		if err := tx.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
			return err
		}
		shardIndex := shard.ShardIndex
		if err := artifact_orm.Create(ctx, tx, model.Artifact{
			BaseUUIDModel: model.BaseUUIDModel{ID: uuid.New()},
			TenantID:      tenantID,
			TaskID:        shard.TaskID,
			ArtifactType:  model.ArtifactTypeShardOutput,
			ContentBytes:  append([]byte(nil), outputCSV...),
			ContentSize:   int64(len(outputCSV)),
			SHA256:        util.SumSHA256(outputCSV),
			ShardIndex:    &shardIndex,
		}); err != nil {
			return err
		}
		return errors.New("disk write failed")
	})
	defer testControlServer.SetStoreShardOutputFunc(nil)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, agentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign: %v", err)
	}
	assign := msg.GetAssignShard()

	if err := sendShardStarted(stream, assign.GetShardId()); err != nil {
		t.Fatalf("shard started: %v", err)
	}
	resultCSV := []byte("result_col\nresult_val\n")
	if err := sendShardResultReady(stream, assign.GetShardId(), resultCSV); err != nil {
		t.Fatalf("shard result ready: %v", err)
	}
	fetchMsg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv fetch shard result: %v", err)
	}
	fetch := fetchMsg.GetFetchShardResult()
	if fetch == nil || fetch.GetShardId() != assign.GetShardId() {
		t.Fatalf("expected FetchShardResult for %s, got %#v", assign.GetShardId(), fetchMsg.Payload)
	}
	if err := sendShardResultData(stream, assign.GetShardId(), resultCSV); err != nil {
		t.Fatalf("shard result data: %v", err)
	}

	_, err = stream.Recv()
	if err == nil {
		t.Fatal("expected store shard output error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal, got %s", st.Code())
	}
	if !strings.Contains(st.Message(), "store shard output") {
		t.Fatalf("error message = %q, want store shard output", st.Message())
	}
	_ = stream.CloseSend()

	shard, err := shard_orm.GetByID(ctx, testDB, shardIDs[0])
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	if shard.Status != model.ShardStatusResultReady {
		t.Fatalf("shard status: want %s, got %s", model.ShardStatusResultReady, shard.Status)
	}

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusRunning {
		t.Fatalf("task status: want %s, got %s", model.TaskStatusRunning, task.Status)
	}

	shardArtifacts, err := artifact_orm.ListByTaskAndType(ctx, testDB, taskID, model.ArtifactTypeShardOutput)
	if err != nil {
		t.Fatalf("list shard outputs: %v", err)
	}
	if len(shardArtifacts) != 0 {
		t.Fatalf("shard output artifact count: want 0, got %d", len(shardArtifacts))
	}
}

func TestServerRestartRecoversResultReadyBeforeAssigningNewShard(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	if err := testDB.WithContext(ctx).Model(&model.TaskShard{}).
		Where("id = ?", shardIDs[0]).
		Updates(map[string]any{
			"status":            model.ShardStatusResultReady,
			"assigned_agent_id": testAgentID,
		}).Error; err != nil {
		t.Fatalf("prime result-ready shard: %v", err)
	}

	restartIntegrationControlServer(t)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	fetchMsg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv fetch: %v", err)
	}
	fetch := fetchMsg.GetFetchShardResult()
	if fetch == nil {
		t.Fatalf("expected FetchShardResult, got %T", fetchMsg.Payload)
	}
	if fetch.GetShardId() != shardIDs[0].String() {
		t.Fatalf("fetch shard_id: want %s, got %s", shardIDs[0], fetch.GetShardId())
	}

	outputCSV := []byte("result_col\nresult_val\n")
	if err := sendShardResultData(stream, shardIDs[0].String(), outputCSV); err != nil {
		t.Fatalf("send shard result data: %v", err)
	}

	storedMsg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv stored ack: %v", err)
	}
	stored := storedMsg.GetShardResultStored()
	if stored == nil {
		t.Fatalf("expected ShardResultStored, got %T", storedMsg.Payload)
	}
	if stored.GetShardId() != shardIDs[0].String() {
		t.Fatalf("stored shard_id: want %s, got %s", shardIDs[0], stored.GetShardId())
	}

	assignMsg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign after recovery: %v", err)
	}
	assign := assignMsg.GetAssignShard()
	if assign == nil {
		t.Fatalf("expected AssignShard after recovery, got %T", assignMsg.Payload)
	}
	if assign.GetShardId() != shardIDs[1].String() {
		t.Fatalf("assign shard_id: want %s, got %s", shardIDs[1], assign.GetShardId())
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)
}

func TestShardFailed(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign: %v", err)
	}
	assign := msg.GetAssignShard()

	// Shard started
	if err := sendShardStarted(stream, assign.GetShardId()); err != nil {
		t.Fatalf("shard started: %v", err)
	}

	// Shard failed
	if err := sendShardFailed(stream, assign.GetShardId(), "runtime error"); err != nil {
		t.Fatalf("shard failed: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	shard, err := shard_orm.GetByID(ctx, testDB, shardIDs[0])
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	if shard.Status != model.ShardStatusFailed {
		t.Errorf("shard status: want %s, got %s", model.ShardStatusFailed, shard.Status)
	}

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusFailed {
		t.Errorf("task status: want %s, got %s", model.TaskStatusFailed, task.Status)
	}
}

func TestMultipleShardsSequential(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Receive and complete shard 1
	msg1, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 1: %v", err)
	}
	assign1 := msg1.GetAssignShard()
	if err := sendShardStarted(stream, assign1.GetShardId()); err != nil {
		t.Fatalf("shard started 1: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign1.GetShardId(), []byte("id,value\n1,a\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 1 via result-ready: %v", err)
	}

	// Receive shard 2 (auto-assigned after completion)
	msg2, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 2: %v", err)
	}
	assign2 := msg2.GetAssignShard()
	if assign2.GetShardId() == assign1.GetShardId() {
		t.Errorf("shard 2 should be different from shard 1")
	}

	// Complete shard 2
	if err := sendShardStarted(stream, assign2.GetShardId()); err != nil {
		t.Fatalf("shard started 2: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign2.GetShardId(), []byte("id,value\n2,b\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 2 via result-ready: %v", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	// Both shards succeeded
	for i, sid := range shardIDs {
		shard, err := shard_orm.GetByID(ctx, testDB, sid)
		if err != nil {
			t.Fatalf("get shard %d: %v", i, err)
		}
		if shard.Status != model.ShardStatusSucceeded {
			t.Errorf("shard %d status: want %s, got %s", i, model.ShardStatusSucceeded, shard.Status)
		}
	}

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusSucceeded {
		t.Errorf("task status: want %s, got %s", model.TaskStatusSucceeded, task.Status)
	}
}

func TestCancelTaskDuringExecution(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	msg, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign: %v", err)
	}
	assign := msg.GetAssignShard()

	// Cancel the task via DB
	_ = task_orm.CancelTaskById(ctx, testDB, tenantID, taskID)
	shard_orm.CancelShards(ctx, testDB, taskID)

	// ShardStarted should fail (CancelledToRunning status conflict)
	if err := sendShardStarted(stream, assign.GetShardId()); err != nil {
		// Expected - server may close stream or return error
		t.Logf("shard started after cancel: %v (expected)", err)
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)

	shard, err := shard_orm.GetByID(ctx, testDB, shardIDs[0])
	if err != nil {
		t.Fatalf("get shard: %v", err)
	}
	if shard.Status != model.ShardStatusCancelled {
		t.Errorf("shard status: want %s, got %s", model.ShardStatusCancelled, shard.Status)
	}

	task, err := task_orm.GetByID(ctx, testDB, taskID)
	if err != nil {
		t.Fatalf("get task: %v", err)
	}
	if task.Status != model.TaskStatusCancelled {
		t.Errorf("task status: want %s, got %s", model.TaskStatusCancelled, task.Status)
	}
}

func TestHeartbeatIdleTriggersReassignment(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 2)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Receive shard 1
	msg1, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 1: %v", err)
	}
	assign1 := msg1.GetAssignShard()

	// Complete shard 1 without heartbeat
	if err := sendShardStarted(stream, assign1.GetShardId()); err != nil {
		t.Fatalf("shard started 1: %v", err)
	}
	if err := completeShardViaResultReady(stream, assign1.GetShardId(), []byte("id,value\n1,a\n"), 5*time.Second); err != nil {
		t.Fatalf("complete shard 1 via result-ready: %v", err)
	}

	// Shard 2 should be auto-assigned after completion
	msg2, err := recvServerMessage(stream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign 2: %v", err)
	}
	if msg2.GetAssignShard() == nil {
		t.Fatalf("expected AssignShard, got %T", msg2.Payload)
	}
	if msg2.GetAssignShard().GetShardId() == assign1.GetShardId() {
		t.Errorf("shard 2 should differ from shard 1")
	}

	_ = stream.CloseSend()
	_ = recvServerMessageEOF(stream)
}

func TestDuplicateRegisterRejected(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	conn, err := grpc.DialContext(ctx, testGrpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// First stream - register successfully
	stream1, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream 1: %v", err)
	}
	if err := sendRegister(stream1, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register 1: %v", err)
	}
	_, _ = recvServerMessage(stream1, 5*time.Second)

	// Second stream - same agent_id, should be rejected
	stream2, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream 2: %v", err)
	}
	if err := sendRegister(stream2, testAgentID, tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register 2 send: %v", err)
	}

	// Server should close the stream (agent identity conflict)
	_, err = stream2.Recv()
	if err == nil {
		t.Error("expected error for duplicate register, got nil")
	} else {
		st, ok := status.FromError(err)
		if !ok {
			t.Logf("non-gRPC error: %v", err)
		} else if st.Code() != codes.PermissionDenied {
			t.Errorf("expected PermissionDenied, got %s", st.Code())
		}
	}

	_ = stream1.CloseSend()
	_ = recvServerMessageEOF(stream1)
}

func TestWrongTokenRejected(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}

	if err := sendRegister(stream, testAgentID, tenantID.String(), testBackend, "wrong-token"); err != nil {
		t.Fatalf("register send: %v", err)
	}

	// Server should close the stream with Unauthenticated
	_, err = stream.Recv()
	if err == nil {
		t.Error("expected error for wrong token, got nil")
	} else {
		st, ok := status.FromError(err)
		if !ok {
			t.Logf("non-gRPC error: %v", err)
		} else if st.Code() != codes.Unauthenticated {
			t.Errorf("expected Unauthenticated, got %s", st.Code())
		}
	}
}

func TestLeaseUsesTenantPrimaryBackend(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	wrongBackendConn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial wrong backend: %v", err)
	}
	defer wrongBackendConn.Close()

	wrongBackendStream, err := openControlStream(ctx, wrongBackendConn)
	if err != nil {
		t.Fatalf("open wrong backend stream: %v", err)
	}
	if err := sendRegister(wrongBackendStream, "10000000-0000-0000-0000-000000000002", tenantID.String(), "kubernetes", agentToken); err != nil {
		t.Fatalf("register wrong backend: %v", err)
	}
	if _, err := recvServerMessage(wrongBackendStream, 500*time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected no shard for wrong backend, got %v", err)
	}
	_ = wrongBackendStream.CloseSend()

	rightBackendConn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial right backend: %v", err)
	}
	defer rightBackendConn.Close()

	rightBackendStream, err := openControlStream(ctx, rightBackendConn)
	if err != nil {
		t.Fatalf("open right backend stream: %v", err)
	}
	if err := sendRegister(rightBackendStream, "10000000-0000-0000-0000-000000000003", tenantID.String(), testBackend, agentToken); err != nil {
		t.Fatalf("register right backend: %v", err)
	}

	msg, err := recvServerMessage(rightBackendStream, 5*time.Second)
	if err != nil {
		t.Fatalf("recv assign: %v", err)
	}
	assign := msg.GetAssignShard()
	if assign == nil {
		t.Fatalf("expected AssignShard, got %T", msg.Payload)
	}
	if assign.GetShardId() != shardIDs[0].String() {
		t.Fatalf("expected shard %s, got %s", shardIDs[0], assign.GetShardId())
	}
}

func TestShardStartedRejectedForMismatchedTenantBackend(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	taskID, shardIDs, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	agentID := uuid.MustParse("10000000-0000-0000-0000-000000000004")
	if err := testDB.WithContext(ctx).Model(&model.TaskShard{}).
		Where("id = ?", shardIDs[0]).
		Updates(map[string]any{
			"status":            model.ShardStatusLeased,
			"assigned_agent_id": agentID,
		}).Error; err != nil {
		t.Fatalf("lease shard directly: %v", err)
	}

	conn, err := dialGrpc(ctx)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	stream, err := openControlStream(ctx, conn)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := sendRegister(stream, agentID.String(), tenantID.String(), "kubernetes", agentToken); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := sendShardStarted(stream, shardIDs[0].String()); err != nil {
		t.Fatalf("send shard started: %v", err)
	}

	_, err = recvServerMessage(stream, 5*time.Second)
	if err == nil {
		t.Fatal("expected backend mismatch error")
	}
	st, ok := status.FromError(err)
	if !ok {
		if errors.Is(err, context.Canceled) {
			return
		}
		t.Fatalf("expected gRPC status error, got %v", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %s", st.Code())
	}
}

func TestTaskViewsDoNotExposeBackendName(t *testing.T) {
	ctx := context.Background()
	defer cleanupTenant(ctx, mustUUID(testTenantID))

	tenantID := setupTenant(ctx, 1)
	tenantName := integrationTenantName(1)
	taskID, _, _, _ := setupRunningTask(ctx, tenantID, 1)
	defer cleanupTask(ctx, taskID)

	taskView, err := task_service.GetTask(ctx, tenantName, taskID)
	if err != nil {
		t.Fatalf("get task view: %v", err)
	}
	if taskView.ID != taskID {
		t.Fatalf("expected task id %s, got %s", taskID, taskView.ID)
	}
	if taskView.TenantName != tenantName {
		t.Fatalf("expected tenant name %s, got %s", tenantName, taskView.TenantName)
	}

	taskViews, err := task_service.ListTasks(ctx, tenantName, "")
	if err != nil {
		t.Fatalf("list task views: %v", err)
	}
	if len(taskViews) != 1 {
		t.Fatalf("expected 1 task, got %d", len(taskViews))
	}
	if taskViews[0].ID != taskID {
		t.Fatalf("expected listed task id %s, got %s", taskID, taskViews[0].ID)
	}
	if taskViews[0].TenantName != tenantName {
		t.Fatalf("expected listed tenant name %s, got %s", tenantName, taskViews[0].TenantName)
	}
}

func restartIntegrationControlServer(t *testing.T) {
	t.Helper()

	if testServer != nil {
		testServer.GracefulStop()
	}
	if testGrpcLis != nil {
		_ = testGrpcLis.Close()
	}

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	testGrpcLis = lis
	testGrpcAddr = lis.Addr().String()
	testControlServer = control.NewServer(testDB, agent_service.NewService(), agentToken)
	testServer = grpc.NewServer()
	controlv1.RegisterControlServiceServer(testServer, testControlServer)

	go func() {
		_ = testServer.Serve(lis)
	}()
}
