package control

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/yichozy/r-orchestrator/internal/model"
	"github.com/yichozy/r-orchestrator/internal/orm"
	"github.com/yichozy/r-orchestrator/internal/orm/artifact_orm"
	"github.com/yichozy/r-orchestrator/internal/service/agent_service"
	controlv1 "github.com/yichozy/r-orchestrator/proto"
	"google.golang.org/grpc/metadata"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestServerFetchesResultAfterShardResultReady(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning, "agent-1")

	server := NewServer(db, "token")
	stream := newFakeControlStream(
		registerMsg("agent-1", tenantID),
		shardResultReadyMsg(shardID, []byte("id,value\n1,a\n")),
		shardResultDataMsg(shardID, []byte("id,value\n1,a\n")),
	)

	if err := server.OpenControlStream(stream); err != nil {
		t.Fatalf("OpenControlStream() error = %v", err)
	}

	assertServerSent(t, stream.sentMessages, "FetchShardResult", shardID.String())
	assertServerSent(t, stream.sentMessages, "ShardResultStored", shardID.String())
	assertShardStatus(t, ctx, db, shardID, model.ShardStatusSucceeded)
	assertShardOutputStored(t, ctx, db, taskID, 0, "id,value\n1,a\n")
}

func TestReconnectResultReadyTriggersFetchInsteadOfNewAssignment(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, shardID, taskID, 0, model.ShardStatusResultReady, "agent-1")

	if err := agent_service.RegisterAgent(agent_service.RegisterAgentParams{
		AgentID:     "agent-1",
		TenantID:    tenantID,
		BackendName: "ray",
	}); err != nil {
		t.Fatalf("RegisterAgent() error = %v", err)
	}
	currentShardID := shardID.String()
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        "agent-1",
		Status:         agent_service.AgentStatusResultReady,
		CurrentShardID: &currentShardID,
	}); err != nil {
		t.Fatalf("HeartbeatAgent() error = %v", err)
	}
	agent_service.DisconnectAgent("agent-1")

	server := NewServer(db, "token")
	stream := newFakeControlStream(registerMsg("agent-1", tenantID))

	if err := server.OpenControlStream(stream); err != nil {
		t.Fatalf("OpenControlStream() error = %v", err)
	}

	assertServerSent(t, stream.sentMessages, "FetchShardResult", shardID.String())
	assertServerDidNotSendAssignShard(t, stream.sentMessages)
}

func TestRegisterRestoresResultReadyFromDBBeforeAssigningNewShard(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	resultReadyShardID := uuid.Must(uuid.NewV7())
	queuedShardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, resultReadyShardID, taskID, 0, model.ShardStatusResultReady, "agent-1")
	mustCreateTaskShardWithAgent(t, ctx, db, queuedShardID, taskID, 1, model.ShardStatusQueued, "")

	server := NewServer(db, "token")
	stream := newFakeControlStream(registerMsg("agent-1", tenantID))

	if err := server.OpenControlStream(stream); err != nil {
		t.Fatalf("OpenControlStream() error = %v", err)
	}

	assertServerSentCount(t, stream.sentMessages, "FetchShardResult", resultReadyShardID.String(), 1)
	assertServerDidNotSendAssignShard(t, stream.sentMessages)
}

func TestReconnectAfterResultReadyWithoutStoredDataFetchesBeforeAssigningNewShard(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	resultReadyShardID := uuid.Must(uuid.NewV7())
	queuedShardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, resultReadyShardID, taskID, 0, model.ShardStatusRunning, "agent-1")
	mustCreateTaskShardWithAgent(t, ctx, db, queuedShardID, taskID, 1, model.ShardStatusQueued, "")

	if err := db.WithContext(ctx).Model(&model.Task{}).Where("id = ?", taskID).Update("shard_count", 2).Error; err != nil {
		t.Fatalf("update task shard_count: %v", err)
	}

	if err := agent_service.RegisterAgent(agent_service.RegisterAgentParams{
		AgentID:     "agent-1",
		TenantID:    tenantID,
		BackendName: "ray",
	}); err != nil {
		t.Fatalf("RegisterAgent() error = %v", err)
	}
	currentShardID := resultReadyShardID.String()
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        "agent-1",
		Status:         agent_service.AgentStatusRunning,
		CurrentShardID: &currentShardID,
	}); err != nil {
		t.Fatalf("HeartbeatAgent() error = %v", err)
	}
	agent_service.DisconnectAgent("agent-1")

	server := NewServer(db, "token")
	outputCSV := []byte("id,value\n1,a\n")

	firstStream := newFakeControlStream(
		registerMsg("agent-1", tenantID),
		shardResultReadyMsg(resultReadyShardID, outputCSV),
	)
	if err := server.OpenControlStream(firstStream); err != nil {
		t.Fatalf("OpenControlStream() first stream error = %v", err)
	}

	assertServerSentCount(t, firstStream.sentMessages, "FetchShardResult", resultReadyShardID.String(), 1)
	assertServerDidNotSendAssignShard(t, firstStream.sentMessages)
	assertShardStatus(t, ctx, db, resultReadyShardID, model.ShardStatusResultReady)

	secondStream := newFakeControlStream(registerMsg("agent-1", tenantID))
	if err := server.OpenControlStream(secondStream); err != nil {
		t.Fatalf("OpenControlStream() second stream error = %v", err)
	}

	assertServerSentCount(t, secondStream.sentMessages, "FetchShardResult", resultReadyShardID.String(), 1)
	assertServerDidNotSendAssignShard(t, secondStream.sentMessages)
}

func TestServerAcksDuplicateShardResultDataAfterStoredAckLoss(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning, "agent-1")

	server := NewServer(db, "token")
	outputCSV := []byte("id,value\n1,a\n")
	stream := newFakeControlStream(
		registerMsg("agent-1", tenantID),
		shardResultReadyMsg(shardID, outputCSV),
		shardResultDataMsg(shardID, outputCSV),
		heartbeatMsg("agent-1", agent_service.AgentStatusResultReady, shardID.String()),
		shardResultDataMsg(shardID, outputCSV),
	)

	if err := server.OpenControlStream(stream); err != nil {
		t.Fatalf("OpenControlStream() error = %v", err)
	}

	assertServerSentCount(t, stream.sentMessages, "FetchShardResult", shardID.String(), 1)
	assertServerSentCount(t, stream.sentMessages, "ShardResultStored", shardID.String(), 3)
	assertShardStatus(t, ctx, db, shardID, model.ShardStatusSucceeded)
	assertShardOutputStored(t, ctx, db, taskID, 0, string(outputCSV))

	artifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeShardOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() error = %v", err)
	}
	if len(artifacts) != 1 {
		t.Fatalf("shard output artifact count = %d, want 1", len(artifacts))
	}
}

func TestAckLossAcrossReconnectRequiresStoredAckBeforeNewAssignment(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	recoveredShardID := uuid.Must(uuid.NewV7())
	queuedShardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, recoveredShardID, taskID, 0, model.ShardStatusRunning, "agent-1")
	mustCreateTaskShardWithAgent(t, ctx, db, queuedShardID, taskID, 1, model.ShardStatusQueued, "")

	if err := db.WithContext(ctx).Model(&model.Task{}).Where("id = ?", taskID).Update("shard_count", 2).Error; err != nil {
		t.Fatalf("update task shard_count: %v", err)
	}

	if err := agent_service.RegisterAgent(agent_service.RegisterAgentParams{
		AgentID:     "agent-1",
		TenantID:    tenantID,
		BackendName: "ray",
	}); err != nil {
		t.Fatalf("RegisterAgent() error = %v", err)
	}
	currentShardID := recoveredShardID.String()
	if err := agent_service.HeartbeatAgent(agent_service.HeartbeatAgentParams{
		AgentID:        "agent-1",
		Status:         agent_service.AgentStatusRunning,
		CurrentShardID: &currentShardID,
	}); err != nil {
		t.Fatalf("HeartbeatAgent() error = %v", err)
	}
	agent_service.DisconnectAgent("agent-1")

	server := NewServer(db, "token")
	outputCSV := []byte("id,value\n1,a\n")

	firstStream := newFakeControlStream(
		registerMsg("agent-1", tenantID),
		shardResultReadyMsg(recoveredShardID, outputCSV),
		shardResultDataMsg(recoveredShardID, outputCSV),
	)
	firstStream.failMessageType = "ShardResultStored"
	firstStream.failOnSendCount = 1

	err := server.OpenControlStream(firstStream)
	if err == nil || err.Error() != "send failure for ShardResultStored" {
		t.Fatalf("OpenControlStream() first stream error = %v, want send failure for ShardResultStored", err)
	}

	assertShardStatus(t, ctx, db, recoveredShardID, model.ShardStatusSucceeded)
	assertShardOutputStored(t, ctx, db, taskID, 0, string(outputCSV))
	assertServerDidNotSendAssignShard(t, firstStream.sentMessages)

	secondStream := newFakeControlStream(
		registerMsg("agent-1", tenantID),
		shardResultDataMsg(recoveredShardID, outputCSV),
	)
	if err := server.OpenControlStream(secondStream); err != nil {
		t.Fatalf("OpenControlStream() second stream error = %v", err)
	}

	assertServerSentCount(t, secondStream.sentMessages, "FetchShardResult", recoveredShardID.String(), 1)
	assertServerSentCount(t, secondStream.sentMessages, "ShardResultStored", recoveredShardID.String(), 1)
	assertServerSentAssignShard(t, secondStream.sentMessages, queuedShardID.String())
}

func TestServerRejectsDeprecatedShardCompletedPath(t *testing.T) {
	ctx := context.Background()
	db := newResultPullTestDB(t)
	tenantID := uuid.Must(uuid.NewV7())
	taskID := uuid.Must(uuid.NewV7())
	shardID := uuid.Must(uuid.NewV7())

	mustCreateTask(t, ctx, db, taskID, tenantID, model.TaskStatusRunning)
	mustCreateTaskShardWithAgent(t, ctx, db, shardID, taskID, 0, model.ShardStatusRunning, "agent-1")

	server := NewServer(db, "token")
	stream := newFakeControlStream(
		registerMsg("agent-1", tenantID),
		shardCompletedMsg(shardID, []byte("id,value\n1,a\n")),
	)

	err := server.OpenControlStream(stream)
	if err == nil || err.Error() != "rpc error: code = InvalidArgument desc = ShardCompleted is deprecated; use ShardResultReady and ShardResultData" {
		t.Fatalf("OpenControlStream() error = %v", err)
	}
}

type fakeControlStream struct {
	ctx          context.Context
	recvMessages []*controlv1.AgentMessage
	recvIndex    int
	sentMessages []*controlv1.ServerMessage
	sendCounts   map[string]int

	failMessageType string
	failOnSendCount int
}

func newFakeControlStream(messages ...*controlv1.AgentMessage) *fakeControlStream {
	return &fakeControlStream{
		ctx:          context.Background(),
		recvMessages: messages,
		sentMessages: make([]*controlv1.ServerMessage, 0, 4),
		sendCounts:   make(map[string]int),
	}
}

func (stream *fakeControlStream) Send(message *controlv1.ServerMessage) error {
	messageType := serverMessageType(message)
	stream.sendCounts[messageType]++
	if stream.failMessageType == messageType && stream.sendCounts[messageType] == stream.failOnSendCount {
		return fmt.Errorf("send failure for %s", messageType)
	}
	stream.sentMessages = append(stream.sentMessages, message)
	return nil
}

func (stream *fakeControlStream) Recv() (*controlv1.AgentMessage, error) {
	if stream.recvIndex >= len(stream.recvMessages) {
		return nil, io.EOF
	}
	message := stream.recvMessages[stream.recvIndex]
	stream.recvIndex++
	return message, nil
}

func (stream *fakeControlStream) SetHeader(metadata.MD) error  { return nil }
func (stream *fakeControlStream) SendHeader(metadata.MD) error { return nil }
func (stream *fakeControlStream) SetTrailer(metadata.MD)       {}
func (stream *fakeControlStream) Context() context.Context     { return stream.ctx }
func (stream *fakeControlStream) SendMsg(any) error            { return nil }
func (stream *fakeControlStream) RecvMsg(any) error            { return nil }

func registerMsg(agentID string, tenantID uuid.UUID) *controlv1.AgentMessage {
	return &controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_Register{
			Register: &controlv1.Register{
				AgentId:     agentID,
				TenantId:    tenantID.String(),
				BackendName: "ray",
				Token:       "token",
			},
		},
	}
}

func shardResultReadyMsg(shardID uuid.UUID, outputCSV []byte) *controlv1.AgentMessage {
	return &controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardResultReady{
			ShardResultReady: &controlv1.ShardResultReady{
				ShardId:    shardID.String(),
				OutputSize: int64(len(outputCSV)),
				Sha256:     fmt.Sprintf("%x", outputCSV),
			},
		},
	}
}

func shardResultDataMsg(shardID uuid.UUID, outputCSV []byte) *controlv1.AgentMessage {
	return &controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardResultData{
			ShardResultData: &controlv1.ShardResultData{
				ShardId:   shardID.String(),
				OutputCsv: outputCSV,
			},
		},
	}
}

func heartbeatMsg(agentID, status, currentShardID string) *controlv1.AgentMessage {
	return &controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_Heartbeat{
			Heartbeat: &controlv1.Heartbeat{
				AgentId:        agentID,
				Status:         status,
				CurrentShardId: currentShardID,
			},
		},
	}
}

func shardCompletedMsg(shardID uuid.UUID, outputCSV []byte) *controlv1.AgentMessage {
	return &controlv1.AgentMessage{
		Payload: &controlv1.AgentMessage_ShardCompleted{
			ShardCompleted: &controlv1.ShardCompleted{
				ShardId:   shardID.String(),
				OutputCsv: outputCSV,
			},
		},
	}
}

func newResultPullTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared&_busy_timeout=5000", uuid.NewString())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	if err := orm.AutoMigrate(db); err != nil {
		t.Fatalf("AutoMigrate() error = %v", err)
	}
	orm.SetTestDB(db)
	agent_service.Init()

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db.DB() error = %v", err)
	}
	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	return db
}

func mustCreateTask(t *testing.T, ctx context.Context, db *gorm.DB, taskID, tenantID uuid.UUID, status string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&model.Tenant{
		BaseUUIDModel:      model.BaseUUIDModel{ID: tenantID},
		PrimaryBackendName: "ray",
		MaxAgents:          1,
	}).Error; err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := db.WithContext(ctx).Create(&model.Task{
		BaseUUIDModel: model.BaseUUIDModel{ID: taskID},
		TenantID:      tenantID,
		Status:        status,
		ShardCount:    1,
	}).Error; err != nil {
		t.Fatalf("create task: %v", err)
	}
}

func mustCreateTaskShardWithAgent(t *testing.T, ctx context.Context, db *gorm.DB, shardID, taskID uuid.UUID, shardIndex int, status, agentID string) {
	t.Helper()

	if err := db.WithContext(ctx).Create(&model.TaskShard{
		BaseUUIDModel:   model.BaseUUIDModel{ID: shardID},
		TaskID:          taskID,
		ShardIndex:      shardIndex,
		Status:          status,
		AssignedAgentID: agentID,
	}).Error; err != nil {
		t.Fatalf("create task shard: %v", err)
	}
}

func assertServerSent(t *testing.T, sent []*controlv1.ServerMessage, messageType, shardID string) {
	t.Helper()

	for _, message := range sent {
		switch messageType {
		case "FetchShardResult":
			if fetch := message.GetFetchShardResult(); fetch != nil && fetch.GetShardId() == shardID {
				return
			}
		case "ShardResultStored":
			if stored := message.GetShardResultStored(); stored != nil && stored.GetShardId() == shardID {
				return
			}
		}
	}

	t.Fatalf("server did not send %s for shard %s", messageType, shardID)
}

func assertServerSentCount(t *testing.T, sent []*controlv1.ServerMessage, messageType, shardID string, want int) {
	t.Helper()

	got := 0
	for _, message := range sent {
		switch messageType {
		case "FetchShardResult":
			if fetch := message.GetFetchShardResult(); fetch != nil && fetch.GetShardId() == shardID {
				got++
			}
		case "ShardResultStored":
			if stored := message.GetShardResultStored(); stored != nil && stored.GetShardId() == shardID {
				got++
			}
		}
	}

	if got != want {
		t.Fatalf("server sent %s for shard %s %d times, want %d", messageType, shardID, got, want)
	}
}

func assertServerDidNotSendAssignShard(t *testing.T, sent []*controlv1.ServerMessage) {
	t.Helper()

	for _, message := range sent {
		if message.GetAssignShard() != nil {
			t.Fatalf("server unexpectedly sent AssignShard: %#v", message.GetAssignShard())
		}
	}
}

func assertServerSentAssignShard(t *testing.T, sent []*controlv1.ServerMessage, shardID string) {
	t.Helper()

	for _, message := range sent {
		if assign := message.GetAssignShard(); assign != nil && assign.GetShardId() == shardID {
			return
		}
	}

	t.Fatalf("server did not send AssignShard for shard %s", shardID)
}

func serverMessageType(message *controlv1.ServerMessage) string {
	switch {
	case message.GetAssignShard() != nil:
		return "AssignShard"
	case message.GetFetchShardResult() != nil:
		return "FetchShardResult"
	case message.GetShardResultStored() != nil:
		return "ShardResultStored"
	case message.GetCancelShard() != nil:
		return "CancelShard"
	case message.GetDrain() != nil:
		return "Drain"
	case message.GetShutdown() != nil:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

func assertShardStatus(t *testing.T, ctx context.Context, db *gorm.DB, shardID uuid.UUID, want string) {
	t.Helper()

	var shard model.TaskShard
	if err := db.WithContext(ctx).Where("id = ?", shardID).First(&shard).Error; err != nil {
		t.Fatalf("load shard: %v", err)
	}
	if shard.Status != want {
		t.Fatalf("shard status = %s, want %s", shard.Status, want)
	}
}

func assertShardOutputStored(t *testing.T, ctx context.Context, db *gorm.DB, taskID uuid.UUID, shardIndex int, want string) {
	t.Helper()

	artifacts, err := artifact_orm.ListByTaskAndType(ctx, db, taskID, model.ArtifactTypeShardOutput)
	if err != nil {
		t.Fatalf("ListByTaskAndType() error = %v", err)
	}
	for _, artifact := range artifacts {
		if artifact.ShardIndex != nil && *artifact.ShardIndex == shardIndex {
			if string(artifact.ContentBytes) != want {
				t.Fatalf("artifact content = %q, want %q", string(artifact.ContentBytes), want)
			}
			return
		}
	}

	t.Fatalf("missing shard output artifact for shard_index %d", shardIndex)
}
