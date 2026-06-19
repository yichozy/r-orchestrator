package controlv1

import "testing"

func TestRegisterUsesBackendName(t *testing.T) {
	register := &Register{
		AgentId:     "agent-1",
		TenantId:    "tenant-1",
		BackendName: "fake",
		Token:       "token",
		Version:     "v1",
	}

	if register.GetBackendName() != "fake" {
		t.Fatalf("expected backend name fake, got %q", register.GetBackendName())
	}
}

func TestAgentMessageKeepsShardCompletedCompatibility(t *testing.T) {
	message := &AgentMessage{
		Payload: &AgentMessage_ShardCompleted{
			ShardCompleted: &ShardCompleted{
				ShardId:   "shard-1",
				OutputCsv: []byte("id,value\n1,a\n"),
			},
		},
	}

	if got := message.GetShardCompleted(); got == nil || got.GetShardId() != "shard-1" {
		t.Fatalf("expected shard_completed payload to round-trip, got %#v", got)
	}
}

func TestAgentMessageSupportsResultReadyPayload(t *testing.T) {
	message := &AgentMessage{
		Payload: &AgentMessage_ShardResultReady{
			ShardResultReady: &ShardResultReady{
				ShardId:    "shard-2",
				OutputSize: 12,
				Sha256:     "abc",
			},
		},
	}

	got := message.GetShardResultReady()
	if got == nil {
		t.Fatal("expected shard_result_ready payload")
	}
	if got.GetShardId() != "shard-2" || got.GetOutputSize() != 12 || got.GetSha256() != "abc" {
		t.Fatalf("unexpected shard_result_ready payload: %#v", got)
	}
}
