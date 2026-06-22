# Script-Based Task Distribution Design

## Problem

Current task distribution splits a CSV into rows and distributes them as shards. This doesn't match the actual use case: users submit a zip bundle containing R code and multiple executable scripts, each needing to run independently. Need to redesign task submission and execution around scripts instead of CSV rows.

## Current Flow

```
zip + csv → SplitCSVRows(csv) → N shards (csv slices) → agent executes R with csv slice → collect csv results → merge
```

## New Flow

```
zip (install.sh + cmd/*.sh + R code) → scan cmd/*.sh → N shards (one per script) → agent downloads bundle from OSS → runs install.sh (once) → executes script in isolated workdir → uploads output to OSS → user queries status + downloads from OSS
```

## Bundle Structure

```
bundle.zip
├── install.sh          # Dependency installation (R packages, etc.)
├── cmd/
│   ├── run1.sh
│   ├── abc.sh
│   └── esfsad.sh       # Each .sh is an executable script
└── (R code directory)  # R code referenced by scripts
```

Validation on submit: must contain `install.sh` and `cmd/` with at least one `.sh` file.

## Data Model Changes

### TaskShard

Add field:
- `ScriptName string` — the script filename under `cmd/` (e.g. `run1.sh`, `abc.sh`)

Remove: nothing. The shard model stays the same otherwise. Shard status machine (QUEUED → LEASED → RUNNING → RESULT_READY → SUCCEEDED/FAILED/CANCELLED) is fully reused.

### Task

- Remove `InputCSVArtifactID` — no more CSV input
- `ResultArtifactID` becomes optional — not needed if results are per-shard on OSS

### Artifact

- Remove `INPUT_CSV` artifact type
- Remove `SHARD_OUTPUT` storing CSV bytes
- Remove `TASK_OUTPUT_CSV` artifact type
- Results are now on OSS, referenced by key stored on TaskShard

### TaskShard (OSS fields)

Add fields to track OSS output:
- `OutputOSSKey string` — OSS key for the script's output zip (e.g. `tasks/{task_id}/shards/run1.sh/output.zip`)
- `OutputSize int64` — output zip file size
- `OutputSHA256 string` — output zip checksum

### SubmitTaskParams

```go
type SubmitTaskParams struct {
    TenantName        string
    ZipBytes          []byte    // Only zip, no CSV
    CompletionHookURL string
}
```

## Task Submission Flow

1. User uploads zip via GraphQL mutation
2. Server extracts and validates zip: `install.sh` exists, `cmd/*.sh` has scripts
3. Server scans `cmd/` directory, collects all `.sh` filenames
4. Server uploads bundle zip to OSS: `tasks/{task_id}/bundle.zip`
5. Server creates Task record (PENDING)
6. Server creates N TaskShard records (QUEUED), each with `ScriptName`
7. Return task_id

## Agent Execution Flow

### Bundle Caching

Agent maintains an in-memory cache by task_id to avoid re-downloading the same bundle:
- On AssignShard: check if bundle for this task_id is cached locally
- If not cached: download from OSS, extract to local cache directory
- If cached: reuse

### Install Execution

Agent maintains an in-memory set of task_ids that have had install.sh executed:
- First shard for a task_id: run `install.sh`, add task_id to installed set
- Subsequent shards for same task_id: skip install.sh

### Script Execution

For each assigned shard:
1. Create isolated work directory: `/work/{task_id}/{script_name}/`
2. Copy R code from bundle cache into work directory
3. `cd` to work directory
4. Execute `cmd/{script_name}` from bundle cache
5. Script outputs files (CSV, images, etc.) into the work directory (naturally isolated)
6. Pack work directory contents into a zip
7. Upload zip to OSS: `tasks/{task_id}/shards/{script_name}/output.zip`
8. Send `ShardResultReady` to server with: `output_oss_key`, `output_size`, `sha256`

### AssignShard Message (gRPC)

```protobuf
message AssignShard {
    string shard_id = 1;
    string task_id = 2;
    string script_name = 3;         // Which script to execute
    string bundle_oss_key = 4;      // OSS key for bundle download
    string output_oss_prefix = 5;   // OSS prefix for output upload
    int32 shard_index = 6;
    int32 total_shards = 7;
}
```

### ShardResultReady Message (gRPC)

```protobuf
message ShardResultReady {
    string shard_id = 1;
    string output_oss_key = 2;  // OSS key where output was uploaded
    int64 output_size = 3;
    string sha256 = 4;
}
```

### Remove from gRPC

- `FetchArtifact` RPC — agent downloads from OSS directly
- `ShardResultData` message with `bytes output_csv` — agent uploads to OSS instead
- `input_csv_artifact_id` from AssignShard

## Result Collection (Server Side)

When server receives `ShardResultReady`:
1. Validate shard in `RUNNING` state
2. Update shard to `SUCCEEDED`, record `OutputOSSKey`, `OutputSize`, `OutputSHA256`
3. Send `ShardResultStored` ack to agent
4. Reset agent to IDLE
5. Try assign next shard
6. RecomputeTaskStatus: check if all shards reached terminal state

RecomputeTaskStatus no longer merges CSV. It only checks shard completion and updates Task status. If all SUCCEEDED → Task SUCCEEDED. If any FAILED → Task FAILED.

## Result Query (GraphQL)

Extend the existing `Task` type:

```graphql
type TaskScript {
    script_name: String!
    shard_index: Int!
    status: String!
    output_oss_key: String
    output_size: Int
    output_sha256: String
    error_message: String
    started_at: Time
    finished_at: Time
}

type Task {
    id: UUID!
    tenant_id: UUID!
    status: String!
    shard_count: Int!
    scripts: [TaskScript!]!        # New: all script execution statuses
    started_at: Time
    finished_at: Time
    created_at: Time!
    updated_at: Time!
}
```

SubmitTask mutation no longer accepts CSV, only zip upload.

Remove `GetTaskResultCSV` query — replaced by `scripts[].output_oss_key`.

## OSS Integration

### OSS Client

Uses `github.com/yichozy/hopebox/aliyun` package. Initialized once at server startup.

Configuration via env vars (already supported by hopebox/aliyun):
- `ALIYUN_OSS_ACCESS_KEY`
- `ALIYUN_OSS_ACCESS_SECRET`
- `ALIYUN_OSS_BUCKET`
- `ALIYUN_OSS_ENDPOINT`

### OSS Key Layout

```
tasks/{task_id}/bundle.zip                  # Input bundle
tasks/{task_id}/shards/{script_name}/output.zip  # Per-script output
```

### Server-Side OSS Operations

- Upload bundle: `PutObjectFromFile` or `UploadBytes`
- No download needed (agent downloads directly)
- Optionally verify output exists: `CheckObjExist`

### Agent-Side OSS Operations

- Download bundle: generate signed URL via server, or agent has own OSS credentials
- Upload output: `UploadBytes` or `PutObjectFromFile`

## Execution Isolation

Each script runs in an isolated work directory (`/work/{task_id}/{script_name}/`). This ensures:
- Output files from different scripts don't conflict
- Each script's output is self-contained
- Packing output is simply zipping the work directory

## Reuse from Current Architecture

- Shard status machine: fully reused (QUEUED → LEASED → RUNNING → RESULT_READY → SUCCEEDED/FAILED/CANCELLED)
- LeaseNextShard: fully reused (find QUEUED shard, assign to agent)
- PollPendingTasks: fully reused (provision clusters, scale agents)
- Agent heartbeat and timeout: fully reused
- Shard rollback on disconnect: fully reused
- Task cancellation: fully reused (mark shards CANCELLED)
- Completion hook: fully reused

## Files Changed

### Model
- `internal/model/task_shard.go` — add ScriptName, OutputOSSKey, OutputSize, OutputSHA256
- `internal/model/task.go` — remove InputCSVArtifactID

### Service
- `internal/service/task_service/submit_task.go` — new logic: extract zip, scan cmd/*.sh, upload to OSS, create shards
- `internal/service/task_service/report_shard_status.go` — record OSS key instead of csv bytes
- `internal/service/task_service/recompute_task_status.go` — remove csv merge, only check completion
- `internal/service/task_service/get_task_scripts.go` — new: query shard statuses with OSS keys
- Remove `internal/service/task_service/aggregate_task_result_csv.go`
- Remove CSV splitting from `internal/util/util.go`

### Control
- `internal/control/try_assign_shard.go` — include script_name, bundle_oss_key in message
- `internal/control/handle_shard_result_ready.go` — accept OSS key instead of requesting bytes
- `internal/control/handle_shard_result_data.go` — removed (no byte transfer)

### Proto
- `proto/control.proto` — update AssignShard, ShardResultReady, remove FetchArtifact

### GraphQL
- `graph/schema/task.graphqls` — add TaskScript type, extend Task, update SubmitTask
- `graph/task.resolvers.go` — update SubmitTask resolver, add scripts field resolver
- Remove GetTaskResultCSV resolver

### New
- `internal/service/oss.go` — OSS client initialization
- Auto-migration for new TaskShard fields

## Agent-Side Changes (r-agent project)

- Add OSS client (hopebox/aliyun)
- Bundle download + local caching by task_id
- Install.sh execution tracking by task_id (in-memory set)
- Isolated work directory per script
- Output packing and OSS upload
- Update gRPC message handling for new AssignShard/ShardResultReady
