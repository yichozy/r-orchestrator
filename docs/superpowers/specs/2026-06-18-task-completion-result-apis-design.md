# Task Completion Hook, Polling, And Result Download Design

## Summary

This document designs three task-facing capabilities on top of the existing task execution flow:

1. `SubmitTask` accepts an optional completion hook URL so the server can notify external callers when a task reaches a final status.
2. Existing task queries are expanded so clients can poll task progress and result readiness without introducing a separate polling API.
3. The server produces a task-level aggregated CSV from shard outputs and exposes it through a dedicated GraphQL query.

The design intentionally keeps the implementation close to the current execution chain:

- task creation still starts in GraphQL and is persisted in `tasks` plus `artifacts`
- shard outputs continue to be stored from the control plane when agents report completion
- task finalization remains centralized around task status recomputation
- the final CSV is fetched through a dedicated GraphQL query instead of a separate HTTP route

## Goals

- Allow callers to provide a completion callback when submitting a task.
- Reuse existing GraphQL task queries for polling instead of adding a second status API.
- Provide a dedicated GraphQL query for fetching the final aggregated CSV.
- Generate the final CSV only when a task succeeds.
- Keep the design minimal and aligned with the current code structure.

## Non-Goals

- No webhook authentication in this iteration.
- No webhook retry queue or delivery scheduler in this iteration.
- No object storage or external file hosting in this iteration.
- No separate HTTP download route in this iteration.
- No real-time push subscription API in this iteration.

## Current State

### GraphQL

Current task GraphQL already exposes:

- `SubmitTask(input: SubmitTaskInput!): SubmitTaskResponse!`
- `GetTaskByID(tenant_id: UUID!, task_id: UUID!): Task!`
- `GetTaskList(tenant_id: UUID!, status: String): [Task!]!`

The current `Task` GraphQL type only includes:

- `id`
- `tenant_id`
- `status`
- `last_error`

This means task polling already exists in shape, but it lacks the fields needed to track lifecycle timing and result availability.

### Task lifecycle

Current task status progression is already managed in the service layer:

- task starts as `PENDING`
- shard assignment and start events can move the task to `RUNNING`
- shard completion and failure ultimately feed into task recomputation
- final task state is decided in `RecomputeTaskStatus`

### Artifacts

Artifacts already exist for:

- submitted R bundle zip
- submitted input CSV
- per-shard output CSV

There is not yet a task-level aggregated output artifact.

## Proposed API Design

### GraphQL schema changes

`SubmitTaskInput` gains one optional field:

```graphql
input SubmitTaskInput {
  tenant_id: UUID!
  r_zip_file: Upload!
  parameters_csv_file: Upload!
  completion_hook_url: String
}
```

`Task` is expanded so polling can expose execution lifecycle and result availability:

```graphql
type Task {
  id: UUID!
  tenant_id: UUID!
  status: String!
  last_error: String!
  created_at: Time!
  started_at: Time
  finished_at: Time
  shard_count: Int!
}
```

No new polling query is added. Clients continue polling `GetTaskByID` or `GetTaskList`.

### Why no dedicated polling API

The current queries already model the resource correctly. Adding a second query such as `PollTaskStatus` would create duplicate read paths for the same task state without adding real capability. The missing piece is richer task data, not a second endpoint name.

### Result fetch shape

GraphQL exposes a dedicated query for the final CSV content rather than placing the CSV on `GetTaskByID` or returning an HTTP download URL.

Recommended schema:

```graphql
extend type Query {
  GetTaskResultCSV(tenant_id: UUID!, task_id: UUID!): TaskResultCSV!
}

type TaskResultCSV {
  task_id: UUID!
  filename: String!
  content_type: String!
  csv_content: String!
}
```

Behavior:

- `GetTaskByID` and `GetTaskList` remain status polling APIs only
- `GetTaskResultCSV` is callable only when the task is `SUCCEEDED`
- the result content is returned as UTF-8 CSV text
- this design is acceptable for the current expected result size range of roughly `1-20 MB`

## Data Model Changes

### Task model additions

The `tasks` table gains:

- `completion_hook_url`
- `result_artifact_id`
- `hook_delivered_at`
- `hook_last_error`

Recommended Go model shape:

```go
type Task struct {
    BaseUUIDModel
    TenantID           uuid.UUID
    Status             string
    BundleArtifactID   uuid.UUID
    InputCSVArtifactID uuid.UUID
    ResultArtifactID   *uuid.UUID
    CompletionHookURL  string
    ShardCount         int
    StartedAt          *time.Time
    FinishedAt         *time.Time
    HookDeliveredAt    *time.Time
    HookLastError      string
    LastError          string
}
```

Notes:

- `ResultArtifactID` is nullable because many tasks will not have a final result yet.
- `CompletionHookURL` is optional and defaults to empty string.
- `HookDeliveredAt` and `HookLastError` allow later retry work without redesigning the schema.

### Artifact model additions

Add a new artifact type:

```go
const ArtifactTypeTaskOutput = "TASK_OUTPUT_CSV"
```

No new table is introduced. The aggregated result remains a normal artifact row tied to the task.

## Task Finalization Design

### Single finalization boundary

Task finalization remains centralized in the task status recomputation path. This is the correct place to attach both:

- final result aggregation
- completion webhook dispatch

This prevents the same side effects from being scattered across GraphQL resolvers, control stream handlers, or shard completion handlers.

### Finalization rules

When recomputation determines the task still has active shards:

- do nothing beyond status maintenance

When recomputation determines the task has terminal failure:

- mark task as `FAILED`
- set `finished_at`
- persist `last_error`
- do not create an aggregated result artifact
- trigger completion hook dispatch if `completion_hook_url` is present

When recomputation determines the task succeeded:

- aggregate shard outputs into a final CSV
- create a `TASK_OUTPUT_CSV` artifact
- set `result_artifact_id`
- mark task as `SUCCEEDED`
- set `finished_at`
- clear `last_error`
- trigger completion hook dispatch if `completion_hook_url` is present

When the task is cancelled:

- leave result artifact empty
- trigger completion hook dispatch if `completion_hook_url` is present

### Ordering guarantees

For successful tasks, the aggregated result artifact must exist before the task is visible to clients as `SUCCEEDED`. This avoids an inconsistent state where polling sees task success but the result query still fails because aggregation is not complete yet.

That means the success path should follow this order inside the completion flow:

1. load shard outputs
2. aggregate CSV bytes
3. create task output artifact
4. update task to `SUCCEEDED`, set `finished_at`, set `result_artifact_id`
5. enqueue or trigger webhook dispatch

## CSV Aggregation Rules

### Input source

Aggregation reads all artifacts of type `SHARD_OUTPUT` for the task, ordered by `shard_index` ascending.

### Merge rules

- the first shard output contributes the header row plus data rows
- later shard outputs must have the same header row
- later shard outputs contribute only data rows
- blank trailing newline differences are tolerated

### Failure rules

Aggregation fails the task if:

- any expected shard output artifact is missing
- shard outputs have inconsistent headers
- a shard output is not valid CSV
- no shard outputs exist for a supposedly successful multi-shard task

When aggregation fails:

- task is marked `FAILED`
- `last_error` describes the aggregation problem
- no result artifact is attached
- completion hook still fires once with final failed status

### Single-shard tasks

Single-shard success still goes through the same aggregation path. The aggregated artifact is simply the normalized content of that shard output.

## Result Query

### GraphQL query

Add a dedicated result query:

```graphql
extend type Query {
  GetTaskResultCSV(tenant_id: UUID!, task_id: UUID!): TaskResultCSV!
}
```

This query:

- validates the task belongs to the tenant
- validates the task status is `SUCCEEDED`
- loads `result_artifact_id` from the task
- reads the stored task-level CSV artifact
- returns the CSV content as a string

### Response shape

Recommended GraphQL payload:

```graphql
type TaskResultCSV {
  task_id: UUID!
  filename: String!
  content_type: String!
  csv_content: String!
}
```

Recommended values:

- `filename`: `task-<task_id>-result.csv`
- `content_type`: `text/csv; charset=utf-8`
- `csv_content`: aggregated CSV text

### Why a dedicated query

This keeps status polling lightweight while still allowing machine clients to fetch the complete final CSV through GraphQL. Because the expected result size is around `1-20 MB`, returning text content through a dedicated query is acceptable for this iteration.

## Webhook Design

### Trigger policy

The completion hook is sent exactly once per task finalization attempt in this iteration, only when the task reaches a final status:

- `SUCCEEDED`
- `FAILED`
- `CANCELLED`

No callbacks are sent for intermediate states such as `PENDING`, `QUEUED`, or `RUNNING`.

### Request format

Method:

- `POST`

Headers:

- `Content-Type: application/json`

No authentication headers are added in this iteration.

Recommended JSON body:

```json
{
  "task_id": "019ed9ae-6c3a-754f-922e-2b6dbebc6081",
  "tenant_id": "019ed9ad-f271-7791-909b-b3c20c9e6540",
  "status": "SUCCEEDED",
  "last_error": "",
  "finished_at": "2026-06-18T12:34:56Z",
  "result_available": true
}
```

For failed and cancelled tasks, `result_available` is `false`. Callers fetch the actual CSV later through `GetTaskResultCSV`.

### Delivery model

The callback should not be executed inline inside the shard report transaction or any critical control-plane transaction. External HTTP calls are unpredictable and should not block database commit of task completion.

For this iteration, the recommended minimum design is:

1. finalize task state and aggregated artifact in the database
2. after commit, launch asynchronous dispatch in-process
3. record delivery result in `hook_delivered_at` or `hook_last_error`

This is intentionally not a durable retry queue, but it avoids coupling external network latency to the main task state transition transaction.

### Idempotency expectation

This iteration aims for one logical delivery attempt per finalization. However, callers should still treat the webhook as idempotent because future retry support may deliver duplicates. The payload is sufficient for idempotency on caller side because it includes stable task and tenant identifiers.

## Resolver And Service Responsibilities

### GraphQL resolver layer

Responsibilities:

- validate `tenant_id`
- read upload bytes
- pass `completion_hook_url` through to task service
- expose enriched task fields from task read models

Non-responsibilities:

- no webhook dispatch
- no CSV aggregation
- no direct file content transfer on the task polling queries

### Task service layer

Responsibilities:

- persist `completion_hook_url` during submission
- compute final task state
- aggregate shard outputs on success
- persist result artifact metadata
- coordinate completion hook dispatch after finalization

### Control plane layer

Responsibilities:

- continue storing per-shard output artifacts when agents report completion

Non-responsibilities:

- no task-level aggregation logic
- no webhook logic

## Error Handling

### Submit task

Invalid hook URL should be rejected during submission if it is not a valid absolute URL. This prevents storing unusable values that later always fail dispatch.

### Polling

Task read APIs continue to return task state even if:

- webhook delivery failed
- result generation failed and task has already been marked `FAILED`

This ensures callers can always inspect the final truth from the task resource.

### Result query

Return behavior:

- not found error if task not found for tenant
- business error if task exists but status is not `SUCCEEDED`
- not found error if task is `SUCCEEDED` but the final result artifact is missing

### Webhook delivery failure

Webhook failure must not revert the already committed task final state. Failures only affect:

- `hook_last_error`
- observability logs

## Observability

### Task completion logging

Add structured logs for:

- aggregation started
- aggregation succeeded with artifact id and content size
- aggregation failed with task id and error
- webhook dispatch started
- webhook dispatch succeeded
- webhook dispatch failed

## Testing Strategy

### Unit or service-level coverage

Add focused tests for:

- submit task persists `completion_hook_url`
- task polling read model exposes lifecycle timestamps and shard count
- result query returns final CSV for successful tasks
- successful multi-shard aggregation produces one `TASK_OUTPUT_CSV`
- header mismatch causes task failure
- missing shard output causes task failure
- completion hook payload contents for succeeded, failed, and cancelled tasks

### Result query coverage

Add query tests for:

- valid final CSV fetch
- task belongs to another tenant
- task is not succeeded yet
- task has no result artifact

### Regression coverage

Keep existing execution flow tests intact so the new completion logic does not change:

- shard assignment
- shard running state
- shard completion persistence
- cancellation behavior

## Rollout Notes

- Schema changes are backward compatible because new fields are additive and `completion_hook_url` is optional.
- The only new behavior change on success is creation of a task-level result artifact and availability of a dedicated result query.
- Failed and cancelled tasks remain terminal without result files.

## Open Decisions Resolved In This Design

- completion callback fires only once at final task state
- existing task queries remain the polling mechanism
- GraphQL exposes a dedicated query for the final CSV content
- no webhook authentication in the first iteration
- no retry queue in the first iteration

## Implementation Outline

Expected implementation will likely touch:

- GraphQL schema and generated models
- task submission service
- task read models and resolvers
- task status recomputation path
- artifact query helpers for task-level aggregation
- GraphQL result query resolver

The follow-up implementation plan should split the work into:

1. schema and model changes
2. task read path updates
3. aggregation and finalization logic
4. result query
5. webhook dispatch
6. focused tests
