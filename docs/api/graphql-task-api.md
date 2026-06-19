# GraphQL Task API

本文档描述当前对外 GraphQL API，重点覆盖租户创建、任务提交、任务查询、结果拉取、任务取消，以及 `completion_hook_url` 的回调行为。

默认 GraphQL HTTP 入口：

```text
POST /graphql
```

## Overview

当前对外 API 使用 `tenant_name` 作为租户标识。

- 创建租户时传 `name`
- 任务相关 API 全部传 `tenant_name`
- `task_id` 使用 UUID
- 任务结果通过单独的 `GetTaskResultCSV` 查询获取

## Data Types

### Tenant

```graphql
type Tenant {
  id: UUID!
  name: String!
  primary_backend_name: String!
  max_agents: Int!
}
```

### Task

```graphql
type Task {
  id: UUID!
  tenant_name: String!
  status: String!
  last_error: String!
  created_at: Time!
  started_at: Time
  finished_at: Time
  shard_count: Int!
}
```

### TaskResultCSV

```graphql
type TaskResultCSV {
  task_id: UUID!
  filename: String!
  content_type: String!
  csv_content: String!
}
```

## Task Status

当前任务状态包括：

- `PENDING`
- `WAITING_FOR_AGENTS`
- `QUEUED`
- `RUNNING`
- `SUCCEEDED`
- `FAILED`
- `CANCELLED`

调用建议：

- 轮询任务状态时优先看 `status`
- 失败或取消时看 `last_error`
- 只有 `SUCCEEDED` 状态才能调用 `GetTaskResultCSV`

## CreateTenant

### Schema

```graphql
input CreateTenantInput {
  name: String!
  primary_backend_name: String!
  max_agents: Int!
}

mutation CreateTenant($input: CreateTenantInput!) {
  CreateTenant(input: $input) {
    id
    name
    primary_backend_name
    max_agents
  }
}
```

### Parameter Rules

- `name`
  - 必填
  - 会先 `trim` 再转小写存储
  - 必须唯一
  - 建议直接使用稳定的小写业务名，例如 `team-alpha`
- `primary_backend_name`
  - 必填
  - 当前用于匹配该租户使用的 backend
- `max_agents`
  - 必填
  - 用于控制任务切 shard 的上限

### Example

```bash
curl -s http://127.0.0.1:8089/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"mutation($input: CreateTenantInput!) { CreateTenant(input: $input) { id name primary_backend_name max_agents } }",
    "variables":{
      "input":{
        "name":"team-alpha",
        "primary_backend_name":"ray",
        "max_agents":2
      }
    }
  }'
```

## SubmitTask

`SubmitTask` 是 multipart GraphQL Upload 请求，不是普通 JSON 请求。

### Schema

```graphql
input SubmitTaskInput {
  tenant_name: String!
  r_zip_file: Upload!
  parameters_csv_file: Upload!
  completion_hook_url: String
}

mutation SubmitTask($input: SubmitTaskInput!) {
  SubmitTask(input: $input) {
    task_id
  }
}
```

### Parameter Rules

#### `tenant_name`

- 必填
- 必须对应一个已存在租户
- 查询时会先做 `trim` 和小写归一化
- 推荐直接传租户创建时的标准小写名称

如果租户不存在，请求会失败。

#### `r_zip_file`

- 必填
- 通过 GraphQL Upload 上传
- 文件内容不能为空
- 应为任务执行 bundle zip

当前 resolver 只校验“文件非空”；更细的 zip 内容是否合法，会在后续执行阶段暴露。

建议 zip 内至少包含任务运行所需脚本和资源，例如：

```text
bundle/
  run.sh
```

#### `parameters_csv_file`

- 必填
- 通过 GraphQL Upload 上传
- 文件内容不能为空
- 必须是合法 CSV
- 必须至少包含：
  - 1 行 header
  - 1 行数据

如果 CSV 为空、没有数据行、或无法被解析，请求会失败。

#### `completion_hook_url`

- 可选
- 如果传入，会在任务进入终态后触发 HTTP 回调
- 允许为空或不传
- 如果传入，必须满足：
  - 是绝对 URL
  - `scheme` 只能是 `http` 或 `https`
  - 必须带 host

例如：

- 合法：`http://127.0.0.1:18080/hook`
- 合法：`https://example.com/task-hook`
- 非法：`/hook`
- 非法：`ftp://example.com/hook`

### CSV Sharding Behavior

`parameters_csv_file` 会按租户的 `max_agents` 拆分成多个 shard。

拆分规则：

- 保留原始 header
- 数据行按轮询方式分配到各 shard
- 最终 `shard_count <= max_agents`
- 如果数据行数少于 `max_agents`，实际 shard 数会小于 `max_agents`

例如：

- `max_agents = 2`
- 输入 CSV：

```csv
id,value
1,a
2,b
3,c
4,d
```

会拆成 2 个 shard：

```csv
id,value
1,a
3,c
```

```csv
id,value
2,b
4,d
```

### SubmitTask Response

```json
{
  "data": {
    "SubmitTask": {
      "task_id": "019eddd3-6173-74ed-935f-af8b79120d1c"
    }
  }
}
```

### Multipart cURL Example

```bash
curl 'http://127.0.0.1:8089/graphql' \
  -H 'Accept: application/json' \
  -F 'operations={
    "query":"mutation SubmitTask($input: SubmitTaskInput!) { SubmitTask(input: $input) { task_id } }",
    "variables":{
      "input":{
        "tenant_name":"team-alpha",
        "r_zip_file":null,
        "parameters_csv_file":null,
        "completion_hook_url":"http://127.0.0.1:18080/hook"
      }
    }
  }' \
  -F 'map={ "0":["variables.input.r_zip_file"], "1":["variables.input.parameters_csv_file"] }' \
  -F '0=@/path/to/bundle.zip;type=application/zip' \
  -F '1=@/path/to/params.csv;type=text/csv'
```

### Common SubmitTask Errors

- `tenant name is required`
  - `tenant_name` 为空或全空白
- `zip is required`
  - 上传的 zip 文件为空
- `csv is required`
  - 上传的 CSV 文件为空
- `invalid completion hook url: ...`
  - `completion_hook_url` 不是合法绝对 `http(s)` URL
- `tenant not found`
  - `tenant_name` 对应的租户不存在
- `split csv: csv is empty`
  - CSV 没有任何内容
- `split csv: csv has no data rows`
  - 只有 header，没有数据行

## GetTaskByID

### Schema

```graphql
query GetTaskByID($tenant_name: String!, $task_id: UUID!) {
  GetTaskByID(tenant_name: $tenant_name, task_id: $task_id) {
    id
    tenant_name
    status
    last_error
    created_at
    started_at
    finished_at
    shard_count
  }
}
```

### Parameter Rules

- `tenant_name`
  - 必填
  - 必须与任务所属租户一致
- `task_id`
  - 必填
  - 必须是合法 UUID

### Example

```bash
curl -s http://127.0.0.1:8089/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"query($tenant_name: String!, $task_id: UUID!) { GetTaskByID(tenant_name: $tenant_name, task_id: $task_id) { id tenant_name status last_error created_at started_at finished_at shard_count } }",
    "variables":{
      "tenant_name":"team-alpha",
      "task_id":"019eddd3-6173-74ed-935f-af8b79120d1c"
    }
  }'
```

## GetTaskList

### Schema

```graphql
query GetTaskList($tenant_name: String!, $status: String) {
  GetTaskList(tenant_name: $tenant_name, status: $status) {
    id
    tenant_name
    status
    last_error
    created_at
    started_at
    finished_at
    shard_count
  }
}
```

### Parameter Rules

- `tenant_name`
  - 必填
- `status`
  - 可选
  - 如果传入，则按精确状态过滤
  - 推荐值：`PENDING`、`WAITING_FOR_AGENTS`、`QUEUED`、`RUNNING`、`SUCCEEDED`、`FAILED`、`CANCELLED`

### Example

```bash
curl -s http://127.0.0.1:8089/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"query($tenant_name: String!, $status: String) { GetTaskList(tenant_name: $tenant_name, status: $status) { id tenant_name status shard_count } }",
    "variables":{
      "tenant_name":"team-alpha",
      "status":"RUNNING"
    }
  }'
```

## GetTaskResultCSV

### Schema

```graphql
query GetTaskResultCSV($tenant_name: String!, $task_id: UUID!) {
  GetTaskResultCSV(tenant_name: $tenant_name, task_id: $task_id) {
    task_id
    filename
    content_type
    csv_content
  }
}
```

### Parameter Rules

- `tenant_name`
  - 必填
- `task_id`
  - 必填
  - 必须属于该租户

### Availability Rule

只有 `task.status = SUCCEEDED` 时才能获取结果。

如果任务仍处于以下状态，会返回错误：

- `PENDING`
- `WAITING_FOR_AGENTS`
- `QUEUED`
- `RUNNING`
- `FAILED`
- `CANCELLED`

### Example

```bash
curl -s http://127.0.0.1:8089/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"query($tenant_name: String!, $task_id: UUID!) { GetTaskResultCSV(tenant_name: $tenant_name, task_id: $task_id) { task_id filename content_type csv_content } }",
    "variables":{
      "tenant_name":"team-alpha",
      "task_id":"019eddd3-6173-74ed-935f-af8b79120d1c"
    }
  }'
```

### Response Example

```json
{
  "data": {
    "GetTaskResultCSV": {
      "task_id": "019eddd3-6173-74ed-935f-af8b79120d1c",
      "filename": "task-019eddd3-6173-74ed-935f-af8b79120d1c-result.csv",
      "content_type": "text/csv; charset=utf-8",
      "csv_content": "id,value\n1,a\n2,b\n3,c\n4,d\n"
    }
  }
}
```

## CancelTask

### Schema

```graphql
mutation CancelTask($tenant_name: String!, $task_id: UUID!) {
  CancelTask(tenant_name: $tenant_name, task_id: $task_id) {
    task_id
    status
  }
}
```

### Behavior

- 当前会把任务标记为 `CANCELLED`
- 如果有正在运行的 shard，会尝试通知 agent 取消
- 任务进入终态后，如果配置了 `completion_hook_url`，仍会发送 completion hook

### Example

```bash
curl -s http://127.0.0.1:8089/graphql \
  -H 'Content-Type: application/json' \
  -d '{
    "query":"mutation($tenant_name: String!, $task_id: UUID!) { CancelTask(tenant_name: $tenant_name, task_id: $task_id) { task_id status } }",
    "variables":{
      "tenant_name":"team-alpha",
      "task_id":"019eddd3-6173-74ed-935f-af8b79120d1c"
    }
  }'
```

## Completion Hook

如果 `SubmitTaskInput.completion_hook_url` 不为空，任务进入终态后会异步发送一个 HTTP `POST` 请求。

### Trigger Timing

以下终态会触发回调：

- `SUCCEEDED`
- `FAILED`
- `CANCELLED`

### Success Criteria

- 服务端认为 HTTP `2xx` 为回调成功
- 非 `2xx` 或网络错误都视为失败

### HTTP Request

- Method: `POST`
- Content-Type: `application/json`

### Payload

当前回调体为：

```json
{
  "task_id": "019eddd3-6173-74ed-935f-af8b79120d1c",
  "tenant_id": "00000000-0000-0000-0000-000000000001",
  "status": "SUCCEEDED",
  "last_error": "",
  "finished_at": "2026-06-19T03:15:22Z",
  "result_available": true
}
```

字段说明：

- `task_id`
  - 任务 UUID
- `tenant_id`
  - 当前回调里仍然返回内部租户 UUID
- `status`
  - 终态，取值为 `SUCCEEDED` / `FAILED` / `CANCELLED`
- `last_error`
  - 失败或取消时的错误信息；成功时通常为空
- `finished_at`
  - 任务完成时间，UTC
- `result_available`
  - 只有 `SUCCEEDED` 且结果已可用时为 `true`
  - `FAILED` / `CANCELLED` 时为 `false`

### Logging

成功发送后，server 日志会打印：

```text
completion hook dispatched
```

并附带字段：

- `task_id`
- `hook_url`
- `status`
- `result_available`

失败时会打印：

```text
dispatch completion hook failed
```

## Recommended Client Flow

推荐的外部调用顺序：

1. `CreateTenant`
2. `SubmitTask`
3. 轮询 `GetTaskByID` 或 `GetTaskList`
4. 任务变为 `SUCCEEDED` 后调用 `GetTaskResultCSV`
5. 如有需要，使用 `CancelTask`

如果使用 `completion_hook_url`，则还可以在 hook 到达后再拉结果。

## Notes

- `SubmitTask` 是 multipart upload，不要用普通 JSON 直接传文件内容
- `GetTaskResultCSV` 返回完整 CSV 文本，不返回下载链接
- `tenant_name` 是当前所有外部 task API 的租户标识
- `tenant_name` 推荐始终使用小写稳定名称
