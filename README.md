# r-orchestrator

`r-orchestrator` 是一个多租户批处理任务编排系统，采用 server-agent 架构。用户通过 GraphQL API 提交任务（包含脚本和依赖的 zip 包），server 将任务拆分为多个 shard 并调度到 Kubernetes 上运行的 agent 执行，agent 完成后将输出上传到 OSS。

## 架构概览

```
                    GraphQL API
                        │
                   ┌────┴────┐
                   │  Server  │  (Go)
                   │          │
                   │ gRPC     │◄──── bidirectional stream ────► Agent (Rust)
                   │ Control  │
                   │          │
                   │  K8s     │──► StatefulSet per tenant
                   │ Backend  │
                   └────┬────┘
                        │
                   ┌────┴────┐
                   │   DB    │  PostgreSQL
                   └────┬────┘
                        │
                   ┌────┴────┐
                   │  OSS    │  Aliyun OSS (bundle + output)
                   └─────────┘
```

- **Server** (`cmd/server`): Go 服务，提供 GraphQL API、gRPC control plane、K8s agent 生命周期管理
- **Agent** (`agent/`): Rust 二进制，通过 gRPC 双向流接收 shard 分配，下载 bundle、执行脚本、上传输出

## API

### HTTP 端点

| 端点 | 方法 | 说明 |
|------|------|------|
| `/healthz` | GET | 健康检查 |
| `/graphql` | POST | GraphQL API |

### GraphQL API

#### 查询

**GetTaskByID**

```graphql
query GetTaskByID($tenantName: String!, $taskID: UUID!) {
  GetTaskByID(tenant_name: $tenantName, task_id: $taskID) {
    id
    status
    last_error
    created_at
    started_at
    finished_at
    shard_count
    shards {
      id
      script_name
      status
      output_oss_key
      output_sha256
      last_error
      started_at
      finished_at
    }
  }
}
```

**GetTaskList**

```graphql
query GetTaskList($tenantName: String!, $status: String) {
  GetTaskList(tenant_name: $tenantName, status: $status) {
    id
    status
    last_error
    created_at
    started_at
    finished_at
    shard_count
    shards {
      id
      script_name
      status
      output_oss_key
      output_sha256
      last_error
      started_at
      finished_at
    }
  }
}
```

`status` 为可选参数，传入则按状态过滤（如 `"SUCCEEDED"`、`"RUNNING"` 等）。

#### 变更操作

**SubmitTask** — 提交批处理任务（详见下方 Bundle Zip 结构）

```graphql
mutation SubmitTask($input: SubmitTaskInput!) {
  SubmitTask(input: $input) {
    task_id
  }
}
```

```json
{
  "input": {
    "tenant_name": "my-tenant",
    "bundle_zip": null,
    "completion_hook_url": "https://example.com/hook"
  }
}
```

`bundle_zip` 字段使用 GraphQL multipart request spec 上传文件。

**CancelTask**

```graphql
mutation CancelTask($tenantName: String!, $taskID: UUID!) {
  CancelTask(tenant_name: $tenantName, task_id: $taskID) {
    task_id
    status
  }
}
```

**CreateTenant**

```graphql
mutation CreateTenant($input: CreateTenantInput!) {
  CreateTenant(input: $input) {
    id
    name
    primary_backend_name
    max_agents
  }
}
```

```json
{
  "input": {
    "name": "my-tenant",
    "primary_backend_name": "kubernetes",
    "max_agents": 10
  }
}
```

### SubmitTask — Bundle Zip 结构要求

提交任务时需上传一个 zip 文件（通过 GraphQL multipart `bundle_zip` 字段），zip 必须满足以下结构：

```
bundle.zip
├── install.sh              # 必需，根目录下的安装脚本（依赖安装等）
├── cmd/
│   ├── run_scenario1.sh    # 必需，至少一个 .sh 脚本
│   ├── run_scenario2.sh    # 每个脚本对应一个 shard
│   └── run_scenario3.sh
├── main.R                  # 可选，用户自定义文件
├── sample.txt              # 可选，用户自定义文件
└── ...                     # 其他任意文件和目录
```

**必填项：**
- `install.sh` — 根目录下必须存在，agent 拉起 bundle 后会首先执行此脚本
- `cmd/*.sh` — `cmd/` 目录下至少需要一个 `.sh` 脚本，每个脚本会创建一个独立的 shard

**可选项：**
- zip 内可包含任意其他文件和目录（数据文件、依赖库等），agent 执行脚本时工作目录为解压后的根目录

**验证规则：**
- 缺少 `install.sh` → 返回错误 `"bundle must contain install.sh at root"`
- `cmd/` 下没有 `.sh` 文件 → 返回错误 `"bundle must contain at least one .sh script in cmd/ directory"`

### Agent 执行流程与输出结构

每个 shard 的执行流程：

1. **下载 bundle** — agent 从 OSS 下载 bundle.zip（server 提供预签名 URL）
2. **解压 bundle** — 解压到临时工作目录
3. **执行 install.sh** — `bash install.sh`（可选，存在时执行）
4. **执行 cmd/{script_name}** — `bash cmd/{script_name}.sh`，以解压根目录作为工作目录
5. **收集输出** — 将工作目录下的 `output/` 目录打包为 zip
6. **上传输出** — 通过预签名 URL 上传到 OSS，key 为 `r-orchestrator/tasks/{task_id}/output/{script_name}-output.zip`

**脚本输出规范：**

脚本执行时工作目录为 bundle 解压根目录。如果脚本需要在当前 shard 产生输出文件，应将结果写入 `output/` 子目录：

```bash
#!/bin/bash
# cmd/run_scenario1.sh
mkdir -p output
# 执行计算并将结果写入 output/
Rscript main.R --output output/result.csv
```

**最终输出文件结构（OSS）：**

```
r-orchestrator/tasks/{task_id}/
├── bundle.zip                              # 原始上传的 bundle
└── output/
    ├── run_scenario1-output.zip            # shard 1 的输出（output/ 目录的 zip）
    ├── run_scenario2-output.zip            # shard 2 的输出
    └── run_scenario3-output.zip            # shard 3 的输出
```

每个 output zip 内部结构取决于脚本写入 `output/` 目录的内容。如果 `output/` 目录不存在，则生成空 zip。

输出 zip 的 SHA256 值会记录在 shard 的 `output_sha256` 字段中，可用于完整性校验。

### Completion Hook

提交任务时可指定 `completion_hook_url`。任务达到终态（SUCCEEDED/FAILED）后，server 会向该 URL 发送 POST 请求：

```json
{
  "task_id": "019...",
  "tenant_id": "019...",
  "status": "SUCCEEDED",
  "last_error": "",
  "finished_at": "2026-06-23T10:00:00Z",
  "result_available": true
}
```

- `result_available` 为 `true` 时表示所有 shard 输出已上传到 OSS
- Hook 请求超时 15 秒，期望返回 2xx 状态码

## 任务生命周期

```
PENDING → WAITING_FOR_AGENTS → QUEUED → RUNNING → SUCCEEDED / FAILED / CANCELLED
```

| 状态 | 说明 |
|------|------|
| `PENDING` | 任务已创建，等待 server 拉起 agent |
| `WAITING_FOR_AGENTS` | server 正在通过 K8s 拉起 agent pod |
| `QUEUED` | agent 已就绪，shard 进入可分配队列 |
| `RUNNING` | 至少一个 shard 正在执行 |
| `SUCCEEDED` | 所有 shard 执行成功 |
| `FAILED` | 至少一个 shard 执行失败 |
| `CANCELLED` | 任务被用户取消 |

**Shard 状态：**

```
QUEUED → LEASED → RUNNING → SUCCEEDED / FAILED / CANCELLED
```

| 状态 | 说明 |
|------|------|
| `QUEUED` | 等待分配给 agent |
| `LEASED` | 已分配给 agent，等待确认 |
| `RUNNING` | 正在执行 |
| `SUCCEEDED` | 执行成功，输出已上传 |
| `FAILED` | 执行失败，可被重新分配 |
| `CANCELLED` | 任务被取消 |

## gRPC Control 协议

Server 与 Agent 之间通过双向流 gRPC 通信（`proto/control.proto`）。

**Agent → Server 消息：**
- `Register` — agent 注册，携带 tenant、backend、token 信息
- `Heartbeat` — 定期心跳，上报当前状态和正在执行的 shard
- `ShardAccepted` — 确认接收 shard 分配
- `ShardStarted` — shard 开始执行
- `ShardResultReady` — shard 执行完成，上报输出 OSS key 和 SHA256
- `ShardFailed` — shard 执行失败

**Server → Agent 消息：**
- `AssignShard` — 分配 shard，包含 bundle 预签名下载 URL 和输出预签名上传 URL
- `CancelShard` — 取消正在执行的 shard
- `ShardResultStored` — 服务端确认 shard 结果已持久化
- `Drain` / `Shutdown` — 通知 agent 优雅退出

## 环境变量

**必需：**

| 变量 | 说明 |
|------|------|
| `CLUSTER_AGENT_TOKEN` | agent 认证 token |

**数据库：**

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `DB_HOST` | PostgreSQL 主机 | — |
| `DB_PORT` | PostgreSQL 端口 | — |
| `DB_USER` | 数据库用户 | — |
| `DB_PASSWORD` | 数据库密码 | — |
| `DB_NAME` | 数据库名 | — |

**Server 配置：**

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `SERVER_HTTP_ADDR` | HTTP 监听地址 | `:8089` |
| `SERVER_GRPC_ADDR` | gRPC 监听地址 | `:9090` |
| `SERVER_GRPC_PUBLIC_ADDR` | agent 回连 gRPC 的公网地址 | — |
| `SERVER_PUBLIC_URL` | server 公网 URL | — |
| `LOG_LEVEL` | 日志级别 | `info` |

**K8s 后端：**

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `CLUSTER_KUBERNETES_NAMESPACE` | K8s 命名空间 | `r-agents` |
| `CLUSTER_KUBERNETES_KUBECONFIG_PATH` | kubeconfig 路径 | — |
| `CLUSTER_KUBERNETES_IMAGE_PULL_SECRETS` | 镜像拉取密钥（JSON 数组） | — |
| `CLUSTER_AGENT_IMAGE` | agent 镜像 | `r-orchestrator/agent:latest` |
| `CLUSTER_AGENT_LOG_LEVEL` | agent 日志级别 | `info` |

**Agent 配置（注入到 pod 环境变量）：**

| 变量 | 说明 |
|------|------|
| `RORCHESTRATOR_SERVER_GRPC_ADDR` | server gRPC 地址 |
| `RORCHESTRATOR_TENANT_ID` | 所属租户 ID |
| `RORCHESTRATOR_BACKEND_NAME` | 后端名称 |
| `RORCHESTRATOR_AGENT_TOKEN` | 认证 token |
| `RUST_LOG` | Rust 日志级别 |

## 本地启动

```bash
# 启动 server
go run ./cmd/server

# 运行测试
go test ./...
cargo test -p agent
```

## 部署

项目包含 Helm chart（`chart/r-orchestrator/`），通过 GitHub Actions（`.github/workflows/dev.yml`）构建镜像并部署：

- 触发条件：push release tag `vX.Y.Z`
- 构建镜像：`r-orchestrator:{tag}`（server）+ `r-agent:{tag}`（agent）
- 部署目标：K8s namespace `yi-dev`

## 关键目录

| 目录 | 说明 |
|------|------|
| `cmd/server/` | 服务启动入口 |
| `graph/` | GraphQL schema 与 resolver |
| `internal/model/` | 持久化模型（Task, TaskShard, Cluster, Agent 等） |
| `internal/orm/` | GORM 数据访问层 |
| `internal/service/task_service/` | 任务提交、查询、取消、状态管理 |
| `internal/service/agent_service/` | agent 注册与心跳 |
| `internal/service/cluster_service/` | K8s agent 生命周期管理（拉起、回收） |
| `internal/control/` | gRPC 控制面（shard 分配、消息路由） |
| `agent/` | Rust agent（gRPC 客户端、执行器、OSS 上传） |
| `proto/` | gRPC protobuf 定义 |
| `chart/` | Helm chart |
