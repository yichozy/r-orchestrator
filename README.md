# r-orchestrator

`r-orchestrator` 是一个面向多租户批处理场景的 `server-agent` 控制面原型。

当前代码基线已经具备：

- 基于 `Gin + GraphQL` 的 HTTP 入口
- 基于 `GORM` 的 PostgreSQL 持久化与 `AutoMigrate`
- `GetTaskByID / GetTaskList / SubmitTask / CancelTask` 的 GraphQL Task 视角接口
- 最小 `control` gRPC 协议实现：`OpenControlStream`、`FetchArtifact`
- 最小 `tenant_service` / `agent_service` 主链路
- `server` 进程内后台 `provisioning coordinator`，异步处理 `SCALING_AGENTS` 任务

## 环境变量

启动 `cmd/server` 需要以下环境变量：

- `AGENT_TOKEN`
- `DB_HOST`
- `DB_PORT`
- `DB_USER`
- `DB_PASSWORD`
- `DB_NAME`

可选环境变量：

- `SERVER_HTTP_ADDR`，默认 `:8089`
- `SERVER_GRPC_ADDR`，默认 `:9090`
- `SERVER_GRPC_PUBLIC_ADDR`：新拉起的 agent 用于回连 `server` 的 gRPC 地址（当 `PROVISIONING_ENABLED=true` 时通常需要）
- `SERVER_PUBLIC_URL`
- `PROVISIONING_ENABLED`，默认 `false`
- `PROVISIONING_PROVIDER_KIND`，默认 `k8s`
- `PROVISIONING_K8S_NAMESPACE`，默认 `default`
- `PROVISIONING_K8S_KUBECONFIG_PATH`
- `PROVISIONING_AGENT_IMAGE`，默认 `r-orchestrator/agent:latest`
- `PROVISIONING_IDLE_TTL_SECONDS`，默认 `900`

其中 PostgreSQL 连接串由 `internal/config` 使用 `DB_*` 变量组装。

`cmd/server` 的加载顺序与优先级如下：

- `cmd/server/main.go` 会先调用 `internal/config.LoadEnvVariable()`，再调用 `internal/config.LoadFromEnv()`
- 当 `ENV=prod` 时，跳过 `.env` 加载
- 当 `ENV` 不是 `prod` 时，会尝试加载当前目录或上层目录中的 `.env`
- 若 `.env` 缺失，会继续使用当前进程环境变量，不把缺失 `.env` 视为启动错误
- 若显式环境变量与 `.env` 同时提供同名键，以显式环境变量为准
- 若 `.env` 读取遇到非“文件不存在”的 I/O 错误，则启动失败

## 本地启动

```bash
go run ./cmd/server
```

当前 `cmd/server/main.go` 会：

- 先按上述规则尝试加载 `.env`
- 读取配置
- 打开 PostgreSQL 连接
- 执行 `AutoMigrate`
- 构造 `provisioning_service` 的 SkyPilot HTTP 配置，并在同一进程内启动后台 `provisioning loop`
- 启动 gRPC control 服务
- 启动 HTTP 服务

## HTTP 入口

- 健康检查：`GET /healthz`
- GraphQL：`POST /graphql`

当前 `cmd/server/main.go` 直接挂载 `gqlgen` handler，没有额外 playground 路由。

## SkyPilot Provisioning

当前 SkyPilot 集成的主运行链路已经收敛为独立 server 模式：

- `r-orchestrator` 不再本地执行 `sky launch` CLI
- `internal/service/provisioning_service` 会读取 `SKYPILOT_TASK_TEMPLATE` 指向的本地模板文件
- 模板文件内容会原样作为 `/launch` 请求体中的 `task`
- `RORCHESTRATOR_TENANT_ID`、`RORCHESTRATOR_SERVER_GRPC_ADDR`、`RORCHESTRATOR_AGENT_TOKEN`、`RORCHESTRATOR_BACKEND_NAME` 会通过 `/launch` 请求体中的 `env_vars` 注入
- `SKYPILOT_SERVER_URL` 用于指定独立部署的 SkyPilot server 根地址，当前主链路只依赖 `POST /launch`

补充说明：

- agent 副本数通过 task template 中的 `__RORCHESTRATOR_NUM_NODES__` 占位符渲染到 `num_nodes`（而不是通过额外的 env var）

补充说明：

- 历史 `internal/backends/skypilot` CLI / adapter legacy 文件已删除
- 当前 provisioning 主路径只保留独立 SkyPilot server 集成，不再走本地 `sky launch`

## GraphQL 主入口

当前 GraphQL schema 位于：

- `graph/schema/task.graphqls`

当前对外只保留 4 个 Task 视角 API：

- Query：`GetTaskByID`
- Query：`GetTaskList`
- Mutation：`SubmitTask`
- Mutation：`CancelTask`

当前 `Task` DTO 只暴露以下字段：

- `id`
- `tenant_id`
- `status`
- `backend_name`
- `last_error`

其中：

- `id`、`task_id` 使用 `scalar UUID`，在 gqlgen 中映射到 `github.com/google/uuid.UUID`
- `Shard` 仍存在于内部调度与持久化层，但不再作为 GraphQL 对外概念暴露
- 历史上曾短暂使用 `Run / BatchRun` 术语，但当前不再作为对外接口语义保留

正式设计文档：

- `docs/superpowers/specs/` 下的 task architecture 设计稿

## Control 协议

控制面协议定义位于：

- `proto/control.proto`

当前主路径为 gRPC service：

- `rorchestrator.control.v1.ControlService/OpenControlStream`
- `rorchestrator.control.v1.ControlService/FetchArtifact`

当前最小实现位于：

- `internal/control/server.go`

其中：

- `OpenControlStream` 处理 `Register`、`Heartbeat`
- `OpenControlStream` 在 agent 可派发 shard 时下发 `AssignShard`
- `FetchArtifact` 按 `artifact_id` 流式返回 PostgreSQL 中保存的 artifact 内容
- `AssignShard.batch_run_id` 为历史 proto 字段名，但当前承载的是任务 ID

## 任务生命周期

当前任务状态机会经过以下主阶段：

- `PROVISIONING`：任务元数据、输入工件与 shard 工件落库
- `SCALING_AGENTS`：`server` 后台 loop 正在通过 SkyPilot server 的 HTTP `/launch` 拉起 agent
- `WAITING_FOR_AGENTS`：扩容请求已发出，等待 agent 回连并进入可派发态
- `QUEUED`：control 已确认任务进入可派发队列
- `RUNNING`：至少一个 shard 已开始执行
- `SUCCEEDED / FAILED / CANCELLED`：任务终态

任务创建与派发的关键约束：

- `Task` 先入库，再进入 `PROVISIONING`
- 输入工件与 shard 工件落库完成后先进入 `SCALING_AGENTS`
- `server` 进程内 `provisioning coordinator` 异步推进 `SCALING_AGENTS -> WAITING_FOR_AGENTS`
- `submitTask` 不会同步阻塞等待 agent 扩容完成
- 当前主调用链里，agent 真正可派发并收到 `AssignShard` 前，任务不会直接跳过准备阶段

补充说明：

- 底层 helper `MarkTaskQueued()` 目前仍接受 `PROVISIONING`、`SCALING_AGENTS`、`WAITING_FOR_AGENTS` 作为可入 `QUEUED` 的前置状态
- 这是为了幂等和兼容已有调用点而保留的放宽边界，不代表主调用链鼓励绕过 `WAITING_FOR_AGENTS`

## UUID 链路现状

当前任务 ID 已经在主要边界上统一为 UUID：

- GraphQL schema 使用 `scalar UUID`
- gqlgen 生成的 `Task.id`、`SubmitTaskResponse.task_id`、`CancelTaskPayload.task_id` 都是 `uuid.UUID`
- `task_service` 的 `TaskView.ID` 与 `SubmitTask` 返回值使用 `uuid.UUID`
- 持久化层的 `model.Task.ID`、`model.Artifact.TaskID`、`model.Shard.TaskID` 都使用 `uuid.UUID`

`internal/model/base_uuid.go` 中的 `BaseUUIDModel` 提供了统一的 UUID 主键和 `BeforeCreate` 自动生成逻辑，适用于标准 `id/created_at/updated_at/deleted_at` 形态的 UUID-native model。当前 `Task` 没有嵌入它，而是继续保留自己的业务时间字段 `submitted_at/started_at/finished_at`，但任务 ID 链路已经与 `BaseUUIDModel` 使用同一套 `uuid.UUID` 语义。

## 当前残余风险

- `provisioning coordinator` 现在是 `cmd/server` 进程内 loop，并依赖 `Service.mu` 做单实例串行
- 这能避免同一进程内的并发重复扩容，但还不能解决多 `server` 实例同时扫描 `SCALING_AGENTS` 任务时的跨实例竞争
- 因此 provisioning 的跨实例竞争仍是当前残余风险，后续若演进到多实例部署，需要补上分布式 lease 或等价 fencing 机制

## 测试与构建

```bash
go test ./...
go build ./...
```

最近一次本地验证使用了显式工具路径：

- `env PATH="/usr/local/go/bin:$PATH" /usr/local/go/bin/go test ./...`
- `env PATH="/usr/local/go/bin:$PATH" /usr/local/go/bin/go build ./...`
- `/opt/homebrew/bin/helm template r-orchestrator ./chart/r-orchestrator`

## GitHub Actions Dev Workflow

The repository includes a development workflow at `.github/workflows/dev.yml`.

Current behavior:

- Triggers on `push` of release tag `vX.Y.Z`
- Uses workflow-level `concurrency` to serialize dev deployments for the same ref and cancel superseded runs
- Runs `go test ./...`
- Runs `cargo test -p agent`
- Builds and pushes two images:
  - `r-orchestrator:${GITHUB_REF_NAME}`
  - `r-agent:${GITHUB_REF_NAME}`
- Updates the `server` container in deployment `r-orchestrator` under namespace `yi-dev`

Required GitHub repository secrets:

- `ALIYUN_REGISTRY_USERNAME`
- `ALIYUN_REGISTRY_PASSWORD`
- `KUBE_CONFIG`

Required GitHub repository variables:

- `ALIYUN_REGISTRY_SG`
- `ALIYUN_REGISTRY_SG_VPC`

Notes:

- The workflow only updates the Kubernetes deployment image for the server.
- The agent image is published, but the current chart values and `skypilot-task.yaml` template are not automatically rewritten to consume that new image in this first version.

## 关键目录

- `cmd/server`：服务启动入口
- `graph`：GraphQL schema 与 resolver
- `internal/model`：持久化模型
- `internal/orm`：GORM 查询与写入
- `internal/service/task_service`：任务提交、查询、取消
- `internal/service/tenant_service`：租户后端解析与 quota 检查
- `internal/service/agent_service`：agent 注册、心跳与选择
- `internal/control`：gRPC 控制面边界
