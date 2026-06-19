# Lightweight Agent Provisioning Design

Date: 2026-06-16

## Problem

r-orchestrator 使用 SkyPilot 创建 agent pod/VM，但 SkyPilot 为 Ray/Python 设计：每个 agent 启动需要安装 Ray + Python 运行时（K8S 上 180+ 秒），通过 Ray driver 调度任务执行。对预编译 Rust binary 完全不必要。

## Design

### 去掉 SkyPilot，多云 Backend 直连

r-orchestrator 通过各云原生 API 直接创建运行 agent 镜像的容器/VM。所有云共享同一个 `Backend` 接口。

### 架构

```
provisioning_service.Service (RunOnce 轮询)
  → tenant 的 PrimaryBackendName 选择 Backend
  → Backend.ScaleUp(ctx, tenantID, numAgents)  // 创建/扩容 agent 资源
  → Backend.ScaleDown(ctx, tenantID)             // 销毁 agent 资源
  → Task: SCALING_AGENTS → WAITING_FOR_AGENTS

Backend 接口:
  k8s     → client-go   → 创建 Deployment
  runpod   → RunPod API  → 创建 Pod
  lambdalabs → LambdaLabs API → 创建 Instance
```

### Backend 接口

```go
// internal/service/provisioning_service/backend.go

type Backend interface {
    // ScaleUp 确保指定数量的 agent 副本在运行
    ScaleUp(ctx context.Context, tenantID string, numAgents int) error

    // ScaleDown 移除所有 agent 资源
    ScaleDown(ctx context.Context, tenantID string) error
}
```

### K8s Backend

用 `k8s.io/client-go` 直接创建 Deployment。

- 创建 Deployment：`r-agent-{tenant_id}`
- Replicas = numAgents
- Container：统一 agent 镜像 + RORCHESTRATOR_* env vars
- livenessProbe / readinessProbe：TCP port 9091
- imagePullPolicy: IfNotPresent

```go
// internal/service/provisioning_service/k8s_backend.go
type K8sBackend struct {
    clientset  *kubernetes.Clientset
    namespace  string
    agentImage string
    healthPort  int
    grpcAddr    string
    agentToken  string
    backendName string
}
```

**ScaleUp 逻辑：**
1. 检查 Deployment 是否存在
2. 不存在 → create
3. 已存在 → update replicas（scale up/down）

### RunPod Backend

用 RunPod REST API 创建 Pod。

- API endpoint: `https://api.runpod.ai/v2/{apikey}`
- 创建 Pod：指定 image + env vars + ports
- Agent 启动后通过 env vars 里的 gRPC addr 连回 server

```go
// internal/service/provisioning_service/runpod_backend.go
type RunPodBackend struct {
    httpClient  *http.Client
    apiKey      string
    cloudRegion string
    agentImage  string
    healthPort  int
    grpcAddr    string
    agentToken  string
    tenantID    string
}
```

**ScaleUp 逻辑：**
1. POST `/v2/pods` 创建 Pod
2. 等待 Pod 状态变为 Running

### LambdaLabs Backend

用 LambdaLabs REST API 创建 Instance。

- API endpoint: `https://api.lambdalabs.com/v2/instance-requests`
- 创建 Instance：指定 container image + env vars
- 支持裸 Docker 容器

```go
// internal/service/provisioning_service/lambdalabs_backend.go
type LambdaLabsBackend struct {
    httpClient  *http.Client
    apiKey      string
    agentImage  string
    healthPort  int
    grpcAddr    string
    agentToken  string
    tenantID    string
}
```

**ScaleUp 逻辑：**
1. POST `/v2/instance-requests` 创建实例
2. 轮询 instance status 直到 Running
3. Instance 通过 Docker image 运行 agent binary

### 配置

```go
// internal/config/config.go
type ProvisioningConfig struct {
    Enabled  bool
    Backends map[string]BackendConfig
}

type BackendConfig struct {
    Type     string // "kubernetes" | "runpod" | "lambdalabs"
    // K8s
    K8sNamespace string
    // RunPod
    RunPodAPIKey   string
    RunPodCloudRegion string
    // LambdaLabs
    LambdaAPIKey string
    // 通用
    AgentImage       string   // 统一 agent 镜像
    HealthPort       int      // default: 9091
    ServerGRPCAddr   string
    AgentToken       string
}
```

Tenant 的 `PrimaryBackendName` 选择使用哪个 backend。

### 数据流

```
Task 提交 → SCALING_AGENTS
  → ProvisioningService.RunOnce
    → 查找 tenant 的 PrimaryBackendName
    → Backend.ScaleUp(tenantID, maxAgents)
      → 调用云 API 创建 agent 资源
    → Task → WAITING_FOR_AGENTS

Agent 启动（几秒 ~ 几十秒）
  → /usr/local/bin/r-agent 从 env vars 读取配置
  → health server :9091
  → gRPC connect → Register → IDLE → receive shards → execute
```

### 改动范围

**新增文件：**
- `internal/service/provisioning_service/backend.go` — Backend 接口
- `internal/service/provisioning_service/k8s_backend.go` — K8s 直连
- `internal/service/provisioning_service/runpod_backend.go` — RunPod 直连
- `internal/service/provisioning_service/lambdalabs_backend.go` — LambdaLabs 直连

**修改文件：**
- `internal/service/provisioning_service/service.go` — 用 Backend 接口替代 SkyPilot client
- `internal/config/config.go` — 新增多云 Backend 配置
- `cmd/server/main.go` — 初始化各 Backend

**删除文件（后续清理）：**
- `internal/service/provisioning_service/skypilot_client.go`
- `skypilot-task.yaml`

**新增依赖：**
- `k8s.io/client-go` — K8s Go client
- RunPod / LambdaLabs: 标准 `net/http` 即可，无需 SDK

### 启动时间对比

| 云 | SkyPilot 方案 | 直连方案 |
|---|---|---|
| Kubernetes | 180+ 秒（apt/conda/ray） | ~2 秒 |
| RunPod | 60+ 秒（SSH + Ray 安装） | ~30 秒（拉取镜像 + 启动） |
| LambdaLabs | 60+ 秒（SSH + Ray 安装） | ~30 秒（拉取镜像 + 启动） |
