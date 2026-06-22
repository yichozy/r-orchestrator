# GraphQL API 示例

服务地址：`http://localhost:8089/graphql`

## 1. 创建租户

```bash
curl -X POST http://localhost:8089/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { CreateTenant(input: { name: \"team-alpha\", primary_backend_name: \"kubernetes\", max_agents: 2 }) { id name } }"}'
```

返回示例：
```json
{
  "data": {
    "CreateTenant": {
      "id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
      "name": "team-alpha"
    }
  }
}
```

## 2. 提交任务

使用 `test-data/bundle.zip` 作为 R 脚本包（包含 `install.sh` + `cmd/*.sh` 脚本 + R 代码）。

```bash
curl -X POST http://localhost:8089/graphql \
  -F 'operations={"query":"mutation ($input: SubmitTaskInput!) { SubmitTask(input: $input) { task_id } }","variables":{"input":{"tenant_name":"team-alpha","bundle_zip":null,"completion_hook_url":"http://127.0.0.1:18080/hook"}}}' \
  -F 'map={"0":["variables.input.bundle_zip"]}' \
  -F '0=@test-data/bundle.zip;type=application/zip'
```

返回示例：
```json
{
  "data": {
    "SubmitTask": {
      "task_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    }
  }
}
```

## 3. 查询任务

将上一步返回的 `task_id` 替换到下面的查询中。

### 按任务 ID 查询

```bash
curl -X POST http://localhost:8089/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ GetTaskByID(tenant_name: \"team-alpha\", task_id: \"TASK_ID_HERE\") { id tenant_name status last_error shard_count created_at started_at finished_at scripts { script_name status output_oss_key output_sha256 error_message started_at finished_at } } }"}'
```

### 查询任务列表

```bash
# 查询全部
curl -X POST http://localhost:8089/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ GetTaskList(tenant_name: \"team-alpha\") { id status shard_count created_at finished_at } }"}'

# 按状态过滤
curl -X POST http://localhost:8089/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"{ GetTaskList(tenant_name: \"team-alpha\", status: \"RUNNING\") { id status shard_count } }"}'
```

## 4. 取消任务

```bash
curl -X POST http://localhost:8089/graphql \
  -H "Content-Type: application/json" \
  -d '{"query":"mutation { CancelTask(tenant_name: \"team-alpha\", task_id: \"TASK_ID_HERE\") { task_id status } }"}'
```
