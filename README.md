# 设备遥测接收 API（device-telemetry-ingest）

基于 **FastAPI + SQLite** 的本地设备遥测接收服务。设备按批次上报带递增序列号的遥测记录，批次可能乱序到达；服务为每台设备维护一个**滑动窗口**，实现乱序暂存、重复幂等丢弃、超时断档标记与断档重放，并在并发请求下保持状态一致。

## 快速启动

### Docker Compose（推荐）

```bash
docker compose up --build
```

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 启动后

| 项目 | 地址 |
|---|---|
| API 基础地址 | `http://localhost:8000/api/v1` |
| 交互式文档 (Swagger) | `http://localhost:8000/docs` |
| 健康检查 | `http://localhost:8000/health` |
| 服务信息/当前配置 | `http://localhost:8000/` |
| 端口 | `8000`（Compose 映射 `8000:8000`，可在 `docker-compose.yml` 修改） |

SQLite 数据库文件默认在容器内 `/data/telemetry.db`，Compose 通过命名卷 `telemetry-data` 持久化。

## 配置项（环境变量）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `TELEMETRY_DB_PATH` | `/data/telemetry.db` | SQLite 数据库文件路径 |
| `TELEMETRY_WINDOW_SIZE` | `1000` | 每设备滑动窗口大小：接受 `[next_expected, next_expected + window_size)` 内的序列号 |
| `TELEMETRY_GAP_TIMEOUT_SECONDS` | `30` | 缺口等待时间（秒），超时标记为断档 |
| `TELEMETRY_SWEEP_INTERVAL_SECONDS` | `5` | 后台扫描间隔（秒），无流量时也会判定断档 |
| `TELEMETRY_SWEEP_ENABLED` | `true` | 是否启用后台扫描（测试时可关闭，断档判定退化为访问时惰性触发） |

## 核心语义

每台设备维护 `next_expected`（下一个期望的连续序列号），接收窗口为
`[next_expected, next_expected + window_size)`。设备首次出现时，以该批最小序列号锚定窗口起点。

| 到达序列号位置 | 处理 | 返回状态 |
|---|---|---|
| `seq == next_expected` | 落库为已提交，窗口前移，并连续冲刷已暂存的记录 | `committed` |
| 窗口内、`next_expected` 之后 | 暂存，等待前方缺口补齐 | `buffered` |
| 已存在相同序列号 | 幂等丢弃，不重复落库（批次整体重试安全） | `duplicate` |
| 窗口之下、属于已声明断档 | 作为补录数据填充断档 | `gap_filled` |
| 窗口之下、无记录、无断档、距窗口 ≤ window_size 且从未断档 | 窗口回锚（并发首批竞争导致锚点偏高时向下修正）后正常接收 | `committed`/`buffered` |
| 窗口之下、其余情况 | 过旧数据，忽略 | `stale` |
| 超出窗口上界 | 拒绝，客户端应在窗口前移后重试 | `out_of_window` |

**断档（gap）**：`next_expected` 与最小暂存序列号之间存在缺口，且持续超过
`TELEMETRY_GAP_TIMEOUT_SECONDS`，即标记断档：写入 `gaps` 表（状态 `open`），
`next_expected` 跳过缺口，暂存记录继续冲刷。断档由后台扫描器周期性判定，也会在每次请求时惰性判定。

**断档重放**：客户端查询断档 → 调用重放接口（状态转 `replaying`，返回缺失序列号区间）
→ 设备通过正常批次接口重发缺失序列号 → 逐条落为 `gap_filled` → 缺口全部补齐后断档自动转为 `recovered`。

**并发一致性**：每请求一个 SQLite 连接（WAL 模式 + `busy_timeout`），所有状态变更包裹在
`BEGIN IMMEDIATE` 写事务中，写操作在数据库层面串行化；同步端点运行在线程池中，
配合每序列号幂等，并发/重试/乱序下状态保持一致。

## API 一览

### 提交批次

```
POST /api/v1/batches
```

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "records": [
    {"seq": 1, "payload": {"temp": 21.5}},
    {"seq": 2, "payload": {"temp": 21.6}}
  ]
}
```

响应（每条记录独立返回处理结果，HTTP 恒为 200，除非请求体校验失败）：

```json
{
  "device_id": "dev-001",
  "batch_id": "optional-client-id",
  "accepted": 2,
  "results": [{"seq": 1, "status": "committed"}, {"seq": 2, "status": "buffered"}],
  "window": {"next_expected": 2, "buffered_count": 1}
}
```

### 查询窗口状态

```
GET /api/v1/devices/{device_id}/window
```

返回 `next_expected`、窗口范围、暂存序列号列表、待判定缺口（含剩余等待时间）、
已声明断档列表与统计计数（committed/buffered/gap_filled/duplicates）。

### 查询断档

```
GET /api/v1/devices/{device_id}/gaps?status=open
```

`status` 可选过滤：`open` / `replaying` / `recovered`。

### 断档重放

```
POST /api/v1/devices/{device_id}/gaps/{gap_id}/replay
```

将断档标记为 `replaying` 并返回缺失区间，例如：

```json
{
  "gap": {"gap_id": 1, "start_seq": 4, "end_seq": 6, "status": "replaying", "...": "..."},
  "missing_ranges": [[4, 6]],
  "missing_count": 3
}
```

设备随后经 `POST /api/v1/batches` 重发缺失序列号即可补齐。

### 其他

- `GET /api/v1/devices` — 设备列表
- `GET /health` — 健康检查
- `GET /` — 服务信息与当前配置

## 完整流程示例

```bash
# 1. 乱序提交：1,2,3,5,6（缺口 4）
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001",
  "records": [{"seq":1,"payload":{}},{"seq":2,"payload":{}},{"seq":3,"payload":{}},
              {"seq":5,"payload":{}},{"seq":6,"payload":{}}]
}'
# -> 1,2,3 committed；5,6 buffered

# 2. 查询窗口：pending_gap 显示缺口 4 及剩余等待时间
curl localhost:8000/api/v1/devices/dev-001/window

# 3. 超过 GAP_TIMEOUT 后，断档被标记（后台扫描或下次访问时）
curl localhost:8000/api/v1/devices/dev-001/gaps
# -> gap_id=1, start=4, end=4, status=open

# 4. 请求重放
curl -X POST localhost:8000/api/v1/devices/dev-001/gaps/1/replay
# -> status=replaying, missing_ranges=[[4,4]]

# 5. 设备重发 seq 4
curl -X POST localhost:8000/api/v1/batches -H 'Content-Type: application/json' -d '{
  "device_id": "dev-001", "records": [{"seq":4,"payload":{"temp":21.7}}]
}'
# -> status=gap_filled；断档自动 recovered
```

## 项目结构

```
app/
  config.py   # 环境变量配置
  db.py       # SQLite 连接、建表、写事务（WAL + BEGIN IMMEDIATE）
  window.py   # 滑动窗口核心引擎（提交/暂存/断档/重放/回锚）
  models.py   # 请求/响应模型
  main.py     # FastAPI 应用、路由、后台断档扫描器
tests/
  test_api.py # 乱序/幂等/断档/重放/回锚/并发一致性测试
Dockerfile
docker-compose.yml
requirements.txt / requirements-dev.txt
```

## 运行测试

```bash
pip install -r requirements-dev.txt
pytest tests/ -q
```

测试覆盖：顺序提交、乱序暂存与冲刷、重复幂等（含批内重复）、窗口外拒绝、
断档超时标记、重放恢复（含多序列号部分补齐）、窗口回锚、设备自举，
以及 8 线程并发提交 200 条记录后的状态一致性校验（真实 uvicorn 服务）。
