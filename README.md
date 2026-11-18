# Buoy Data Lake Compaction Service

单进程浮标数据湖 API：按 UTC 日分区追加上传 CSV（浮标号、UTC 时刻、盐度），
每次上传落为一个 Parquet 文件；支持按日读取全部记录与当前文件数；支持按日
触发后台压实，把启动时已有的若干小文件合并为一份，降低下游扫描的小文件开销。

技术栈：FastAPI + PyArrow + SQLite（WAL），单进程设计。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 服务监听 http://localhost:8000 ，交互文档 http://localhost:8000/docs
```

所有状态（Parquet 数据 + SQLite 元数据）都在 `/data` 卷中，容器重启不丢数据。

### 本地运行

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

### 运行测试

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -q
```

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/days/{day}/upload` | 上传 CSV（multipart 文件或 `text/csv` 原始 body），追加为一个 Parquet；可选批次 ID 实现幂等重试 |
| GET | `/days/{day}/records` | 读取当日全部记录（一致快照），含 `file_count`/`row_count` |
| GET | `/days/{day}/files` | 当日活跃文件清单与文件数 |
| POST | `/days/{day}/compactions` | 启动后台压实（202）；当日已有压实进行中返回 409 |
| GET | `/days/{day}/compactions` | 当日压实任务列表 |
| GET | `/days/{day}/compactions/{job_id}` | 查询压实任务状态 |
| GET | `/health` | 健康检查 |

`day` 格式为 `YYYY-MM-DD`（UTC 日期分区）。CSV 列为 `buoy_id, ts_utc, salinity`，
表头可选（无表头时按此顺序解析）；时间戳支持 ISO-8601（`Z` 或时区偏移，
裸时间按 UTC 处理）。

### 幂等上传（批次 ID）

采集端上传后未收到响应（如网络超时）时，重试会重复写入。上传接口支持可选的
**全局批次 ID**（查询参数 `batch_id` 或请求头 `X-Batch-Id`，1–128 位
`[A-Za-z0-9._:-]` 字符）：

- **同 ID + 同 UTC 日期 + CSV 原始字节相同**：判定为重试，不再写新 Parquet，
  返回首次登记的文件名与行数（`deduplicated: true`）；
- **同 ID 但跨日期或内容不同**：返回 `409 Conflict`，不产生任何写入；
- **不同 ID**：即使内容相同也各自正常追加；
- **不带 ID**：沿用原行为，每次上传都追加。

带批次 ID 的响应示例：

```json
{"day": "2026-11-18", "file": "part-<uuid>.parquet", "rows": 2,
 "file_count": 1, "batch_id": "ship-42", "deduplicated": false}
```

### 示例

```bash
# 上传（multipart 或原始 body 均可）
curl -F "file=@obs.csv" http://localhost:8000/days/2026-11-18/upload
curl -X POST -H "Content-Type: text/csv" --data-binary @obs.csv \
     http://localhost:8000/days/2026-11-18/upload

# 幂等上传：采集端为每批观测分配全局唯一批次 ID，超时重试不会重复写入
curl -X POST -H "Content-Type: text/csv" -H "X-Batch-Id: ship-42" \
     --data-binary @obs.csv http://localhost:8000/days/2026-11-18/upload
# 或等价地使用查询参数 ?batch_id=ship-42

# 读取全部记录与文件数
curl http://localhost:8000/days/2026-11-18/records

# 启动压实并轮询状态
curl -X POST http://localhost:8000/days/2026-11-18/compactions
curl http://localhost:8000/days/2026-11-18/compactions/1
```

## 一致性与可靠性设计

- **上传原子性**：Parquet 先写 `*.tmp-*` 临时文件，`fsync` 后 `rename` 为正式
  文件并 `fsync` 目录，再在 SQLite 登记为 `active`。读者永远看不到半成品文件；
  崩溃最多留下临时碎片，启动恢复时清理。
- **幂等上传**：批次行（`upload_batches`，以批次 ID 为主键，记录日期、内容
  SHA-256、首次登记的文件名与行数）与文件登记行在**同一 SQLite 事务**提交。
  主键约束保证并发同 ID 只登记一份（后到者改返回先到者的回执）；写文件或
  登记失败时事务回滚、仅删除本次自写的临时文件，批次 ID 不会被失败占用，
  也绝不会误删已提交批次的文件。重试命中时直接读批次表返回回执，不解析、
  不写盘。回执保存在批次表而非 files 表，因此压实回收原 Parquet 后重试仍
  返回原批次回执，不会恢复旧文件或新增记录。批次登记持久化于 SQLite，
  进程重启后重试依然可识别。
- **读取一致性**：读取在同一把锁内完成「快照当日 active 文件列表 + 引用计数
  +1」，因此一次读取必然对应完整的切换前或切换后状态，不会出现新旧混杂；
  响应中的 `file_count` 与实际读取的文件集合严格一致。
- **压实切换原子性**：合并输出完整落盘（tmp+fsync+rename）后，「退役输入文件
  + 登记输出文件 + 任务置为 done」在**单个 SQLite 事务**中提交。提交前崩溃
  → 原文件全部仍在，任务重启后续跑；提交后崩溃 → 切换已生效。不存在中间态。
- **压实期间可写可读**：合并只处理任务启动时快照的输入集合；期间追加的文件
  不在集合内，切换时原样保留、全程可读，不会被遗漏或误删。上传、读取均不
  等待压实结束。
- **同日只受理一项压实**：SQLite 部分唯一索引
  `WHERE status='running'` 在数据库层面保证，并发请求第二个返回 409，
  进程重启后约束依然有效。
- **失败保留原数据**：合并或落盘失败 → 任务标记 `failed`（带错误信息），
  输入文件保持 `active`，可修复后重新发起压实。
- **中断恢复**：启动时 `recover()` 先清理退役文件、孤儿 Parquet（未登记的
  上传残留、被取代的压实输出）和 tmp 碎片，再把仍处于 `running` 的任务重新
  投入执行（其输入必然仍全部 active，重跑是安全的）。已提交的上传不受影响。
- **旧文件回收**：退役文件仅在在途读取引用计数归零后才 unlink；回收只针对
  登记表中 `retired` 的文件，`active` 文件（包括压实期间新追加的）永远不会
  被误删。另有后台周期清扫兜底。

## 目录布局

```
data/
  lake_meta.db                 # SQLite 元数据（files 登记表 + compactions 任务表 + upload_batches 批次表）
  dt=2026-11-18/
    part-<uuid>.parquet        # 上传产生的文件
    compacted-<job>-<id>.parquet  # 压实输出
```

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATA_DIR` | `./data`（容器内 `/data`） | 数据与元数据目录 |
| `LAKE_GC_INTERVAL_SEC` | `2` | 退役文件周期清扫间隔 |
| `LAKE_COMPACTION_DELAY_SEC` | `0` | 测试/演示钩子：合并前人为延迟，便于观察压实期间的并发读写 |

> 注意：一致性依赖进程内锁 + SQLite 事务，请以单进程运行（不要多 worker
> 水平扩展）。
