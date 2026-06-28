# Azure FOCUS 账单查询 API

基于 Azure Cost Management 导出的 FOCUS 账单（v1.2-preview，向 v1.3 兼容），提供对内的日账单 / 月账单分页查询 API。

- 导出与处理：按订阅独立配置（有几个订阅就有几套参数）。
- 查询统一：仅在 API 层通过 `cloud=china|global` 聚合多个订阅。
- 数据层：Parquet 全程在 Azure Blob，路径通过环境变量读取（本地 `local` 后端仅用于离线开发/测试）。

详见 [docs/design.md](docs/design.md)。

## 目录结构

```
app/
  config.py           多订阅配置（文件/JSON）+ cloud->订阅映射
  schema.py           FOCUS 1.3 canonical schema + v1.2->1.3 归一
  db.py               DuckDB 连接 + Blob 读 + 分页查询 + 分区写
  storage.py          Azure SDK 上传 curated（DuckDB 不能写 az://）
  scheduler.py        进程内每日调度（APScheduler）
  models.py           响应模型
  routers/billing.py  /daily /monthly 查询端点
  routers/admin.py    /admin 摄取/刷新/调度状态/meta
  static/index.html   管理调试 UI
ingestion/
  export_setup.py     按订阅创建 FOCUS 导出（Parquet+Snappy）
  ingest.py           Raw -> Curated 归一与分区覆盖（run_ingest）
  refresh.py          每日刷新（daily 当月 + monthly 上月）
config/subscriptions.json  订阅配置文件
scripts/gen_sample.py 生成 FOCUS 1.3 模拟数据（开发用）
tests/                端到端测试（local 后端）
```

## 快速开始（本地，模拟数据）

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env

# 用 local 后端造数据并跑通 API
export STORAGE_BACKEND=local
export LOCAL_DATA_ROOT=./data
python -m scripts.gen_sample --dataset daily   --period 2026-06 --rows 500
python -m scripts.gen_sample --dataset monthly --period 2026-06 --rows 50

uvicorn app.main:app --reload
```

打开管理调试界面：浏览器访问 `http://127.0.0.1:8000/ui/`（可手动拉取账单、设置日期/月份查询、查看分页结果、复制查询 URL/curl）。

查询示例：

```bash
curl "http://127.0.0.1:8000/api/v1/billing/daily?cloud=global&date=2026-06-15&page=1&pageSize=50"
curl "http://127.0.0.1:8000/api/v1/billing/monthly?cloud=china&month=2026-06&pageSize=100"
```

## 生产（Azure Blob）

在 `.env` 设置（鉴权推荐 Service Principal）：

```
STORAGE_BACKEND=azure_blob
BLOB_ACCOUNT_URL=https://<your-storage-account>.blob.core.windows.net
BLOB_CONTAINER=report
# curated 可与原始同账户/容器，也可分离（留空=复用原始）
CURATED_ACCOUNT_URL=
CURATED_CONTAINER=
CURATED_PREFIX=curated/focus

AZURE_STORAGE_AUTH_MODE=service_principal   # 或 managed_identity / sas / connection_string
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...                       # 或用 AZURE_CLIENT_CERTIFICATE_PATH

# 订阅配置（文件优先；内联 JSON 非空则覆盖文件）
FOCUS_SUBSCRIPTIONS_CONFIG_FILE=config/subscriptions.json
FOCUS_SUBSCRIPTIONS_CONFIG_JSON=[]
```

**权限（最小化）**：给该身份在存储账户上授 **Storage Blob Data Contributor**（读原始 + 写 curated）。若 curated 在另一账户，则两个账户都授。Azure 上运行可改用 Managed Identity（`managed_identity`，无需密钥）。China 为独立云，需单独身份与存储账户（通常单独部署）。

### 订阅配置

每个订阅一条，`cloud` 仅作查询层聚合标签。两种方式（内联 JSON 非空时覆盖文件）：

- **文件（推荐）**：`config/subscriptions.json`，与内联 JSON 同格式，运维可整段复制到 env 做容器覆盖。
- **内联**：`FOCUS_SUBSCRIPTIONS_CONFIG_JSON=[{...}]`。

```json
[
  {
    "subscriptionKey": "autotsp-global",
    "subscriptionId": "<guid>",
    "cloud": "global",
    "dailyPrefix": "focus-cost/autotsp-focus-cost-daily-parquet",
    "monthlyPrefix": "focus-cost/autotsp-focus-cost-monthly-parquet"
  }
]
```

数据流水线：

```bash
# 1) 按订阅创建导出（一次性）
python -m ingestion.export_setup --frequency daily   --focus-version 1.3
python -m ingestion.export_setup --frequency monthly --focus-version 1.3

# 2) 摄取：Raw -> Curated（按账期分区覆盖，幂等）
python -m ingestion.ingest --dataset daily   --period 2026-06
python -m ingestion.ingest --dataset monthly --period 2026-06
```

## API

| 端点 | 参数 |
|---|---|
| `GET /api/v1/billing/daily` | `cloud`(必填), `date=YYYY-MM-DD`(必填), `subscriptionId`(可选), `page`, `pageSize` |
| `GET /api/v1/billing/monthly` | `cloud`(必填), `month=YYYY-MM`(必填), `subscriptionId`(可选), `page`, `pageSize` |
| `POST /api/v1/admin/ingest` | body: `{dataset, period, subscription?}` 手动摄取 |
| `POST /api/v1/admin/refresh` | 立即跑每日刷新（daily 当月 + monthly 上月） |
| `GET /api/v1/admin/refresh/status` | 调度状态、下次/上次运行 |
| `GET /api/v1/admin/meta` | UI 用：云/订阅清单 |
| `GET /ui/` | 管理调试界面 |
| `GET /health` | - |

- 不传 `subscriptionId`：聚合该 cloud 下全部订阅。
- 传 `subscriptionId`：只查该订阅。
- 分页：`pageSize` 默认 100、上限 1000；返回 `{ data, pagination: { page, pageSize, total, totalPages } }`。

## 每日自动拉取

每天拉取「daily=当月、monthly=上一个完整月」，按账期分区幂等覆盖。两种部署方式二选一。

**方式 A：进程内调度（单实例最省事）** — 在 `.env` 开启：
```
SCHEDULER_ENABLED=true
SCHEDULER_HOUR=2      # UTC
SCHEDULER_MINUTE=30
```
API 进程启动时自动挂每日任务，状态见 `GET /api/v1/admin/refresh/status` 或 UI「1b. 每日自动拉取」。

**方式 B：外部调度（多实例/集中调度）** — `SCHEDULER_ENABLED=false`，用 cron 或 systemd 跑：
```bash
# cron：每天 02:30 UTC
30 2 * * * cd /opt/azure-focus-billing-api && .venv/bin/python -m ingestion.refresh >> /var/log/focus-refresh.log 2>&1
```
```ini
# systemd: /etc/systemd/system/focus-refresh.service
[Service]
Type=oneshot
WorkingDirectory=/opt/azure-focus-billing-api
EnvironmentFile=/opt/azure-focus-billing-api/.env
ExecStart=/opt/azure-focus-billing-api/.venv/bin/python -m ingestion.refresh

# /etc/systemd/system/focus-refresh.timer
[Timer]
OnCalendar=*-*-* 02:30:00 UTC
Persistent=true
[Install]
WantedBy=timers.target
```
```bash
systemctl enable --now focus-refresh.timer
```
手动补跑：`python -m ingestion.refresh`（或 `--now 2026-06-28` 模拟日期）。

## 测试

```bash
python -m pytest tests/ -v
```

## 备注

- FOCUS 1.3 在 Azure 暂未原生提供；`schema.py` 以 1.3 为目标列集，对 v1.2-preview 缺列做补齐；Azure 上线 1.3 后自动直通。
- Azure China（世纪互联）ARM 端点/鉴权 scope 与 Global 不同，`export_setup.py` 已按 cloud 区分。
