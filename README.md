# Azure FOCUS 账单查询 API

基于 Azure Cost Management 导出的 FOCUS 账单（v1.2-preview，向 v1.3 兼容），提供对内的日账单 / 月账单分页查询 API。

- **单部署单云**：一套代码，一次部署对接 **China 或 Global 之一**，只需一套存储 + Service Principal 配置。代码会按 `BLOB_ACCOUNT_URL` 后缀自动识别主权云（`*.chinacloudapi.cn` → Azure China，否则 Global），自动切换 AAD 授权终结点与存储终结点，无需手工指定云。
- 导出与处理：按订阅独立配置（同一云下有几个订阅就有几套参数）。
- 查询聚合：API 层通过 `cloud=china|global` 聚合该云下的多个订阅（单云部署时即为本部署对应的云）。
- 数据层：Parquet 全程在 Azure Blob，路径通过环境变量读取。

> 若同时需要 China 与 Global，分别独立部署两套（各自一套存储 + SP 配置），互不混用。

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
  export_setup.py     按订阅创建 FOCUS 导出（Parquet，不压缩）
  ingest.py           Raw -> Curated 归一与分区覆盖（run_ingest）
  refresh.py          每日刷新（daily 当月 + monthly 上月）
config/subscriptions_*.json  订阅配置文件（china / global 各一份）
tests/                端到端测试
```

## 环境准备

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # 然后按下面「配置」填写 .env
```

## 配置（.env）

一套部署对接单一云（China 或 Global），只配置一套存储 + Service Principal。云类型由 `BLOB_ACCOUNT_URL` 后缀自动识别，无需额外开关。

```
# ---- 存储 ----
STORAGE_BACKEND=azure_blob
# Global：https://<acct>.blob.core.windows.net
# China ：https://<acct>.blob.core.chinacloudapi.cn （自动切到 login.chinacloudapi.cn 授权终结点）
BLOB_ACCOUNT_URL=https://<your-storage-account>.blob.core.windows.net
BLOB_CONTAINER=report
# curated（规整层）可与原始同账户/容器，也可分离（留空=复用原始）
CURATED_ACCOUNT_URL=
CURATED_CONTAINER=
CURATED_PREFIX=curated/focus

# ---- 鉴权 ----
AZURE_STORAGE_AUTH_MODE=service_principal   # service_principal | managed_identity | sas | connection_string
AZURE_TENANT_ID=...
AZURE_CLIENT_ID=...
AZURE_CLIENT_SECRET=...                     # 或改用 AZURE_CLIENT_CERTIFICATE_PATH
# managed_identity 无需上面三项；sas / connection_string 用下列之一
# AZURE_STORAGE_SAS_TOKEN=...
# AZURE_STORAGE_CONNECTION_STRING=...

# ---- 订阅（仅本云）----
# 文件优先；内联 JSON 非空则覆盖文件。China 用 config/subscriptions_china.json，Global 用 config/subscriptions_global.json。
FOCUS_SUBSCRIPTIONS_CONFIG_FILE=config/subscriptions.json
FOCUS_SUBSCRIPTIONS_CONFIG_JSON=[]

# ---- 每日调度（默认进程内调度，见「每日自动拉取」）----
SCHEDULER_ENABLED=true
```

参数说明：

| 变量 | 说明 |
|---|---|
| `BLOB_ACCOUNT_URL` | 原始导出所在存储账户；后缀决定主权云（`.chinacloudapi.cn`=China，否则 Global） |
| `BLOB_CONTAINER` | 原始导出所在容器 |
| `CURATED_ACCOUNT_URL` / `CURATED_CONTAINER` | 规整层存储；留空则复用原始账户/容器 |
| `CURATED_PREFIX` | 规整层路径前缀 |
| `AZURE_STORAGE_AUTH_MODE` | 鉴权方式，四选一 |
| `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` | `service_principal` 必填（或用 `AZURE_CLIENT_CERTIFICATE_PATH` 证书） |
| `FOCUS_SUBSCRIPTIONS_CONFIG_FILE` | 本云订阅清单文件路径 |

**权限（最小化）**：给该身份在存储账户上授 **Storage Blob Data Contributor**（读原始 + 写 curated）。若 curated 在另一账户，则两个账户都授。Azure 上运行可改用 Managed Identity（`managed_identity`，无需密钥）。

> **China 部署**：使用 Azure China（世纪互联）租户下的 Service Principal 与存储账户，`BLOB_ACCOUNT_URL` 填 `*.blob.core.chinacloudapi.cn` 即可；代码自动使用中国云的 AAD 授权终结点（`login.chinacloudapi.cn`）与存储终结点。China 与 Global 各自独立部署，配置互不共用。

### 订阅配置文件

每个订阅一条，`cloud` 取本部署对应的云。两种方式（内联 JSON 非空时覆盖文件）：

- **文件（推荐）**：如 `config/subscriptions.json`，运维可整段复制到 env 做容器覆盖。
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

## 运行

> 账单导出任务已在 Azure Portal 配置完成，第 1 步可跳过；直接从摄取开始。若日后需用脚本创建/更新导出，再执行 `export_setup`。

```bash
# 1) （可选，已在 Portal 完成）按订阅创建 Azure 导出（Parquet 不压缩）
# python -m ingestion.export_setup --frequency daily   --focus-version 1.3
# python -m ingestion.export_setup --frequency monthly --focus-version 1.3

# 2) 摄取：Raw -> Curated（按账期分区覆盖，幂等）
python -m ingestion.ingest --dataset daily   --period 2026-06
python -m ingestion.ingest --dataset monthly --period 2026-06

# 3) 启动 API
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

- 管理调试界面：浏览器访问 `http://127.0.0.1:8000/ui/`（手动拉取账单、按日期/月份查询、查看分页结果、复制查询 URL/curl）。
- 查询示例（`cloud` 取本部署对应的云）：

```bash
curl "http://127.0.0.1:8000/api/v1/billing/daily?cloud=global&date=2026-06-15&page=1&pageSize=50"
curl "http://127.0.0.1:8000/api/v1/billing/monthly?cloud=global&month=2026-06&pageSize=100"
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

每天拉取「daily=当月、monthly=上一个完整月」，按账期分区幂等覆盖。默认采用**进程内调度**（`SCHEDULER_ENABLED=true`）。

**方式 A：进程内调度（默认，单实例最省事）** — `.env` 默认已开启，可调时间：
```
SCHEDULER_ENABLED=true
SCHEDULER_HOUR=2      # UTC
SCHEDULER_MINUTE=30
```
API 进程启动时自动挂每日任务，状态见 `GET /api/v1/admin/refresh/status` 或 UI「1b. 每日自动拉取」。

**方式 B：外部调度（多实例/集中调度）** — 多实例部署时将 `SCHEDULER_ENABLED=false` 避免重复拉取，改用 cron 或 systemd 跑：
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
- 主权云自适应：代码按 `BLOB_ACCOUNT_URL` 后缀自动选择 AAD 授权终结点与存储终结点（`db.py` 的 DuckDB secret、`storage.py` 的 Azure SDK 凭据均已处理），`export_setup.py` 的 ARM 端点/鉴权 scope 也按 cloud 区分。一次部署只对接一个云。
