# Azure FOCUS 账单查询 API 方案（按最新约束）

## 1. 目标与范围

- 数据来源：Azure Cost Management 导出的 FOCUS 账单（当前版本为 v1.2-preview）。
- 目标能力：提供对内查询 API，支持
  - 日账单查询
  - 月账单查询
  - 按日期或月份过滤
  - 大数据量分页查询
- 组织方式：china 和 global 各包含多订阅，导出与处理完全按订阅独立执行。
- 查询视图：仅在 API 查询层通过 cloud=china|global 做统一聚合。

## 2. 关键约束（本次确认）

- Parquet 数据层不落本地，开发期和部署期都放在 Azure Blob。
- Blob 账户地址通过环境变量读取。
- 导出与处理按订阅独立配置：有几个订阅就有几套配置参数。
- Blob 鉴权方式：托管身份（Managed Identity）。
- 已核实的真实导出布局（单个 global 订阅，币种 USD）：
  - `focus-cost/autotsp-focus-cost-daily-parquet/<YYYYMMDD-YYYYMMDD>/<RunID>/manifest.json + part_*.parquet`
  - `focus-cost/autotsp-focus-cost-monthly-parquet/<YYYYMMDD-YYYYMMDD>/<RunID>/...`
  - 该 export 路径对应单个订阅，没有按订阅再分子目录；开启 Overwrite，每月文件夹仅一个 RunID。
- China 订阅属于 Azure China（世纪互联）独立云：使用独立 storage account / ARM 端点，配置为单独的订阅条目（或单独部署），不与 global 混在同一路径。

## 3. 存储与路径设计

### 3.1 源数据（Raw，按订阅独立）

- Blob Account: `https://<your-storage-account>.blob.core.windows.net/`
- Container: `report`
- Source Prefix（每个订阅一套）：
  - `focus-cost/<subscription-key>/autotsp-focus-cost-daily-parquet/`
  - `focus-cost/<subscription-key>/autotsp-focus-cost-monthly-parquet/`

其中 `<subscription-key>` 是系统内定义的订阅代号（例如 `global-prod-01`、`china-fin-02`），用于将每个订阅的导出路径和处理配置解耦。

说明：源数据由 Export 任务产生，包含分区文件和 manifest（若该导出形态提供 manifest）。

### 3.2 规整数据（Curated，仍在 Blob）

为避免直接依赖 export run 的目录结构、并支撑 schema 演进，新增一层规整路径（仍在同一 Blob container）：

- Curated Prefix（建议）：`curated/focus/`
- 分区建议：`dataset=daily|monthly/cloud=<china|global>/subscription=<subscription-key>/period=YYYY-MM/`

示例：

- `curated/focus/dataset=daily/cloud=global/subscription=global-prod-01/period=2026-06/part-000.parquet`
- `curated/focus/dataset=monthly/cloud=china/subscription=china-fin-02/period=2026-06/part-000.parquet`

说明：

- 规整层继续使用 Parquet（可保持 snappy）。
- 查询 API 默认读 Curated；Raw 仅作为摄取输入。
- 不要求本地持久化数据文件。

## 4. 架构与数据流

1. Export（已存在）按订阅输出 daily/monthly FOCUS Parquet 到各自 Raw 路径。
2. Ingestion 作业按订阅读取 Raw 分区文件，按统一 schema 做清洗与字段补齐。
3. 写回 Curated Blob 分区路径（按 dataset + subscription + period 覆盖写，保持幂等）。
4. API 服务使用 DuckDB 查询 Blob 上 Curated Parquet；查询层按 cloud 参数汇总对应订阅集合并返回分页结果。

## 5. FOCUS 版本策略（v1.2-preview -> v1.3）

现状：Azure 导出 dataVersion=`1.2-preview`（已通过真实 parquet 的 manifest 核实）。目标：对外 API 统一按 v1.3 字段语义。

### 5.1 官方字段对比（严格依据 FOCUS Column Library，非推测）

- 来源：
  - v1.2：https://focus.finops.org/focus-columns/?version=v1-2 （57 列）
  - v1.3：https://focus.finops.org/focus-columns/?version=v1-3&dataset=cost-and-usage （65 列）
- 结论：v1.3 == v1.2 + 8 个新增列，无重命名、无删除。新增列：

| 分类 | 新增列 |
|---|---|
| Allocation（新分类） | AllocatedMethodDetails, AllocatedMethodId, AllocatedResourceId, AllocatedResourceName, AllocatedTags |
| Charge Origination | HostProviderName, ServiceProviderName |
| Contract（新分类） | ContractApplied |

### 5.2 真实 Azure 1.2-preview parquet 实况

- 实际只含 53 个 FOCUS 标准列（比官方 v1.2 的 57 少 4 个）：缺 `AvailabilityZone`、`PricingCurrencyContractedUnitPrice`、`PricingCurrencyEffectiveCost`、`PricingCurrencyListUnitPrice`。
- 额外含 52 个 Azure 专有 `x_` 扩展列（如 `x_BilledCostInUsd`、`x_BillingExchangeRate`、`x_ResourceGroupName`）。

### 5.3 归一策略

- canonical schema 以 v1.3（65 列）为目标列集。
- 缺失列（4 个 Azure 未给 + 8 个 v1.3 新增 = 共 12 个）置 NULL，不做数值猜测。
- `x_` 扩展列原样透传保留（携带资源组、USD 折算、汇率等有价值信息）。
- Azure 将来原生提供 1.3 后：新增 8 列命中即直通，无需改 API 契约。
- 已用真实 parquet 验证：105 源列 -> 117 输出列（65 canonical + 52 x_），行数 12668 保持不变。

## 6. 查询 API 设计

### 6.1 接口

- `GET /api/v1/billing/daily`
  - 参数：`cloud`、`date`、`subscriptionId`(可选)、`page`、`pageSize`
- `GET /api/v1/billing/monthly`
  - 参数：`cloud`、`month`、`subscriptionId`(可选)、`page`、`pageSize`

说明：

- `cloud` 是查询层聚合维度，不代表底层只有两套导出配置。
- 不传 `subscriptionId` 时，接口会查询 cloud 对应的全部订阅并聚合返回。
- 传 `subscriptionId` 时，仅查询该订阅数据。

### 6.2 过滤规则

- daily：按账单日期过滤（date=YYYY-MM-DD）。
- monthly：按账期月份过滤（month=YYYY-MM）。
- cloud：取值 `china|global`，在查询层映射到订阅列表后做分区裁剪。
- subscriptionId：可选精确过滤。

### 6.3 分页规则

- 默认：`page=1`，`pageSize=100`。
- 上限：`pageSize<=1000`（防止单次拉取过大）。
- 返回：`data + pagination`，其中 `pagination` 包含
  - `page`
  - `pageSize`
  - `total`
  - `totalPages`

## 7. 环境变量设计（建议，支持多订阅）

- `BLOB_ACCOUNT_URL=https://<your-storage-account>.blob.core.windows.net`
- `BLOB_CONTAINER=report`
- `CURATED_PREFIX=curated/focus`
- `AZURE_STORAGE_AUTH_MODE=managed_identity|sas|connection_string`
- `FOCUS_SUBSCRIPTIONS_CONFIG_JSON`：订阅配置清单（JSON 字符串）

示例：

```json
[
  {
    "subscriptionKey": "global-prod-01",
    "subscriptionId": "<guid>",
    "cloud": "global",
    "dailyPrefix": "focus-cost/global-prod-01/autotsp-focus-cost-daily-parquet",
    "monthlyPrefix": "focus-cost/global-prod-01/autotsp-focus-cost-monthly-parquet"
  },
  {
    "subscriptionKey": "china-fin-02",
    "subscriptionId": "<guid>",
    "cloud": "china",
    "dailyPrefix": "focus-cost/china-fin-02/autotsp-focus-cost-daily-parquet",
    "monthlyPrefix": "focus-cost/china-fin-02/autotsp-focus-cost-monthly-parquet"
  }
]
```

可选（按认证方式）：

- `AZURE_STORAGE_CONNECTION_STRING`
- `AZURE_STORAGE_SAS_TOKEN`

## 8. DuckDB 访问 Blob 方案

- 使用 DuckDB 读取 Blob 上 Parquet（通过 Azure Storage 凭据配置）。
- 查询侧仅拉取所需列和分区，减少网络与计算开销。
- 建议在 SQL 中显式分区过滤：`dataset/cloud/subscription/period`，避免全量扫描。

## 9. 作业与幂等

- 调度：每日固定窗口执行摄取（可补跑）。
- 幂等：按 `dataset + subscription + period` 分区覆盖写。
- 质量校验：记录行数、文件数、金额汇总校验值。
- 失败恢复：分区级重试，不影响其他账期分区。

## 10. 后续开发落地建议

1. 先实现多订阅配置读取与 Blob 连接层（环境变量驱动）。
2. 实现 ingestion（Raw -> Curated）与 v1.3 补齐规则。
3. 实现 daily/monthly API 与分页，并在查询层增加 cloud -> subscriptions 映射。
4. 加入最小可用校验：连通性、分区存在性、分页边界测试。
