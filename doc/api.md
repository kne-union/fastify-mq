### 发布消息

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| topic | 消息主题 | string | (必填) |
| payload | 消息内容 | object | (必填) |
| priority | 优先级，数值越大越高 | number | 0 |
| executeAt | 定时执行时间 | string(ISO日期) | - |
| maxRetries | 最大重试次数 | number | 3 |
| traceId | 追踪ID | string | 自动生成 |
| meta | 扩展元数据 | object | - |

### 拉取消息

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| topic | 消息主题 | string | (必填) |
| limit | 拉取数量上限 | number | 10 |
| lockTimeout | 锁定超时时间(ms) | number | 30000 |

### 确认完成

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| id | 消息ID | string | (必填) |

### 标记失败

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| id | 消息ID | string | (必填) |
| error | 错误信息 | string | - |

### 消息列表

`GET /mq/message/list`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| topic | 按主题筛选 | string | - |
| status | 按状态筛选 | string | - |
| traceId | 按追踪ID筛选 | string | - |
| perPage | 每页条数 | number | 20 |
| currentPage | 当前页码 | number | 1 |

### 死信列表

`GET /mq/dlq/list`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| topic | 按主题筛选 | string | - |
| replayed | 按重放状态筛选 | boolean | - |
| perPage | 每页条数 | number | 20 |
| currentPage | 当前页码 | number | 1 |

### 重放死信(批量)

`POST /mq/dlq/replay`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| id | 死信ID(单条重放) | string | - |
| ids | 死信ID数组(批量重放) | string[] | - |

### 重放死信(单条，路径参数)

`POST /mq/dlq/replay/:id`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| id | 死信ID(路径参数) | string | (必填) |

### 查询消息轨迹(query参数)

`GET /mq/trace/detail?traceId=xxx`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| traceId | 追踪ID | string | (必填) |

### 查询消息轨迹(路径参数)

`GET /mq/trace/:traceId`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| traceId | 追踪ID(路径参数) | string | (必填) |

### 消息轨迹列表

`GET /mq/trace/list`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| topic | 按主题筛选 | string | - |
| messageId | 按消息ID筛选 | string | - |
| event | 按事件类型筛选 | string | - |
| perPage | 每页条数 | number | 20 |
| currentPage | 当前页码 | number | 1 |

### 队列深度

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| topic | 消息主题(可选) | string | - |

### 消息清理

`POST /mq/queue/cleanup`

| 属性名 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| status | 要清理的消息状态 | string | COMPLETED |
| olderThan | 清理此时间之前更新的消息 | string(ISO日期) | - |

### 锁定超时恢复

插件自动运行定时任务，将超时未确认的 `PROCESSING` 消息恢复为 `PENDING`，防止消费者崩溃导致消息永久锁定。

- 恢复间隔：`lockRecoveryInterval` 配置项（默认 30000ms）
- 超时判定：`lockTimeout` 配置项（默认 30000ms）
- 可通过 Service 直接调用：`services.queue.recoverLocked({})`

### Dashboard 数据接口

`GET /mq/dashboard`

返回 Dashboard 所需的全部数据，包含当前快照和时序数据。

| 参数 | 说明 | 类型 | 默认值 |
|-----|----|----|-----|
| window | Rate计算窗口(ms) | number | 300000 (5分钟) |
| step | 时序数据步长(ms) | number | 60000 (1分钟) |

**响应结构：**

```json
{
  "timestamp": 1747000000000,
  "current": {
    "queueDepth": { "byTopic": { "order.created": 5 }, "total": 5 },
    "consumedTotal": { "byTopic": { "order.created": 100 }, "total": 100 },
    "failedTotal": { "byTopic": { "order.created": 3 }, "total": 3 },
    "dlqTotal": { "byTopic": { "order.created": 1 }, "total": 1 },
    "consumeRate": { "byTopic": { "order.created": 0.5 }, "total": 0.5 },
    "failureRate": { "byTopic": { "order.created": 0.02 }, "total": 0.02 },
    "dlqRate": { "byTopic": { "order.created": 0.005 }, "total": 0.005 },
    "successRatio": 0.96,
    "successRatioByTopic": { "order.created": 0.96 }
  },
  "timeSeries": {
    "queueDepth": [{ "timestamp": 1747000000000, "order.created": 5 }],
    "consumeRate": [{ "timestamp": 1747000000000, "order.created": 0.5 }],
    "failureRate": [{ "timestamp": 1747000000000, "order.created": 0.02 }],
    "dlqRate": [{ "timestamp": 1747000000000, "order.created": 0.005 }]
  }
}
```

**数据说明：**

| 字段 | 说明 |
|-----|-----|
| current.queueDepth | 当前各主题队列积压数 |
| current.consumedTotal | 累计消费成功数 |
| current.failedTotal | 累计失败数 |
| current.dlqTotal | 累计进入死信数 |
| current.consumeRate | 消费速率(msg/s)，基于window窗口计算 |
| current.failureRate | 失败速率(msg/s) |
| current.dlqRate | 死信速率(msg/s) |
| current.successRatio | 整体成功率(0~1)，无数据时为null |
| current.successRatioByTopic | 各主题成功率 |
| timeSeries.queueDepth | 队列深度时序数据 |
| timeSeries.consumeRate | 消费速率时序数据 |
| timeSeries.failureRate | 失败速率时序数据 |
| timeSeries.dlqRate | 死信速率时序数据 |

**Dashboard面板与字段映射：**

| Dashboard面板 | 使用字段 |
|-----|-----|
| Queue Depth | timeSeries.queueDepth / current.queueDepth |
| Consume Rate | timeSeries.consumeRate / current.consumeRate |
| Failure Rate | timeSeries.failureRate / current.failureRate |
| DLQ Rate | timeSeries.dlqRate / current.dlqRate |
| Success Ratio | current.successRatio |
| Success Ratio by Topic | current.successRatioByTopic |
| Total Consumed/Failed/DLQ | current.consumedTotal/failedTotal/dlqTotal 的 total |
| Consumed vs Failed | timeSeries.consumeRate + failureRate + dlqRate |

### Prometheus Metrics

`GET /mq/metrics`

输出标准 Prometheus 文本格式，包含以下指标：

| 指标名 | 类型 | 说明 |
|-----|----|-----|
| mq_queue_depth | Gauge | 当前队列积压深度(按topic) |
| mq_consumed_total | Counter | 成功消费消息总数(按topic) |
| mq_failed_total | Counter | 失败消息总数(按topic) |
| mq_dlq_total | Counter | 进入死信队列消息总数(按topic) |

### Grafana Dashboard

提供 `grafana/mq-dashboard.json` 可直接导入 Grafana，包含以下面板：

- **Queue Depth** — 各主题队列积压趋势
- **Consume Rate** — 消费速率 (msg/s)
- **Failure Rate** — 失败速率 (msg/s)
- **DLQ Rate** — 死信进入速率 (msg/s)
- **Success Ratio** — 整体/各主题成功率
- **Consumed vs Failed Comparison** — 成功与失败对比堆叠图
