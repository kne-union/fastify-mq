# fastify-mq

### 描述

Reliable message queue plugin for Fastify based on PostgreSQL, supporting at-least-once delivery, DLQ, delayed messages, priority, tracing and Kafka migration

### 安装

```shell
npm i --save @kne/fastify-mq
```

### 概述

# Fastify + PostgreSQL 可靠消息队列插件

基于 PostgreSQL 的可靠消息队列 Fastify 插件，支持至少一次投递、并发消费、死信队列、延迟消息、优先级排序、消息轨迹追踪等功能，可通过配置切换到 Kafka。

## 特性

- **消息持久化** - 基于 PostgreSQL，消息不丢失
- **并发消费** - 支持 `SELECT ... FOR UPDATE` 锁定，多实例安全
- **幂等处理** - 通过 traceId 追踪消息生命周期
- **死信队列 (DLQ)** - 超过重试次数自动进入死信，支持单条/批量重放
- **延迟/定时消息** - 支持 executeAt 延迟投递
- **优先级** - 数值越大优先级越高
- **消息轨迹** - 完整的 PUBLISHED → PROCESSING → COMPLETED/FAILED 轨迹链
- **指数退避重试** - 可配置的重试策略
- **锁定超时恢复** - 消费者崩溃后自动恢复超时锁定消息
- **消息清理** - 支持清理已完成的旧消息，防止数据膨胀
- **Kafka 迁移** - 通过 adapter 抽象，业务代码无感知切换

## 安装

```bash
npm install @kne/fastify-mq
```

## 快速开始

```javascript
const fastify = require('fastify')();

await fastify.register(require('@kne/fastify-sequelize'), {
  db: { dialect: 'sqlite', storage: './mq.sqlite' }
});

await fastify.register(require('@kne/fastify-mq'), {
  name: 'mq',
  prefix: '/mq'
});

await fastify.ready();
await fastify.sequelize.sync();

// 发布消息
await fastify.mq.services.message.publish({}, {
  topic: 'order.created',
  payload: { orderId: '123' }
});

// 订阅消费
fastify.mq.services.queue.subscribe('order.created', async (msg) => {
  console.log('Processing:', msg.payload);
});

// 启动消费者
fastify.mq.services.queue.startConsumer();
```

## 配置选项

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `name` | string | `'mq'` | 命名空间标识 |
| `prefix` | string | `'/mq'` | 路由前缀 |
| `dbTableNamePrefix` | string | `'t_'` | 数据库表前缀 |
| `defaultMaxRetries` | number | `3` | 默认最大重试次数 |
| `pollLimit` | number | `10` | 每次拉取消息数量上限 |
| `pollInterval` | number | `5000` | 消费者轮询间隔(ms) |
| `lockTimeout` | number | `30000` | 消息锁定超时(ms)，超时自动恢复 |
| `lockRecoveryInterval` | number | `30000` | 锁定恢复检查间隔(ms) |
| `retryBaseDelay` | number | `1000` | 重试基础延迟(ms) |
| `retryMaxDelay` | number | `60000` | 重试最大延迟(ms) |
| `metricsSampleInterval` | number | `10000` | 指标采样间隔(ms) |
| `metricsMaxSamples` | number | `360` | 最大采样数 |
| `getAuthenticate` | function | `() => []` | 按功能分类返回认证中间件 |
| `getMessageModel` | function | 自动获取 | 注入消息模型 |

## API 接口

### 消息操作

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/mq/message/publish` | 发布消息 |
| GET | `/mq/message/poll` | 拉取消息 |
| POST | `/mq/message/complete` | 确认完成 |
| POST | `/mq/message/fail` | 标记失败 |
| GET | `/mq/message/list` | 消息列表（支持 topic/status/traceId 筛选） |

### 死信队列

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/mq/dlq/list` | 死信列表（支持 topic/replayed 筛选） |
| POST | `/mq/dlq/replay` | 重放死信(单条/批量) |
| POST | `/mq/dlq/replay/:id` | 重放死信(路径参数) |

### 轨迹查询

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/mq/trace/detail` | 查询消息轨迹(query参数) |
| GET | `/mq/trace/:traceId` | 查询消息轨迹(路径参数) |
| GET | `/mq/trace/list` | 轨迹列表（支持 topic/messageId/event 筛选） |

### 队列监控

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/mq/queue/depth` | 队列积压深度 |
| POST | `/mq/queue/cleanup` | 清理已完成的消息 |
| GET | `/mq/dashboard` | Dashboard 数据接口 |
| GET | `/mq/metrics` | Prometheus Metrics |

## 数据库表

### 消息表 (mq_messages)

| 字段 | 类型 | 说明 |
|------|------|------|
| topic | TEXT | 消息主题 |
| payload | JSONB | 消息内容 |
| status | TEXT | 状态: PENDING/PROCESSING/COMPLETED/FAILED |
| retryCount | INT | 已重试次数 |
| maxRetries | INT | 最大重试次数 |
| priority | INT | 优先级 |
| executeAt | TIMESTAMP | 定时执行时间 |
| nextRetryAt | TIMESTAMP | 下次重试时间 |
| consumerId | TEXT | 消费者标识 |
| lockedAt | TIMESTAMP | 锁定时间 |
| traceId | UUID | 追踪ID |

### 死信表 (mq_dead_letters)

| 字段 | 类型 | 说明 |
|------|------|------|
| originalId | BIGINT | 原始消息ID |
| topic | TEXT | 消息主题 |
| payload | JSONB | 消息内容 |
| errorMessage | TEXT | 错误信息 |
| replayed | BOOLEAN | 是否已重放 |
| replayedAt | TIMESTAMP | 重放时间 |

### 轨迹表 (mq_message_traces)

| 字段 | 类型 | 说明 |
|------|------|------|
| traceId | UUID | 追踪ID |
| messageId | BIGINT | 消息ID |
| topic | TEXT | 消息主题 |
| event | TEXT | 事件类型 |
| detail | JSONB | 事件详情 |

## 使用示例

### 发布消息

```javascript
// 普通消息
await fastify.mq.services.message.publish({}, {
  topic: 'order.created',
  payload: { orderId: '123', amount: 99.9 }
});

// 延迟消息
await fastify.mq.services.message.publish({}, {
  topic: 'order.timeout',
  payload: { orderId: '123' },
  executeAt: new Date(Date.now() + 30 * 60 * 1000) // 30分钟后
});

// 高优先级消息
await fastify.mq.services.message.publish({}, {
  topic: 'payment.completed',
  payload: { paymentId: '456' },
  priority: 10
});
```

### 订阅消费

```javascript
fastify.mq.services.queue.subscribe('order.created', async (msg) => {
  await processOrder(msg.payload);
});

fastify.mq.services.queue.startConsumer();
```

### 死信重放

```javascript
// 单条重放
await fastify.mq.services.deadLetter.replay({}, { id: 'dead-letter-id' });

// 批量重放
await fastify.mq.services.deadLetter.batchReplay({}, { ids: ['id1', 'id2', 'id3'] });
```

### 消息轨迹

```javascript
const traces = await fastify.mq.services.trace.get({}, { traceId: 'trace-uuid' });
// 返回: [{event: 'PUBLISHED'}, {event: 'PROCESSING'}, {event: 'COMPLETED'}]
```

### 锁定超时恢复

```javascript
// 自动运行：消费者崩溃后，锁定超时的消息会自动恢复为 PENDING
// 也可手动触发：
const result = await fastify.mq.services.queue.recoverLocked({});
console.log(`Recovered ${result.recovered} timed-out messages`);
```

### 消息清理

```javascript
// 清理已完成的消息
const result = await fastify.mq.services.queue.cleanup({}, {
  status: 'COMPLETED',
  olderThan: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000) // 7天前
});
console.log(`Deleted ${result.deleted} old messages`);
```

## 权限控制

> **安全警告：** 默认配置下所有接口无需认证即可访问，生产环境务必配置 `getAuthenticate`。

```javascript
await fastify.register(require('@kne/fastify-mq'), {
  getAuthenticate: (type) => {
    const { authenticate } = fastify.account;
    switch (type) {
      case 'dlq:manage':
        return [authenticate.user, authenticate.admin];
      case 'message':
      case 'dlq':
      case 'trace':
      case 'dashboard':
      default:
        return [authenticate.user];
    }
  }
});
```

## License

ISC


### 示例

### API

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
