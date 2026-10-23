const fp = require('fastify-plugin');
const { num, list, success, pageResult, MESSAGE_STATUS, message } = require('../schema/response');

const MESSAGE_TAG = '消息队列-消息';
const QUEUE_TAG = '消息队列-队列';

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.post(
    `${options.prefix}/message/publish`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        tags: [MESSAGE_TAG],
        summary: '发布消息',
        description: '向指定主题投递一条消息并记录 PUBLISHED 轨迹；PG 模式写入消息表等待消费，Kafka 模式直接发送到 Kafka（不支持定时执行和最大重试次数）',
        body: {
          type: 'object',
          required: ['topic', 'payload'],
          properties: {
            topic: { type: 'string', description: '消息主题' },
            payload: { type: 'object', description: '消息内容' },
            priority: { type: 'number', default: 0, description: '优先级，数值越大越先被消费' },
            executeAt: { type: 'string', format: 'date-time', description: '定时执行时间，到达该时间前不会被拉取；不传立即可消费' },
            maxRetries: { type: 'number', description: '最大重试次数，不传使用插件配置 defaultMaxRetries' },
            traceId: { type: 'string', description: '追踪ID，不传自动生成 UUID；同一业务链路可复用以便查询轨迹' },
            meta: { type: 'object', description: '扩展元数据，保存在消息的 options 字段' }
          }
        },
        response: success('PG 模式返回新建的消息记录；Kafka 模式只返回 topic、traceId 和 status（PUBLISHED）', message)
      }
    },
    async request => {
      return services.message.publish(request.body);
    }
  );

  fastify.get(
    `${options.prefix}/message/poll`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        tags: [MESSAGE_TAG],
        summary: '拉取消息',
        description: '按优先级从高到低、创建时间从早到晚拉取已到执行时间的待消费消息，并将其锁定为 PROCESSING（记录 PROCESSING 轨迹）；消费方处理后需调用确认完成或标记失败接口，超时未确认会被自动恢复为待消费。Kafka 模式始终返回空数组',
        query: {
          type: 'object',
          required: ['topic'],
          properties: {
            topic: { type: 'string', description: '消息主题' },
            limit: { type: 'number', default: 10, description: '拉取数量' },
            lockTimeout: { type: 'number', description: '锁定超时时间(ms)；当前实现未使用，锁超时以插件配置 lockTimeout 为准' }
          }
        },
        response: success('本次拉取并锁定的消息列表，无可消费消息时为空数组', list('消息列表', message))
      }
    },
    async request => {
      return services.message.poll(request.query);
    }
  );

  fastify.post(
    `${options.prefix}/message/complete`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        tags: [MESSAGE_TAG],
        summary: '确认消息完成',
        description: '把 PROCESSING 状态的消息标记为 COMPLETED 并释放锁，记录 COMPLETED 轨迹；消息不存在返回 404，状态不是 PROCESSING 返回 409。Kafka 模式直接返回空对象',
        body: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', description: '消息ID（拉取消息返回的 id）' }
          }
        },
        response: success('更新后的消息记录；Kafka 模式为空对象', message)
      }
    },
    async request => {
      return services.message.complete(request.body);
    }
  );

  fastify.post(
    `${options.prefix}/message/fail`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        tags: [MESSAGE_TAG],
        summary: '标记消息失败',
        description: '标记 PROCESSING 状态的消息消费失败：未达最大重试次数时按指数退避设置下次重试时间并回到 PENDING；达到上限时移入死信并置为 FAILED。消息不存在返回 404，状态不是 PROCESSING 返回 409。Kafka 模式直接返回空对象',
        body: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', description: '消息ID（拉取消息返回的 id）' },
            error: { type: 'string', description: '错误信息，记录到轨迹和死信中' }
          }
        },
        response: success('更新后的消息记录（PENDING 待重试或 FAILED 已进入死信）；Kafka 模式为空对象', message)
      }
    },
    async request => {
      return services.message.fail(request.body);
    }
  );

  fastify.get(
    `${options.prefix}/message/list`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        tags: [MESSAGE_TAG],
        summary: '消息列表',
        description: '分页查询消息，按创建时间倒序；Kafka 模式不支持',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '按主题筛选' },
            status: { type: 'string', description: `按状态筛选，${MESSAGE_STATUS}` },
            traceId: { type: 'string', description: '按追踪ID筛选' },
            perPage: { type: 'number', default: 20, description: '每页条数' },
            currentPage: { type: 'number', default: 1, description: '页码，从 1 开始' }
          }
        },
        response: success('消息分页结果', pageResult('消息', message))
      }
    },
    async request => {
      const { topic, status, traceId, perPage, currentPage } = request.query;
      const filter = {};
      if (topic) filter.topic = topic;
      if (status) filter.status = status;
      if (traceId) filter.traceId = traceId;
      return services.message.list({ filter, perPage, currentPage });
    }
  );

  fastify.get(
    `${options.prefix}/queue/depth`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        tags: [QUEUE_TAG],
        summary: '队列深度',
        description: '统计 PENDING 状态的消息数量（含未到执行时间和等待重试的消息），并同步更新 mq_queue_depth 指标；Kafka 模式不支持',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '消息主题(可选,不传则查询全部)' }
          }
        },
        response: success('队列深度', {
          type: 'object',
          additionalProperties: true,
          properties: { depth: num('待消费（PENDING）消息数') }
        })
      }
    },
    async request => {
      return services.queue.getDepth(request.query);
    }
  );

  fastify.post(
    `${options.prefix}/queue/cleanup`,
    {
      onRequest: options.getAuthenticate('dlq:manage'),
      schema: {
        tags: [QUEUE_TAG],
        summary: '清理已完成的消息',
        description: '物理删除指定状态（默认 COMPLETED）的消息，可限定只删除某时间之前更新的；Kafka 模式不支持',
        body: {
          type: 'object',
          properties: {
            status: { type: 'string', default: 'COMPLETED', description: `要清理的消息状态，${MESSAGE_STATUS}` },
            olderThan: { type: 'string', format: 'date-time', description: '清理此时间之前更新的消息，不传则不限时间' }
          }
        },
        response: success('清理结果', {
          type: 'object',
          additionalProperties: true,
          properties: { deleted: num('删除的消息条数') }
        })
      }
    },
    async request => {
      return services.queue.cleanup(request.body);
    }
  );
});
