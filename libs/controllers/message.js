const fp = require('fastify-plugin');

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.post(
    `${options.prefix}/message/publish`,
    {
      onRequest: options.getAuthenticate('message'),
      schema: {
        summary: '发布消息',
        body: {
          type: 'object',
          required: ['topic', 'payload'],
          properties: {
            topic: { type: 'string', description: '消息主题' },
            payload: { type: 'object', description: '消息内容' },
            priority: { type: 'number', default: 0, description: '优先级' },
            executeAt: { type: 'string', format: 'date-time', description: '定时执行时间' },
            maxRetries: { type: 'number', description: '最大重试次数' },
            traceId: { type: 'string', description: '追踪ID' },
            meta: { type: 'object', description: '扩展元数据' }
          }
        }
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
        summary: '拉取消息',
        query: {
          type: 'object',
          required: ['topic'],
          properties: {
            topic: { type: 'string', description: '消息主题' },
            limit: { type: 'number', default: 10, description: '拉取数量' },
            lockTimeout: { type: 'number', description: '锁定超时时间(ms)' }
          }
        }
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
        summary: '确认消息完成',
        body: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', description: '消息ID' }
          }
        }
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
        summary: '标记消息失败',
        body: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', description: '消息ID' },
            error: { type: 'string', description: '错误信息' }
          }
        }
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
        summary: '消息列表',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '按主题筛选' },
            status: { type: 'string', description: '按状态筛选' },
            traceId: { type: 'string', description: '按追踪ID筛选' },
            perPage: { type: 'number', default: 20 },
            currentPage: { type: 'number', default: 1 }
          }
        }
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
        summary: '队列深度',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '消息主题(可选,不传则查询全部)' }
          }
        }
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
        summary: '清理已完成的消息',
        body: {
          type: 'object',
          properties: {
            status: { type: 'string', default: 'COMPLETED', description: '要清理的消息状态' },
            olderThan: { type: 'string', format: 'date-time', description: '清理此时间之前更新的消息' }
          }
        }
      }
    },
    async request => {
      return services.queue.cleanup(request.body);
    }
  );
});
