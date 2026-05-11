const fp = require('fastify-plugin');

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.get(
    `${options.prefix}/dlq/list`,
    {
      onRequest: options.getAuthenticate('dlq'),
      schema: {
        summary: '死信列表',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '按主题筛选' },
            replayed: { type: 'boolean', description: '按重放状态筛选' },
            perPage: { type: 'number', default: 20 },
            currentPage: { type: 'number', default: 1 }
          }
        }
      }
    },
    async request => {
      const { topic, replayed, perPage, currentPage } = request.query;
      const filter = {};
      if (topic) filter.topic = topic;
      if (replayed !== undefined) filter.replayed = replayed;
      return services.deadLetter.list({ filter, perPage, currentPage });
    }
  );

  fastify.post(
    `${options.prefix}/dlq/replay`,
    {
      onRequest: options.getAuthenticate('dlq:manage'),
      schema: {
        summary: '重放死信(批量)',
        body: {
          type: 'object',
          properties: {
            id: { type: 'string', description: '死信ID(单条重放)' },
            ids: { type: 'array', items: { type: 'string' }, description: '死信ID数组(批量重放)' }
          }
        }
      }
    },
    async request => {
      const { id, ids } = request.body;
      if (ids && ids.length > 0) {
        return services.deadLetter.batchReplay({ ids });
      }
      return services.deadLetter.replay({ id });
    }
  );

  fastify.post(
    `${options.prefix}/dlq/replay/:id`,
    {
      onRequest: options.getAuthenticate('dlq:manage'),
      schema: {
        summary: '重放死信(单条)',
        params: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', description: '死信ID' }
          }
        }
      }
    },
    async request => {
      return services.deadLetter.replay({ id: request.params.id });
    }
  );

  fastify.get(
    `${options.prefix}/trace/detail`,
    {
      onRequest: options.getAuthenticate('trace'),
      schema: {
        summary: '查询消息轨迹(query参数)',
        query: {
          type: 'object',
          required: ['traceId'],
          properties: {
            traceId: { type: 'string', description: '追踪ID' }
          }
        }
      }
    },
    async request => {
      return services.trace.get(request.query);
    }
  );

  fastify.get(
    `${options.prefix}/trace/:traceId`,
    {
      onRequest: options.getAuthenticate('trace'),
      schema: {
        summary: '查询消息轨迹(路径参数)',
        params: {
          type: 'object',
          required: ['traceId'],
          properties: {
            traceId: { type: 'string', description: '追踪ID' }
          }
        }
      }
    },
    async request => {
      return services.trace.get({ traceId: request.params.traceId });
    }
  );

  fastify.get(
    `${options.prefix}/trace/list`,
    {
      onRequest: options.getAuthenticate('trace'),
      schema: {
        summary: '消息轨迹列表',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '按主题筛选' },
            messageId: { type: 'string', description: '按消息ID筛选' },
            event: { type: 'string', description: '按事件类型筛选' },
            perPage: { type: 'number', default: 20 },
            currentPage: { type: 'number', default: 1 }
          }
        }
      }
    },
    async request => {
      const { topic, messageId, event, perPage, currentPage } = request.query;
      const filter = {};
      if (topic) filter.topic = topic;
      if (messageId) filter.messageId = messageId;
      if (event) filter.event = event;
      return services.trace.list({ filter, perPage, currentPage });
    }
  );
});
