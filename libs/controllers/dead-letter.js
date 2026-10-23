const fp = require('fastify-plugin');
const { list, success, pageResult, TRACE_EVENT, message, deadLetter, trace } = require('../schema/response');

const DLQ_TAG = '消息队列-死信';
const TRACE_TAG = '消息队列-轨迹';

const replayedMessage = '重放生成的新消息记录（新的追踪 ID，状态为 PENDING）';

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.get(
    `${options.prefix}/dlq/list`,
    {
      onRequest: options.getAuthenticate('dlq'),
      schema: {
        tags: [DLQ_TAG],
        summary: '死信列表',
        description: '分页查询超过最大重试次数后进入死信队列的消息，按创建时间倒序；Kafka 模式不支持',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '按主题筛选' },
            replayed: { type: 'boolean', description: '按重放状态筛选：true 已重放，false 未重放，不传查全部' },
            perPage: { type: 'number', default: 20, description: '每页条数' },
            currentPage: { type: 'number', default: 1, description: '页码，从 1 开始' }
          }
        },
        response: success('死信分页结果', pageResult('死信', deadLetter))
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
        tags: [DLQ_TAG],
        summary: '重放死信(批量)',
        description: '把死信重新投递为一条新的待消费消息并标记死信已重放（记录 REPLAYED 轨迹）。传 ids 时批量重放，逐条返回结果且单条失败不影响其他；只传 id 时单条重放，死信不存在返回 404，已重放过返回 400。Kafka 模式不支持',
        body: {
          type: 'object',
          properties: {
            id: { type: 'string', description: '死信ID(单条重放)' },
            ids: { type: 'array', items: { type: 'string' }, description: '死信ID数组(批量重放)，非空时优先于 id' }
          }
        },
        response: {
          200: {
            description: `单条重放返回${replayedMessage}；批量重放返回数组，每项为 { id: 死信 ID, success: 是否成功, messageId: 成功时的新消息 ID, error: 失败原因 }`
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
        tags: [DLQ_TAG],
        summary: '重放死信(单条)',
        description: '把指定死信重新投递为一条新的待消费消息并标记已重放；死信不存在返回 404，已重放过返回 400。Kafka 模式不支持',
        params: {
          type: 'object',
          required: ['id'],
          properties: {
            id: { type: 'string', description: '死信ID' }
          }
        },
        response: success(replayedMessage, message)
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
        tags: [TRACE_TAG],
        summary: '查询消息轨迹(query参数)',
        description: '按追踪 ID 查询该链路上的全部轨迹事件，按发生时间正序；Kafka 模式不支持',
        query: {
          type: 'object',
          required: ['traceId'],
          properties: {
            traceId: { type: 'string', description: '追踪ID' }
          }
        },
        response: success('轨迹事件列表，按发生时间正序', list('轨迹事件列表', trace))
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
        tags: [TRACE_TAG],
        summary: '查询消息轨迹(路径参数)',
        description: '与「查询消息轨迹(query参数)」相同，追踪 ID 通过路径传入；Kafka 模式不支持',
        params: {
          type: 'object',
          required: ['traceId'],
          properties: {
            traceId: { type: 'string', description: '追踪ID' }
          }
        },
        response: success('轨迹事件列表，按发生时间正序', list('轨迹事件列表', trace))
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
        tags: [TRACE_TAG],
        summary: '消息轨迹列表',
        description: '分页查询轨迹事件，按创建时间倒序；Kafka 模式不支持',
        query: {
          type: 'object',
          properties: {
            topic: { type: 'string', description: '按主题筛选' },
            messageId: { type: 'string', description: '按消息ID筛选' },
            event: { type: 'string', description: `按事件类型筛选，${TRACE_EVENT}` },
            perPage: { type: 'number', default: 20, description: '每页条数' },
            currentPage: { type: 'number', default: 1, description: '页码，从 1 开始' }
          }
        },
        response: success('轨迹事件分页结果', pageResult('轨迹事件', trace))
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
