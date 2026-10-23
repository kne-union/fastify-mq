const fp = require('fastify-plugin');
const { num, any, obj, success } = require('../schema/response');

const TAG = '消息队列-监控';

const metric = (description, unit) =>
  obj(description, {
    byTopic: any(`按主题统计的${unit}，键为主题（_all 表示未区分主题）`),
    total: num(`全部主题合计的${unit}`)
  });

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.get(
    `${options.prefix}/dashboard`,
    {
      onRequest: options.getAuthenticate('dashboard'),
      schema: {
        tags: [TAG],
        summary: 'Dashboard 数据接口',
        description: '返回当前进程内存中的消息队列指标快照和时序数据（多实例部署时只反映当前实例）',
        query: {
          type: 'object',
          properties: {
            window: { type: 'number', default: 300000, description: 'Rate计算窗口(ms), 默认5分钟' },
            step: { type: 'number', default: 60000, description: '时序数据步长(ms), 默认1分钟' }
          }
        },
        response: success('Dashboard 数据', {
          type: 'object',
          additionalProperties: true,
          properties: {
            timestamp: num('生成时间戳（毫秒）'),
            current: obj('当前指标', {
              queueDepth: metric('队列深度', '待消费消息数'),
              consumedTotal: metric('累计消费成功数', '消费成功数'),
              failedTotal: metric('累计消费失败数', '消费失败数'),
              dlqTotal: metric('累计进入死信数', '进入死信数'),
              consumeRate: metric('窗口内消费速率', '每秒消费数'),
              failureRate: metric('窗口内失败速率', '每秒失败数'),
              dlqRate: metric('窗口内死信速率', '每秒进入死信数'),
              successRatio: num('窗口内消费成功率（0-1），窗口内无消费时为 null'),
              successRatioByTopic: any('按主题的消费成功率，键为主题，值为 0-1 或 null')
            }),
            timeSeries: obj('时序数据，每项为一个时间桶的数据点', {
              queueDepth: any('队列深度时序'),
              consumeRate: any('消费速率时序'),
              failureRate: any('失败速率时序'),
              dlqRate: any('死信速率时序')
            })
          }
        })
      }
    },
    async request => {
      return services.dashboard.getData({
        windowMs: request.query.window || 300000,
        stepMs: request.query.step || 60000
      });
    }
  );

  fastify.get(
    `${options.prefix}/dashboard/sse`,
    {
      onRequest: options.getAuthenticate('dashboard'),
      schema: {
        tags: [TAG],
        summary: 'Dashboard SSE 实时推送接口',
        description: '以 text/event-stream 长连接按间隔推送 Dashboard 数据（与 Dashboard 数据接口结构相同），取数失败时推送 event: error；客户端断开后停止推送',
        query: {
          type: 'object',
          properties: {
            window: { type: 'number', default: 300000, description: 'Rate计算窗口(ms), 默认5分钟' },
            step: { type: 'number', default: 60000, description: '时序数据步长(ms), 默认1分钟' },
            interval: { type: 'number', default: 5000, description: '推送间隔(ms), 默认5秒' }
          }
        },
        response: {
          200: { description: 'text/event-stream 事件流，每条 data 为 Dashboard 数据 JSON' }
        }
      }
    },
    (request, reply) => {
      const windowMs = request.query.window || 300000;
      const stepMs = request.query.step || 60000;
      const intervalMs = request.query.interval || 5000;

      reply.raw.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
        'X-Accel-Buffering': 'no'
      });

      const sendEvent = async () => {
        try {
          const data = await services.dashboard.getData({ windowMs, stepMs });
          reply.raw.write(`data: ${JSON.stringify(data)}\n\n`);
        } catch (e) {
          reply.raw.write(`event: error\ndata: ${JSON.stringify({ message: e.message })}\n\n`);
        }
      };

      sendEvent();
      const timer = setInterval(sendEvent, intervalMs);

      request.raw.on('close', () => {
        clearInterval(timer);
      });
    }
  );
});
