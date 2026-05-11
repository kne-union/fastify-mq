const fp = require('fastify-plugin');

module.exports = fp(async (fastify, options) => {
  const { services } = fastify[options.name];

  fastify.get(
    `${options.prefix}/dashboard`,
    {
      onRequest: options.getAuthenticate('dashboard'),
      schema: {
        summary: 'Dashboard 数据接口',
        query: {
          type: 'object',
          properties: {
            window: { type: 'number', default: 300000, description: 'Rate计算窗口(ms), 默认5分钟' },
            step: { type: 'number', default: 60000, description: '时序数据步长(ms), 默认1分钟' }
          }
        }
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
        summary: 'Dashboard SSE 实时推送接口',
        query: {
          type: 'object',
          properties: {
            window: { type: 'number', default: 300000, description: 'Rate计算窗口(ms), 默认5分钟' },
            step: { type: 'number', default: 60000, description: '时序数据步长(ms), 默认1分钟' },
            interval: { type: 'number', default: 5000, description: '推送间隔(ms), 默认5秒' }
          }
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
