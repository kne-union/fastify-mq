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
});
