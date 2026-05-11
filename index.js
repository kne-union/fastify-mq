const fp = require('fastify-plugin');
const path = require('node:path');
const { registry, sampler } = require('./libs/utils/metrics');

module.exports = fp(async (fastify, options) => {
  options = Object.assign(
    {},
    {
      dbTableNamePrefix: 't_',
      name: 'mq',
      prefix: '/mq',
      defaultMaxRetries: 3,
      pollLimit: 10,
      pollInterval: 5000,
      lockTimeout: 30000,
      lockRecoveryInterval: 30000,
      retryBaseDelay: 1000,
      retryMaxDelay: 60000,
      metricsSampleInterval: 10000,
      metricsMaxSamples: 360,
      adapter: 'pg',
      kafka: null,
      topicPrefix: '',
      kafkaGroupId: undefined,
      getAuthenticate: type => {
        switch (type) {
          case 'dlq:manage':
          case 'message':
          case 'dlq':
          case 'trace':
          case 'dashboard':
          default:
            return [];
        }
      },
      getMessageModel: () => {
        return fastify[options.name]?.models?.message;
      }
    },
    options
  );

  options._subscribers = {};
  options._consumerTimer = null;
  options._consumerRunning = false;
  options._consumerRunningPromise = null;
  options._lockRecoveryTimer = null;
  options._adapter = null;

  fastify.register(require('@kne/fastify-namespace'), {
    options,
    name: options.name,
    modules: [
      ['controllers', path.resolve(__dirname, './libs/controllers')],
      [
        'models',
        await fastify.sequelize.addModels(path.resolve(__dirname, './libs/models'), {
          prefix: options.dbTableNamePrefix,
          getMessageModel: options.getMessageModel
        })
      ],
      ['services', path.resolve(__dirname, './libs/services')]
    ]
  });

  // Metrics endpoint
  if (registry) {
    fastify.get(`${options.prefix}/metrics`, async (request, reply) => {
      reply.type('text/plain');
      return registry.metrics();
    });
  }

  // Start sampler
  if (sampler) {
    sampler.start();
  }

  // 启动锁定恢复定时器
  fastify.addHook('onReady', () => {
    const { services } = fastify[options.name];
    if (services?.queue?.recoverLocked) {
      options._lockRecoveryTimer = setInterval(async () => {
        try {
          const result = await services.queue.recoverLocked();
          if (result.recovered > 0) {
            console.warn(`[fastify-mq] Recovered ${result.recovered} timed-out locked messages`);
          }
        } catch (e) {
          console.error('[fastify-mq] Lock recovery error:', e.message);
        }
      }, options.lockRecoveryInterval);
      options._lockRecoveryTimer.unref();
    }
  });

  // 优雅关闭
  fastify.addHook('onClose', async () => {
    const { services } = fastify[options.name];

    // 停止消费者（优雅等待当前消费周期完成）
    if (services?.queue?.stopConsumer) {
      await services.queue.stopConsumer();
    } else {
      if (options._consumerTimer) {
        clearInterval(options._consumerTimer);
        options._consumerTimer = null;
      }
      options._consumerRunning = false;
    }

    // 停止锁定恢复定时器
    if (options._lockRecoveryTimer) {
      clearInterval(options._lockRecoveryTimer);
      options._lockRecoveryTimer = null;
    }

    // 停止采样器
    if (sampler) sampler.stop();

    // 断开适配器连接
    if (options._adapter?.disconnect) {
      await options._adapter.disconnect();
    }
  });
});
