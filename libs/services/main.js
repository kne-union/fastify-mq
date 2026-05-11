const fp = require('fastify-plugin');
const { createPgAdapter, createKafkaAdapter } = require('../utils/adapter');
const { metrics, getValuesMap, sampler } = require('../utils/metrics');

module.exports = fp(async (fastify, options) => {
  const { models } = fastify[options.name];
  const sequelize = fastify.sequelize.instance;

  // 根据配置创建适配器
  let adapter;
  if (options.adapter === 'kafka') {
    adapter = createKafkaAdapter({
      kafka: options.kafka,
      topicPrefix: options.topicPrefix,
      metrics
    });
  } else {
    adapter = createPgAdapter({
      models,
      sequelize,
      generateId: () => fastify.sequelize.generateId(),
      metrics,
      defaultMaxRetries: options.defaultMaxRetries,
      retryBaseDelay: options.retryBaseDelay,
      retryMaxDelay: options.retryMaxDelay,
      lockTimeout: options.lockTimeout,
      pollLimit: options.pollLimit
    });
  }

  // 存储适配器引用供 index.js onClose 使用
  options._adapter = adapter;

  // === 消费者逻辑 ===

  const subscribe = (topic, handler) => {
    if (!options._subscribers[topic]) {
      options._subscribers[topic] = [];
    }
    options._subscribers[topic].push(handler);
  };

  const startConsumer = () => {
    if (options._consumerTimer) return;
    options._consumerRunning = false;

    // Kafka 适配器使用自身的消费者机制
    if (adapter.adapterType === 'kafka') {
      const topics = Object.keys(options._subscribers);
      for (const topic of topics) {
        const handlers = options._subscribers[topic] || [];
        adapter.subscribe({
          topic,
          handler: async data => {
            for (const handler of handlers) {
              await handler(data);
            }
          },
          groupId: options.kafkaGroupId
        });
      }
      return;
    }

    // PG 适配器使用轮询机制
    const runOnce = async () => {
      if (options._consumerRunning) return;
      options._consumerRunning = true;
      let resolveRun;
      options._consumerRunningPromise = new Promise(r => {
        resolveRun = r;
      });

      try {
        const topics = Object.keys(options._subscribers);
        if (topics.length === 0) return;

        // 各 topic 并行消费
        await Promise.all(
          topics.map(async topic => {
            try {
              const messages = await adapter.poll({ topic });
              for (const msg of messages) {
                const handlers = options._subscribers[topic] || [];
                // 所有 handler 必须全部成功才 complete，否则 fail
                let allSucceeded = true;
                let lastError = null;
                for (const handler of handlers) {
                  try {
                    await handler(msg);
                  } catch (e) {
                    allSucceeded = false;
                    lastError = e;
                    console.error(`[fastify-mq] Consumer handler error for topic ${topic}, message ${msg.id}:`, e.message);
                  }
                }
                if (allSucceeded) {
                  await adapter.complete({ id: msg.id });
                } else {
                  await adapter.fail({ id: msg.id, error: lastError?.message || 'Handler error' });
                }
              }
            } catch (e) {
              console.error(`[fastify-mq] Consumer poll error for topic ${topic}:`, e.message);
            }
          })
        );
      } finally {
        options._consumerRunning = false;
        resolveRun();
      }
    };

    options._consumerTimer = setInterval(runOnce, options.pollInterval);
  };

  const stopConsumer = async () => {
    if (options._consumerTimer) {
      clearInterval(options._consumerTimer);
      options._consumerTimer = null;
    }
    // 等待当前消费周期完成
    if (options._consumerRunningPromise) {
      await options._consumerRunningPromise;
    }
    options._consumerRunning = false;
  };

  // === Dashboard 逻辑 ===

  const getDashboardData = async ({ windowMs = 300000, stepMs = 60000 } = {}) => {
    const queueDepthValues = getValuesMap(metrics.queueDepth);
    const consumedValues = getValuesMap(metrics.consumedTotal);
    const failedValues = getValuesMap(metrics.failedTotal);
    const dlqValues = getValuesMap(metrics.dlqTotal);

    const consumeRate = sampler.getRate('mq_consumed_total', windowMs);
    const failureRate = sampler.getRate('mq_failed_total', windowMs);
    const dlqRate = sampler.getRate('mq_dlq_total', windowMs);

    const totalConsumeRate = Object.values(consumeRate).reduce((a, b) => a + b, 0);
    const totalFailureRate = Object.values(failureRate).reduce((a, b) => a + b, 0);
    const successRatio = totalConsumeRate + totalFailureRate > 0 ? totalConsumeRate / (totalConsumeRate + totalFailureRate) : null;

    const successRatioByTopic = {};
    const allTopics = new Set([...Object.keys(consumeRate), ...Object.keys(failureRate)]);
    for (const topic of allTopics) {
      const cr = consumeRate[topic] || 0;
      const fr = failureRate[topic] || 0;
      successRatioByTopic[topic] = cr + fr > 0 ? cr / (cr + fr) : null;
    }

    const totalConsumed = Object.values(consumedValues).reduce((a, b) => a + b, 0);
    const totalFailed = Object.values(failedValues).reduce((a, b) => a + b, 0);
    const totalDlq = Object.values(dlqValues).reduce((a, b) => a + b, 0);
    const totalQueueDepth = Object.values(queueDepthValues).reduce((a, b) => a + b, 0);

    const queueDepthTimeSeries = sampler.getTimeSeries('mq_queue_depth', { windowMs, stepMs });
    const consumeRateTimeSeries = sampler.getTimeSeries('mq_consumed_total', { windowMs, stepMs, rate: true });
    const failureRateTimeSeries = sampler.getTimeSeries('mq_failed_total', { windowMs, stepMs, rate: true });
    const dlqRateTimeSeries = sampler.getTimeSeries('mq_dlq_total', { windowMs, stepMs, rate: true });

    return {
      timestamp: Date.now(),
      current: {
        queueDepth: { byTopic: queueDepthValues, total: totalQueueDepth },
        consumedTotal: { byTopic: consumedValues, total: totalConsumed },
        failedTotal: { byTopic: failedValues, total: totalFailed },
        dlqTotal: { byTopic: dlqValues, total: totalDlq },
        consumeRate: { byTopic: consumeRate, total: totalConsumeRate },
        failureRate: { byTopic: failureRate, total: totalFailureRate },
        dlqRate: { byTopic: dlqRate, total: Object.values(dlqRate).reduce((a, b) => a + b, 0) },
        successRatio,
        successRatioByTopic
      },
      timeSeries: {
        queueDepth: queueDepthTimeSeries,
        consumeRate: consumeRateTimeSeries,
        failureRate: failureRateTimeSeries,
        dlqRate: dlqRateTimeSeries
      }
    };
  };

  // === 注册服务 ===

  Object.assign(fastify[options.name].services, {
    message: {
      publish: params => adapter.publish(params),
      poll: params => adapter.poll(params),
      complete: params => adapter.complete(params),
      fail: params => adapter.fail(params),
      list: params => adapter.listMessages(params)
    },
    deadLetter: {
      replay: params => adapter.replayDeadLetter(params),
      batchReplay: params => adapter.batchReplayDeadLetters(params),
      list: params => adapter.listDeadLetters(params)
    },
    trace: {
      get: params => adapter.getTrace(params),
      list: params => adapter.listTraces(params)
    },
    queue: {
      getDepth: params => adapter.getQueueDepth(params),
      recoverLocked: () => adapter.recoverLocked(),
      cleanup: params => adapter.cleanupMessages(params),
      subscribe,
      startConsumer,
      stopConsumer
    },
    dashboard: {
      getData: getDashboardData
    }
  });
});
