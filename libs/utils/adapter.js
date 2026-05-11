const crypto = require('crypto');
const { MESSAGE_STATUS, TRACE_EVENTS } = require('../constants');

/**
 * PG 适配器 - 基于 PostgreSQL/Sequelize 的消息队列实现
 * 用于需要从 PG 迁移到 Kafka 的场景，提供统一接口
 */
const createPgAdapter = options => {
  const { models, sequelize, generateId, metrics, defaultMaxRetries = 3, retryBaseDelay = 1000, retryMaxDelay = 60000, lockTimeout = 30000, pollLimit = 10 } = options;
  const { Op } = sequelize.constructor;
  const instance = sequelize;

  const CONSUMER_ID = `${process.pid || 'unknown'}_${Date.now()}`;

  const addTrace = async (traceId, topic, event, detail, messageId) => {
    await models.messageTrace.create({
      traceId,
      topic,
      event,
      detail: detail || null,
      messageId: messageId || null
    });
  };

  return {
    adapterType: 'pg',

    async publish({ topic, payload, priority, executeAt, maxRetries, traceId, meta }) {
      const id = generateId();
      const resolvedTraceId = traceId || crypto.randomUUID();
      const message = await models.message.create({
        id,
        topic,
        payload,
        status: MESSAGE_STATUS.PENDING,
        priority: priority || 0,
        executeAt: executeAt || null,
        maxRetries: maxRetries !== undefined ? maxRetries : defaultMaxRetries,
        traceId: resolvedTraceId,
        options: meta || {}
      });
      await addTrace(resolvedTraceId, topic, TRACE_EVENTS.PUBLISHED, { priority, executeAt }, id);
      return message;
    },

    async poll({ topic, limit }) {
      const now = new Date();
      return instance.transaction(
        {
          isolationLevel: sequelize.constructor.Transaction.ISOLATION_LEVELS.READ_COMMITTED
        },
        async t => {
          const where = {
            topic,
            status: MESSAGE_STATUS.PENDING,
            [Op.and]: [{ [Op.or]: [{ executeAt: null }, { executeAt: { [Op.lte]: now } }] }, { [Op.or]: [{ nextRetryAt: null }, { nextRetryAt: { [Op.lte]: now } }] }]
          };
          const query = {
            where,
            order: [
              ['priority', 'DESC'],
              ['createdAt', 'ASC']
            ],
            limit: limit || pollLimit,
            transaction: t
          };
          if (t.LOCK && t.LOCK.UPDATE) {
            query.lock = t.LOCK.UPDATE;
          }
          const rows = await models.message.findAll(query);

          if (rows.length === 0) return [];

          const ids = rows.map(msg => msg.id);
          await models.message.update({ status: MESSAGE_STATUS.PROCESSING, consumerId: CONSUMER_ID, lockedAt: new Date() }, { where: { id: { [Op.in]: ids } }, transaction: t });

          const traces = rows.map(msg => ({
            traceId: msg.traceId,
            topic: msg.topic,
            event: TRACE_EVENTS.PROCESSING,
            detail: { consumerId: CONSUMER_ID },
            messageId: msg.id
          }));
          await models.messageTrace.bulkCreate(traces, { transaction: t });

          return rows.map(msg => {
            msg.status = MESSAGE_STATUS.PROCESSING;
            msg.consumerId = CONSUMER_ID;
            msg.lockedAt = new Date();
            return msg;
          });
        }
      );
    },

    async complete({ id }) {
      const message = await models.message.findByPk(id);
      if (!message) {
        const err = new Error('消息不存在');
        err.statusCode = 404;
        throw err;
      }
      if (message.status !== MESSAGE_STATUS.PROCESSING) {
        const err = new Error(`消息状态不正确，当前: ${message.status}，期望: ${MESSAGE_STATUS.PROCESSING}`);
        err.statusCode = 409;
        throw err;
      }
      await message.update({ status: MESSAGE_STATUS.COMPLETED, consumerId: null, lockedAt: null });
      await addTrace(message.traceId, message.topic, TRACE_EVENTS.COMPLETED, null, message.id);
      if (metrics) metrics.consumedTotal.inc({ topic: message.topic });
      return message;
    },

    async fail({ id, error }) {
      const message = await models.message.findByPk(id);
      if (!message) {
        const err = new Error('消息不存在');
        err.statusCode = 404;
        throw err;
      }
      if (message.status !== MESSAGE_STATUS.PROCESSING) {
        const err = new Error(`消息状态不正确，当前: ${message.status}，期望: ${MESSAGE_STATUS.PROCESSING}`);
        err.statusCode = 409;
        throw err;
      }

      const newRetryCount = message.retryCount + 1;
      const shouldRetry = newRetryCount < message.maxRetries;

      if (shouldRetry) {
        const delay = Math.min(retryBaseDelay * Math.pow(2, newRetryCount - 1), retryMaxDelay);
        const nextRetryAt = new Date(Date.now() + delay);
        await message.update({ status: MESSAGE_STATUS.PENDING, retryCount: newRetryCount, nextRetryAt, consumerId: null, lockedAt: null });
        await addTrace(message.traceId, message.topic, TRACE_EVENTS.FAILED, { retryCount: newRetryCount, error: error || 'Unknown error', nextRetryAt }, message.id);
        if (metrics) metrics.failedTotal.inc({ topic: message.topic });
      } else {
        await instance.transaction(async t => {
          await models.deadLetter.create({ originalId: message.id, topic: message.topic, payload: message.payload, errorMessage: error || 'Max retries exceeded' }, { transaction: t });
          await message.update({ status: MESSAGE_STATUS.FAILED, consumerId: null, lockedAt: null }, { transaction: t });
        });
        await addTrace(message.traceId, message.topic, TRACE_EVENTS.MOVED_TO_DLQ, { error: error || 'Max retries exceeded', retryCount: newRetryCount }, message.id);
        if (metrics) {
          metrics.failedTotal.inc({ topic: message.topic });
          metrics.dlqTotal.inc({ topic: message.topic });
        }
      }
      return message;
    },

    async replayDeadLetter({ id }) {
      const deadLetter = await models.deadLetter.findByPk(id);
      if (!deadLetter) {
        const err = new Error('死信不存在');
        err.statusCode = 404;
        throw err;
      }
      if (deadLetter.replayed) {
        const err = new Error('该死信已被重放');
        err.statusCode = 400;
        throw err;
      }

      const message = await instance.transaction(async t => {
        const msg = await models.message.create(
          {
            topic: deadLetter.topic,
            payload: deadLetter.payload,
            status: MESSAGE_STATUS.PENDING,
            traceId: crypto.randomUUID()
          },
          { transaction: t }
        );
        await deadLetter.update({ replayed: true, replayedAt: new Date() }, { transaction: t });
        return msg;
      });

      await addTrace(message.traceId, deadLetter.topic, TRACE_EVENTS.REPLAYED, { originalId: deadLetter.originalId, deadLetterId: deadLetter.id }, message.id);
      return message;
    },

    async batchReplayDeadLetters({ ids }) {
      const results = [];
      for (const id of ids) {
        try {
          const message = await this.replayDeadLetter({ id });
          results.push({ id, success: true, messageId: message.id });
        } catch (e) {
          results.push({ id, success: false, error: e.message || 'Unknown error' });
        }
      }
      return results;
    },

    async listDeadLetters({ filter = {}, perPage = 20, currentPage = 1 }) {
      const where = {};
      if (filter.topic) where.topic = filter.topic;
      if (filter.replayed !== undefined) where.replayed = filter.replayed;

      const { rows, count } = await models.deadLetter.findAndCountAll({
        where,
        offset: perPage * (currentPage - 1),
        limit: perPage,
        order: [['createdAt', 'DESC']]
      });

      return { pageData: rows, totalCount: count };
    },

    async listMessages({ filter = {}, perPage = 20, currentPage = 1 }) {
      const where = {};
      if (filter.topic) where.topic = filter.topic;
      if (filter.status) where.status = filter.status;
      if (filter.traceId) where.traceId = filter.traceId;

      const { rows, count } = await models.message.findAndCountAll({
        where,
        offset: perPage * (currentPage - 1),
        limit: perPage,
        order: [['createdAt', 'DESC']]
      });

      return { pageData: rows, totalCount: count };
    },

    async getTrace({ traceId }) {
      return models.messageTrace.findAll({ where: { traceId }, order: [['createdAt', 'ASC']] });
    },

    async listTraces({ filter = {}, perPage = 20, currentPage = 1 }) {
      const where = {};
      if (filter.topic) where.topic = filter.topic;
      if (filter.messageId) where.messageId = filter.messageId;
      if (filter.event) where.event = filter.event;

      const { rows, count } = await models.messageTrace.findAndCountAll({
        where,
        offset: perPage * (currentPage - 1),
        limit: perPage,
        order: [['createdAt', 'DESC']]
      });

      return { pageData: rows, totalCount: count };
    },

    async getQueueDepth({ topic }) {
      const where = { status: MESSAGE_STATUS.PENDING };
      if (topic) where.topic = topic;
      const count = await models.message.count({ where });
      if (metrics) metrics.queueDepth.set({ topic: topic || '_all' }, count);
      return { depth: count };
    },

    async recoverLocked() {
      const cutoff = new Date(Date.now() - lockTimeout);
      const lockedMessages = await models.message.findAll({
        where: {
          status: MESSAGE_STATUS.PROCESSING,
          lockedAt: { [Op.lte]: cutoff }
        }
      });

      if (lockedMessages.length === 0) return { recovered: 0 };

      const ids = lockedMessages.map(m => m.id);
      await models.message.update(
        {
          status: MESSAGE_STATUS.PENDING,
          consumerId: null,
          lockedAt: null
        },
        {
          where: { id: { [Op.in]: ids } }
        }
      );

      await models.messageTrace.bulkCreate(
        lockedMessages.map(m => ({
          traceId: m.traceId,
          topic: m.topic,
          event: TRACE_EVENTS.LOCK_RECOVERED,
          detail: { previousConsumerId: m.consumerId, lockedAt: m.lockedAt },
          messageId: m.id
        }))
      );

      return { recovered: ids.length };
    },

    async cleanupMessages({ status = MESSAGE_STATUS.COMPLETED, olderThan = null } = {}) {
      const where = { status };
      if (olderThan) {
        where.updatedAt = { [Op.lt]: new Date(olderThan) };
      }
      const count = await models.message.destroy({ where });
      return { deleted: count };
    }
  };
};

/**
 * Kafka 适配器 - 基于 kafkajs 的消息队列实现
 * 注意：Kafka 模式下不支持 DLQ 重放、轨迹查询、队列深度和锁定恢复
 */
const createKafkaAdapter = options => {
  const { kafka, topicPrefix, metrics } = options;
  const producer = kafka.producer();
  const consumers = {};
  let connected = false;

  const ensureConnected = async () => {
    if (!connected) {
      await producer.connect();
      connected = true;
    }
  };

  const notSupported = method => () => {
    throw new Error(`Kafka adapter does not support ${method}`);
  };

  return {
    adapterType: 'kafka',

    async publish({ topic, payload, priority, traceId, meta }) {
      await ensureConnected();
      const resolvedTraceId = traceId || crypto.randomUUID();
      await producer.send({
        topic: `${topicPrefix || ''}${topic}`,
        messages: [
          {
            key: resolvedTraceId,
            value: JSON.stringify({ payload, traceId: resolvedTraceId, priority: priority || 0, meta: meta || {} }),
            headers: priority ? { priority: String(priority) } : {}
          }
        ]
      });
      return { topic, traceId: resolvedTraceId, status: TRACE_EVENTS.PUBLISHED };
    },

    async poll() {
      return [];
    },

    async subscribe({ topic, handler, groupId }) {
      await ensureConnected();
      const consumer = kafka.consumer({ groupId: groupId || `${topic}-group` });
      await consumer.connect();
      await consumer.subscribe({ topic: `${topicPrefix || ''}${topic}`, fromBeginning: false });
      await consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const data = JSON.parse(message.value.toString());
            await handler(data);
            if (metrics) metrics.consumedTotal.inc({ topic });
          } catch (e) {
            if (metrics) metrics.failedTotal.inc({ topic });
            console.error(`[fastify-mq] Kafka consumer error for topic ${topic}:`, e.message);
          }
        }
      });
      consumers[topic] = consumer;
    },

    async complete() {
      return {};
    },
    async fail() {
      return {};
    },
    replayDeadLetter: notSupported('DLQ replay'),
    batchReplayDeadLetters: notSupported('batch DLQ replay'),
    listDeadLetters: notSupported('DLQ listing'),
    listMessages: notSupported('message listing'),
    getTrace: notSupported('trace query'),
    listTraces: notSupported('trace listing'),
    getQueueDepth: notSupported('queue depth'),
    recoverLocked: notSupported('lock recovery'),
    cleanupMessages: notSupported('message cleanup'),

    async disconnect() {
      for (const [topic, consumer] of Object.entries(consumers)) {
        try {
          await consumer.disconnect();
        } catch (e) {
          console.error(`[fastify-mq] Kafka consumer disconnect error for topic ${topic}:`, e.message);
        }
      }
      if (connected) {
        try {
          await producer.disconnect();
        } catch (e) {
          console.error('[fastify-mq] Kafka producer disconnect error:', e.message);
        }
        connected = false;
      }
    }
  };
};

module.exports = { createPgAdapter, createKafkaAdapter };
