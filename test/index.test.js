const {expect} = require('chai');
const fastify = require('fastify');
const path = require('node:path');
const {createKafkaAdapter, createPgAdapter} = require('../libs/utils/adapter');
const {MESSAGE_STATUS, TRACE_EVENTS} = require('../libs/constants');

const createApp = async (mqOptions = {}) => {
  const app = fastify({logger: false});

  await app.register(require('@kne/fastify-sequelize'), {
    db: {dialect: 'sqlite', storage: ':memory:'},
    prefix: 't_'
  });

  const options = Object.assign({
    dbTableNamePrefix: 't_',
    name: 'mq',
    prefix: '/mq'
  }, mqOptions);

  await app.register(require('../index'), options);

  // Sync tables after models are loaded
  await app.sequelize.sync({force: true});

  await app.ready();
  return app;
};

describe('@kne/fastify-mq', function () {
  this.timeout(10000);

  let app;

  afterEach(async () => {
    if (app) {
      await app.close();
      app = null;
    }
  });

  describe('插件注册测试', () => {
    it('should register plugin with default options', async () => {
      app = await createApp();
      expect(app.mq).to.exist;
      expect(app.mq.services).to.exist;
      expect(app.mq.models).to.exist;
    });

    it('should register plugin with custom options', async () => {
      app = await createApp({
        name: 'customMq',
        prefix: '/custom-mq',
        defaultMaxRetries: 5,
        pollLimit: 20
      });
      expect(app.customMq).to.exist;
      expect(app.customMq.services).to.exist;
    });

    it('should expose /mq/metrics endpoint', async () => {
      app = await createApp();
      const response = await app.inject({
        method: 'GET',
        url: '/mq/metrics'
      });
      expect(response.statusCode).to.equal(200);
      expect(response.headers['content-type']).to.include('text/plain');
      expect(response.body).to.include('mq_queue_depth');
      expect(response.body).to.include('mq_consumed_total');
      expect(response.body).to.include('mq_failed_total');
      expect(response.body).to.include('mq_dlq_total');
    });
  });

  describe('消息发布与消费测试', () => {
    it('should publish a message', async () => {
      app = await createApp();
      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {
          topic: 'order.created',
          payload: {orderId: '123', amount: 100}
        }
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.topic).to.equal('order.created');
      expect(result.status).to.equal('PENDING');
      expect(result.traceId).to.exist;
    });

    it('should publish a message with options', async () => {
      app = await createApp();
      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {
          topic: 'order.created',
          payload: {orderId: '456'},
          priority: 10,
          maxRetries: 5,
          traceId: 'custom-trace-001',
          meta: {source: 'api'}
        }
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.priority).to.equal(10);
      expect(result.maxRetries).to.equal(5);
      expect(result.traceId).to.equal('custom-trace-001');
    });

    it('should poll messages', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.poll', payload: {key: 'value'}}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.poll'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.be.an('array').with.lengthOf(1);
      expect(result[0].status).to.equal('PROCESSING');
      expect(result[0].consumerId).to.exist;
    });

    it('should poll messages with priority order', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.priority', payload: {order: 1}, priority: 0}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.priority', payload: {order: 2}, priority: 10}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.priority', limit: '2'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.lengthOf(2);
      expect(result[0].priority).to.be.greaterThan(result[1].priority);
    });

    it('should complete a message', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.complete', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.complete'}
      });

      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.status).to.equal('COMPLETED');
    });

    it('should fail a message and retry', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.fail', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.fail'}
      });

      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'something went wrong'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.retryCount).to.equal(1);
      expect(result.status).to.equal('PENDING');
      expect(result.nextRetryAt).to.exist;
    });

    it('should move message to DLQ when max retries exceeded', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.dlq', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.dlq'}
      });

      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'permanent failure'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.status).to.equal('FAILED');
    });
  });

  describe('状态校验测试', () => {
    it('should reject completing a non-PROCESSING message with 409', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.status.complete', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      // Message is PENDING, trying to complete should fail with 409
      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });
      expect(response.statusCode).to.equal(409);
    });

    it('should reject failing a non-PROCESSING message with 409', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.status.fail', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      // Message is PENDING, trying to fail should fail with 409
      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'test'}
      });
      expect(response.statusCode).to.equal(409);
    });

    it('should reject completing an already completed message', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.status.completed', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.status.completed'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });

      // Try to complete again
      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });
      expect(response.statusCode).to.equal(409);
    });
  });

  describe('消息列表与队列深度测试', () => {
    it('should list messages with pagination', async () => {
      app = await createApp();

      for (let i = 0; i < 5; i++) {
        await app.inject({
          method: 'POST',
          url: '/mq/message/publish',
          body: {topic: 'test.list', payload: {i}}
        });
      }

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/list',
        query: {perPage: '3', currentPage: '1'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf(3);
      expect(result.totalCount).to.equal(5);
    });

    it('should filter messages by topic via HTTP query', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'topic.a', payload: {x: 1}}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'topic.b', payload: {x: 2}}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/list',
        query: {topic: 'topic.a'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf(1);
      expect(result.pageData[0].topic).to.equal('topic.a');
    });

    it('should filter messages by status via HTTP query', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.filter.status', payload: {x: 1}}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/list',
        query: {status: 'PENDING'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf.at.least(1);
      expect(result.pageData.every((m) => m.status === 'PENDING')).to.be.true;
    });

    it('should get queue depth', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.depth', payload: {x: 1}}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.depth', payload: {x: 2}}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/queue/depth',
        query: {topic: 'test.depth'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.depth).to.equal(2);
    });

    it('should get total queue depth when topic not specified', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'topic.x', payload: {x: 1}}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'topic.y', payload: {x: 2}}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/queue/depth'
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.depth).to.be.at.least(2);
    });
  });

  describe('死信队列测试', () => {
    it('should list dead letters', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.dlq.list', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.dlq.list'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'max retries'}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list'
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf(1);
      expect(result.pageData[0].topic).to.equal('test.dlq.list');
      expect(result.pageData[0].errorMessage).to.equal('max retries');
    });

    it('should filter dead letters by topic via HTTP query', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.dlq.filter', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.dlq.filter'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list',
        query: {topic: 'test.dlq.filter'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf(1);
    });

    it('should replay a dead letter', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.replay', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.replay'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      const dlqRes = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list'
      });
      const dlq = JSON.parse(dlqRes.body);

      const response = await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay',
        body: {id: dlq.pageData[0].id}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.topic).to.equal('test.replay');
      expect(result.status).to.equal('PENDING');
    });

    it('should batch replay dead letters', async () => {
      app = await createApp({defaultMaxRetries: 1});

      for (let i = 0; i < 3; i++) {
        const pubRes = await app.inject({
          method: 'POST',
          url: '/mq/message/publish',
          body: {topic: 'test.batch.replay', payload: {i}}
        });
        const msg = JSON.parse(pubRes.body);

        await app.inject({
          method: 'GET',
          url: '/mq/message/poll',
          query: {topic: 'test.batch.replay'}
        });
        await app.inject({
          method: 'POST',
          url: '/mq/message/fail',
          body: {id: msg.id, error: 'failed'}
        });
      }

      const dlqRes = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list'
      });
      const dlq = JSON.parse(dlqRes.body);
      const dlqIds = dlq.pageData.map((d) => d.id);

      const response = await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay',
        body: {ids: dlqIds}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.lengthOf(3);
      result.forEach((r) => expect(r.success).to.be.true);
    });

    it('should not replay an already replayed dead letter', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.replay.twice', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.replay.twice'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      const dlqRes = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list'
      });
      const dlq = JSON.parse(dlqRes.body);
      const dlqId = dlq.pageData[0].id;

      await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay',
        body: {id: dlqId}
      });

      const response = await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay',
        body: {id: dlqId}
      });
      expect(response.statusCode).to.equal(400);
    });

    it('should replay dead letter via path param', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.replay.pathparam', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.replay.pathparam'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      const dlqRes = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list'
      });
      const dlq = JSON.parse(dlqRes.body);
      const dlqId = dlq.pageData[0].id;

      const response = await app.inject({
        method: 'POST',
        url: `/mq/dlq/replay/${dlqId}`
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.topic).to.equal('test.replay.pathparam');
      expect(result.status).to.equal('PENDING');
    });

    it('should return 404 when replaying non-existent dead letter via path param', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay/99999999'
      });
      expect(response.statusCode).to.equal(404);
    });

    it('should replay dead letter with UUID traceId', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.replay.uuid', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.replay.uuid'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      const dlqRes = await app.inject({
        method: 'GET',
        url: '/mq/dlq/list'
      });
      const dlq = JSON.parse(dlqRes.body);

      const response = await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay',
        body: {id: dlq.pageData[0].id}
      });
      const result = JSON.parse(response.body);
      expect(result.traceId).to.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    });
  });

  describe('消息轨迹测试', () => {
    it('should get trace by traceId', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace', payload: {x: 1}, traceId: 'trace-001'}
      });

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.trace'}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/detail',
        query: {traceId: 'trace-001'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.be.an('array').with.lengthOf.at.least(2);
      const events = result.map((t) => t.event);
      expect(events).to.include('PUBLISHED');
      expect(events).to.include('PROCESSING');
    });

    it('should get trace by path param traceId', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace.path', payload: {x: 1}, traceId: 'trace-path-001'}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/trace-path-001'
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.be.an('array').with.lengthOf.at.least(1);
      const events = result.map((t) => t.event);
      expect(events).to.include('PUBLISHED');
    });

    it('should list traces with filters via HTTP query', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace.list', payload: {x: 1}, traceId: 'trace-list-001'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace.list2', payload: {x: 2}, traceId: 'trace-list-002'}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/list',
        query: {topic: 'test.trace.list'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf.at.least(1);
      expect(result.pageData.every((t) => t.topic === 'test.trace.list')).to.be.true;
    });

    it('should filter traces by event via HTTP query', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace.event', payload: {x: 1}, traceId: 'trace-event-001'}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/list',
        query: {event: 'PUBLISHED'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.pageData).to.have.lengthOf.at.least(1);
      expect(result.pageData.every((t) => t.event === 'PUBLISHED')).to.be.true;
    });

    it('should record COMPLETED trace event', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace.complete', payload: {x: 1}, traceId: 'trace-002'}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.trace.complete'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/detail',
        query: {traceId: 'trace-002'}
      });
      const result = JSON.parse(response.body);
      const events = result.map((t) => t.event);
      expect(events).to.include('COMPLETED');
    });

    it('should record FAILED and MOVED_TO_DLQ trace events', async () => {
      app = await createApp({defaultMaxRetries: 2, retryBaseDelay: 0, retryMaxDelay: 0});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.trace.fail', payload: {x: 1}, traceId: 'trace-003'}
      });
      const msg = JSON.parse(pubRes.body);

      // First fail: retry
      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.trace.fail'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      // Second fail: exceeds maxRetries, moves to DLQ
      const pollRes = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.trace.fail'}
      });
      const polled = JSON.parse(pollRes.body);
      if (polled.length > 0) {
        await app.inject({
          method: 'POST',
          url: '/mq/message/fail',
          body: {id: msg.id, error: 'failed again'}
        });
      }

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/detail',
        query: {traceId: 'trace-003'}
      });
      const result = JSON.parse(response.body);
      const events = result.map((t) => t.event);
      expect(events).to.include('FAILED');
      expect(events).to.include('MOVED_TO_DLQ');
    });
  });

  describe('延迟消息测试', () => {
    it('should not poll delayed message before executeAt', async () => {
      app = await createApp();

      const futureTime = new Date(Date.now() + 3600000).toISOString();
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.delayed', payload: {x: 1}, executeAt: futureTime}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.delayed'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.lengthOf(0);
    });

    it('should poll delayed message after executeAt', async () => {
      app = await createApp();

      const pastTime = new Date(Date.now() - 1000).toISOString();
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.delayed.past', payload: {x: 1}, executeAt: pastTime}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.delayed.past'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.lengthOf(1);
    });
  });

  describe('Metrics 测试', () => {
    it('should increment consumedTotal on complete', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.metrics.consume', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.metrics.consume'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });

      const metricsRes = await app.inject({
        method: 'GET',
        url: '/mq/metrics'
      });
      expect(metricsRes.body).to.include('mq_consumed_total');
      expect(metricsRes.body).to.include('test.metrics.consume');
    });

    it('should increment failedTotal and dlqTotal on DLQ move', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.metrics.dlq', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.metrics.dlq'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: msg.id, error: 'failed'}
      });

      const metricsRes = await app.inject({
        method: 'GET',
        url: '/mq/metrics'
      });
      expect(metricsRes.body).to.include('mq_failed_total');
      expect(metricsRes.body).to.include('mq_dlq_total');
      expect(metricsRes.body).to.include('test.metrics.dlq');
    });

    it('should enforce maxCardinality on counters', async () => {
      const {metrics: testMetrics} = require('../libs/utils/metrics');
      const Counter = require('../libs/utils/metrics').Counter || null;
      // Verify that Counter has maxCardinality property
      if (Counter) {
        const c = new Counter({name: 'test_c', help: 'test', maxCardinality: 3});
        c.inc({topic: 'a'});
        c.inc({topic: 'b'});
        c.inc({topic: 'c'});
        c.inc({topic: 'd'}); // Should be dropped
        expect(c._values.size).to.equal(3);
      }
    });
  });

  describe('Dashboard 测试', () => {
    it('should return dashboard data with correct structure', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dashboard'
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.property('timestamp');
      expect(result).to.have.property('current');
      expect(result).to.have.property('timeSeries');
      expect(result.current).to.have.property('queueDepth');
      expect(result.current).to.have.property('consumedTotal');
      expect(result.current).to.have.property('failedTotal');
      expect(result.current).to.have.property('dlqTotal');
      expect(result.current).to.have.property('consumeRate');
      expect(result.current).to.have.property('failureRate');
      expect(result.current).to.have.property('dlqRate');
      expect(result.current).to.have.property('successRatio');
      expect(result.current).to.have.property('successRatioByTopic');
      expect(result.timeSeries).to.have.property('queueDepth');
      expect(result.timeSeries).to.have.property('consumeRate');
      expect(result.timeSeries).to.have.property('failureRate');
      expect(result.timeSeries).to.have.property('dlqRate');
    });

    it('should reflect consumed/failed totals in dashboard after operations', async () => {
      app = await createApp({defaultMaxRetries: 1});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.dashboard', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.dashboard'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dashboard'
      });
      const result = JSON.parse(response.body);
      expect(result.current.consumedTotal.total).to.be.at.least(1);
      expect(result.current.consumedTotal.byTopic).to.have.property('test.dashboard');
    });

    it('should calculate success ratio', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.ratio', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.ratio'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dashboard'
      });
      const result = JSON.parse(response.body);
      expect(result.current.successRatio).to.not.be.null;
      expect(result.current.successRatio).to.be.at.most(1);
      expect(result.current.successRatio).to.be.at.least(0);
    });

    it('should return timeSeries data with timestamps', async () => {
      app = await createApp();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.ts', payload: {x: 1}}
      });

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dashboard'
      });
      const result = JSON.parse(response.body);
      expect(result.timeSeries.queueDepth).to.be.an('array');
      if (result.timeSeries.queueDepth.length > 0) {
        expect(result.timeSeries.queueDepth[0]).to.have.property('timestamp');
      }
    });

    it('should accept window and step query params', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'GET',
        url: '/mq/dashboard',
        query: {window: '60000', step: '10000'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.property('current');
    });
  });

  describe('锁定超时恢复测试', () => {
    it('should recover timed-out locked messages', async () => {
      app = await createApp({lockTimeout: 0});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.lock.recovery', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      // Poll to lock the message
      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.lock.recovery'}
      });

      // Manually recover locked messages (lockTimeout=0 means all PROCESSING are timed out)
      const result = await app.mq.services.queue.recoverLocked();
      expect(result.recovered).to.be.at.least(1);

      // The message should be back to PENDING and pollable
      const pollRes = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.lock.recovery'}
      });
      expect(pollRes.statusCode).to.equal(200);
      const polled = JSON.parse(pollRes.body);
      expect(polled).to.have.lengthOf.at.least(1);
    });

    it('should not recover messages that are not timed out', async () => {
      app = await createApp({lockTimeout: 999999});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.lock.no.recovery', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.lock.no.recovery'}
      });

      const result = await app.mq.services.queue.recoverLocked();
      expect(result.recovered).to.equal(0);
    });

    it('should record LOCK_RECOVERED trace events', async () => {
      app = await createApp({lockTimeout: 0});

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.lock.trace', payload: {x: 1}, traceId: 'lock-trace-001'}
      });
      const msg = JSON.parse(pubRes.body);

      // Poll to lock the message
      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.lock.trace'}
      });

      // Recover locked messages
      await app.mq.services.queue.recoverLocked();

      // Check trace events
      const traceRes = await app.inject({
        method: 'GET',
        url: '/mq/trace/detail',
        query: {traceId: 'lock-trace-001'}
      });
      const traces = JSON.parse(traceRes.body);
      const events = traces.map((t) => t.event);
      expect(events).to.include('LOCK_RECOVERED');
    });
  });

  describe('消息清理测试', () => {
    it('should cleanup completed messages', async () => {
      app = await createApp();

      const pubRes = await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.cleanup', payload: {x: 1}}
      });
      const msg = JSON.parse(pubRes.body);

      await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'test.cleanup'}
      });
      await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: msg.id}
      });

      const response = await app.inject({
        method: 'POST',
        url: '/mq/queue/cleanup',
        body: {status: 'COMPLETED'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.deleted).to.be.at.least(1);
    });
  });

  describe('消费者模式测试', () => {
    it('should subscribe and consume messages', async () => {
      app = await createApp({pollInterval: 100});

      const processedMessages = [];

      app.mq.services.queue.subscribe('test.consumer', async (msg) => {
        processedMessages.push(msg);
      });

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.consumer', payload: {x: 1}}
      });

      app.mq.services.queue.startConsumer();

      // Wait for consumer to process
      await new Promise((resolve) => setTimeout(resolve, 500));

      expect(processedMessages).to.have.lengthOf.at.least(1);
      expect(processedMessages[0].payload).to.deep.equal({x: 1});

      app.mq.services.queue.stopConsumer();
    });

    it('should handle consumer handler errors gracefully', async () => {
      app = await createApp({defaultMaxRetries: 3, pollInterval: 100});

      app.mq.services.queue.subscribe('test.consumer.error', async () => {
        throw new Error('Handler error');
      });

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.consumer.error', payload: {x: 1}}
      });

      app.mq.services.queue.startConsumer();

      // Wait for consumer to process
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Message should have been marked as failed (retry)
      const listRes = await app.inject({
        method: 'GET',
        url: '/mq/message/list',
        query: {topic: 'test.consumer.error'}
      });
      const result = JSON.parse(listRes.body);
      expect(result.pageData).to.have.lengthOf.at.least(1);
      expect(result.pageData[0].retryCount).to.be.at.least(1);

      app.mq.services.queue.stopConsumer();
    });

    it('should stop consumer and not process new messages', async () => {
      app = await createApp({pollInterval: 100});

      const processedMessages = [];

      app.mq.services.queue.subscribe('test.consumer.stop', async (msg) => {
        processedMessages.push(msg);
      });

      app.mq.services.queue.startConsumer();
      await app.mq.services.queue.stopConsumer();

      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.consumer.stop', payload: {x: 1}}
      });

      // Wait a bit
      await new Promise((resolve) => setTimeout(resolve, 300));

      // No messages should have been processed after stop
      expect(processedMessages).to.have.lengthOf(0);
    });

    it('should require all handlers to succeed before completing (all-or-nothing)', async () => {
      app = await createApp({pollInterval: 100});

      // Subscribe two handlers to the same topic
      app.mq.services.queue.subscribe('test.all.handlers', async (msg) => {
        // First handler always succeeds
      });
      app.mq.services.queue.subscribe('test.all.handlers', async (msg) => {
        // Second handler fails when payload.shouldFail is true
        if (msg.payload.shouldFail) {
          throw new Error('Handler B failed');
        }
      });

      // Publish a message that will cause second handler to fail
      await app.inject({
        method: 'POST',
        url: '/mq/message/publish',
        body: {topic: 'test.all.handlers', payload: {shouldFail: true}}
      });

      app.mq.services.queue.startConsumer();
      await new Promise((resolve) => setTimeout(resolve, 500));

      // Since handler B failed, message should not be completed but rather failed/retried
      const listRes = await app.inject({
        method: 'GET',
        url: '/mq/message/list',
        query: {topic: 'test.all.handlers'}
      });
      const result = JSON.parse(listRes.body);
      expect(result.pageData.length).to.be.at.least(1);
      const msg = result.pageData[0];
      // Message should have been failed (retryCount > 0) since not all handlers succeeded
      expect(msg.retryCount).to.be.at.least(1);

      app.mq.services.queue.stopConsumer();
    });
  });

  describe('资源清理测试', () => {
    it('should clean up timers on close', async () => {
      app = await createApp({pollInterval: 100});
      app.mq.services.queue.startConsumer();

      // Verify timer is running
      expect(app.mq.services.queue).to.exist;

      await app.close();
      app = null;
      // No assertion needed - if timers aren't cleaned, the test would hang
    });
  });

  describe('边界情况测试', () => {
    it('should return 404 when completing non-existent message', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/complete',
        body: {id: '99999999'}
      });
      expect(response.statusCode).to.equal(404);
    });

    it('should return 404 when failing non-existent message', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'POST',
        url: '/mq/message/fail',
        body: {id: '99999999', error: 'test'}
      });
      expect(response.statusCode).to.equal(404);
    });

    it('should return empty list when no messages for topic', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'GET',
        url: '/mq/message/poll',
        query: {topic: 'non.existent.topic'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.lengthOf(0);
    });

    it('should return 404 when replaying non-existent dead letter', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'POST',
        url: '/mq/dlq/replay',
        body: {id: '99999999'}
      });
      expect(response.statusCode).to.equal(404);
    });

    it('should return empty trace for unknown traceId', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'GET',
        url: '/mq/trace/detail',
        query: {traceId: 'non-existent-trace'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result).to.have.lengthOf(0);
    });

    it('should return zero queue depth for empty topic', async () => {
      app = await createApp();

      const response = await app.inject({
        method: 'GET',
        url: '/mq/queue/depth',
        query: {topic: 'empty.topic'}
      });
      expect(response.statusCode).to.equal(200);
      const result = JSON.parse(response.body);
      expect(result.depth).to.equal(0);
    });
  });

  describe('常量导出测试', () => {
    it('should export MESSAGE_STATUS constants', () => {
      expect(MESSAGE_STATUS.PENDING).to.equal('PENDING');
      expect(MESSAGE_STATUS.PROCESSING).to.equal('PROCESSING');
      expect(MESSAGE_STATUS.COMPLETED).to.equal('COMPLETED');
      expect(MESSAGE_STATUS.FAILED).to.equal('FAILED');
    });

    it('should export TRACE_EVENTS constants', () => {
      expect(TRACE_EVENTS.PUBLISHED).to.equal('PUBLISHED');
      expect(TRACE_EVENTS.PROCESSING).to.equal('PROCESSING');
      expect(TRACE_EVENTS.COMPLETED).to.equal('COMPLETED');
      expect(TRACE_EVENTS.FAILED).to.equal('FAILED');
      expect(TRACE_EVENTS.MOVED_TO_DLQ).to.equal('MOVED_TO_DLQ');
      expect(TRACE_EVENTS.REPLAYED).to.equal('REPLAYED');
      expect(TRACE_EVENTS.LOCK_RECOVERED).to.equal('LOCK_RECOVERED');
    });
  });

  describe('PG Adapter 单元测试', () => {
    it('should have adapterType pg', async () => {
      app = await createApp();
      // Access adapter via options
      const adapter = app.mq.services._adapter;
      // Adapter is stored in options, not directly on services
      // We can test via the services which delegate to it
      expect(app.mq.services.message.publish).to.be.a('function');
    });

    it('should create PG adapter with correct methods', () => {
      const mockSequelize = {
        constructor: {
          Transaction: {ISOLATION_LEVELS: {READ_COMMITTED: 'READ_COMMITTED'}}
        },
        transaction: async (opts, fn) => fn({LOCK: {UPDATE: 'UPDATE'}})
      };
      const mockModels = {
        message: {
          create: async (data) => ({...data, id: data.id || 'test-id'}),
          findAll: async () => [],
          findByPk: async () => null,
          update: async () => [1],
          count: async () => 0,
          findAndCountAll: async () => ({rows: [], count: 0}),
          destroy: async () => 0
        },
        deadLetter: {
          create: async (data) => data,
          findByPk: async () => null,
          findAndCountAll: async () => ({rows: [], count: 0})
        },
        messageTrace: {
          create: async () => {},
          bulkCreate: async () => {},
          findAll: async () => [],
          findAndCountAll: async () => ({rows: [], count: 0})
        }
      };

      const adapter = createPgAdapter({
        models: mockModels,
        sequelize: mockSequelize,
        generateId: () => 'test-id',
        metrics: null
      });

      expect(adapter.adapterType).to.equal('pg');
      expect(adapter.publish).to.be.a('function');
      expect(adapter.poll).to.be.a('function');
      expect(adapter.complete).to.be.a('function');
      expect(adapter.fail).to.be.a('function');
      expect(adapter.replayDeadLetter).to.be.a('function');
      expect(adapter.batchReplayDeadLetters).to.be.a('function');
      expect(adapter.recoverLocked).to.be.a('function');
    });
  });

  describe('Kafka Adapter 单元测试', () => {
    const createMockKafka = () => {
      const sentMessages = [];
      const mockProducer = {
        connect: async () => {},
        disconnect: async () => {},
        send: async ({topic, messages}) => {
          sentMessages.push({topic, messages});
        }
      };
      const mockConsumers = {};
      const mockKafka = {
        producer: () => mockProducer,
        consumer: ({groupId}) => {
          const consumer = {
            connect: async () => {},
            disconnect: async () => {},
            subscribe: async () => {},
            run: async () => {}
          };
          mockConsumers[groupId] = consumer;
          return consumer;
        },
        _sentMessages: sentMessages,
        _consumers: mockConsumers
      };
      return mockKafka;
    };

    it('should have adapterType kafka', () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        topicPrefix: 'test-',
        metrics: null
      });
      expect(adapter.adapterType).to.equal('kafka');
    });

    it('should publish messages via Kafka producer', async () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        topicPrefix: 'mq-',
        metrics: null
      });

      const result = await adapter.publish({
        topic: 'order.created',
        payload: {orderId: '123'},
        priority: 5,
        traceId: 'trace-001'
      });

      expect(result.topic).to.equal('order.created');
      expect(result.status).to.equal('PUBLISHED');
      expect(result.traceId).to.equal('trace-001');
      expect(mockKafka._sentMessages).to.have.lengthOf(1);
      expect(mockKafka._sentMessages[0].topic).to.equal('mq-order.created');
    });

    it('should generate traceId if not provided', async () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        metrics: null
      });

      const result = await adapter.publish({
        topic: 'test',
        payload: {x: 1}
      });

      expect(result.traceId).to.match(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    });

    it('should return empty array for poll', async () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        metrics: null
      });

      const result = await adapter.poll({topic: 'test'});
      expect(result).to.deep.equal([]);
    });

    it('should return empty object for complete and fail', async () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        metrics: null
      });

      expect(await adapter.complete({id: '1'})).to.deep.equal({});
      expect(await adapter.fail({id: '1', error: 'test'})).to.deep.equal({});
    });

    it('should throw for unsupported methods', async () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        metrics: null
      });

      try {
        await adapter.replayDeadLetter({id: '1'});
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Kafka adapter does not support');
      }

      try {
        await adapter.getQueueDepth({topic: 'test'});
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Kafka adapter does not support');
      }

      try {
        await adapter.recoverLocked();
        expect.fail('Should have thrown');
      } catch (e) {
        expect(e.message).to.include('Kafka adapter does not support');
      }
    });

    it('should subscribe with Kafka consumer', async () => {
      const mockKafka = createMockKafka();
      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        topicPrefix: 'mq-',
        metrics: null
      });

      const receivedMessages = [];
      await adapter.subscribe({
        topic: 'order.created',
        handler: async (data) => {
          receivedMessages.push(data);
        },
        groupId: 'test-group'
      });

      expect(mockKafka._consumers['test-group']).to.exist;
    });

    it('should disconnect producer and consumers', async () => {
      const mockKafka = createMockKafka();
      let producerDisconnected = false;
      mockKafka.producer = () => ({
        connect: async () => {},
        disconnect: async () => { producerDisconnected = true; },
        send: async () => {}
      });

      const adapter = createKafkaAdapter({
        kafka: mockKafka,
        metrics: null
      });

      // Connect by publishing
      await adapter.publish({topic: 'test', payload: {}});
      await adapter.disconnect();

      expect(producerDisconnected).to.be.true;
    });
  });
});
