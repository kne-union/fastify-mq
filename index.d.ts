declare module '@kne/fastify-mq' {
  interface MQPublishOptions {
    topic: string;
    payload: Record<string, unknown>;
    priority?: number;
    executeAt?: string | Date;
    maxRetries?: number;
    traceId?: string;
    meta?: Record<string, unknown>;
  }

  interface MQPollOptions {
    topic: string;
    limit?: number;
    lockTimeout?: number;
  }

  interface MQMessage {
    id: string;
    topic: string;
    payload: Record<string, unknown>;
    status: 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED';
    retryCount: number;
    maxRetries: number;
    priority: number;
    executeAt: Date | null;
    nextRetryAt: Date | null;
    consumerId: string | null;
    lockedAt: Date | null;
    traceId: string;
    options: Record<string, unknown>;
    createdAt: Date;
    updatedAt: Date;
  }

  interface MQDeadLetter {
    id: string;
    originalId: string;
    topic: string;
    payload: Record<string, unknown>;
    errorMessage: string | null;
    replayed: boolean;
    replayedAt: Date | null;
    createdAt: Date;
    updatedAt: Date;
  }

  interface MQTrace {
    id: string;
    traceId: string;
    topic: string;
    event: string;
    detail: Record<string, unknown> | null;
    messageId: string | null;
    createdAt: Date;
  }

  interface MQPaginatedResult<T> {
    pageData: T[];
    totalCount: number;
  }

  interface MQMessageService {
    publish(params: MQPublishOptions): Promise<MQMessage>;
    poll(params: MQPollOptions): Promise<MQMessage[]>;
    complete(params: { id: string }): Promise<MQMessage>;
    fail(params: { id: string; error?: string }): Promise<MQMessage>;
    list(params: {
      filter?: { topic?: string; status?: string; traceId?: string };
      perPage?: number;
      currentPage?: number;
    }): Promise<MQPaginatedResult<MQMessage>>;
  }

  interface MQDeadLetterService {
    replay(params: { id: string }): Promise<MQMessage>;
    batchReplay(params: { ids: string[] }): Promise<Array<{ id: string; success: boolean; messageId?: string; error?: string }>>;
    list(params: {
      filter?: { topic?: string; replayed?: boolean };
      perPage?: number;
      currentPage?: number;
    }): Promise<MQPaginatedResult<MQDeadLetter>>;
  }

  interface MQTraceService {
    get(params: { traceId: string }): Promise<MQTrace[]>;
    list(params: {
      filter?: { topic?: string; messageId?: string; event?: string };
      perPage?: number;
      currentPage?: number;
    }): Promise<MQPaginatedResult<MQTrace>>;
  }

  interface MQQueueService {
    getDepth(params: { topic?: string }): Promise<{ depth: number }>;
    recoverLocked(): Promise<{ recovered: number }>;
    cleanup(params?: { status?: string; olderThan?: string | Date }): Promise<{ deleted: number }>;
    subscribe(topic: string, handler: (msg: MQMessage) => Promise<void> | void): void;
    startConsumer(): void;
    stopConsumer(): Promise<void>;
  }

  interface MQDashboardService {
    getData(params?: { windowMs?: number; stepMs?: number }): Promise<{
      timestamp: number;
      current: {
        queueDepth: { byTopic: Record<string, number>; total: number };
        consumedTotal: { byTopic: Record<string, number>; total: number };
        failedTotal: { byTopic: Record<string, number>; total: number };
        dlqTotal: { byTopic: Record<string, number>; total: number };
        consumeRate: { byTopic: Record<string, number>; total: number };
        failureRate: { byTopic: Record<string, number>; total: number };
        dlqRate: { byTopic: Record<string, number>; total: number };
        successRatio: number | null;
        successRatioByTopic: Record<string, number | null>;
      };
      timeSeries: {
        queueDepth: Array<{ timestamp: number } & Record<string, number>>;
        consumeRate: Array<{ timestamp: number } & Record<string, number>>;
        failureRate: Array<{ timestamp: number } & Record<string, number>>;
        dlqRate: Array<{ timestamp: number } & Record<string, number>>;
      };
    }>;
  }

  interface MQServices {
    message: MQMessageService;
    deadLetter: MQDeadLetterService;
    trace: MQTraceService;
    queue: MQQueueService;
    dashboard: MQDashboardService;
  }

  interface MQPluginOptions {
    name?: string;
    prefix?: string;
    dbTableNamePrefix?: string;
    defaultMaxRetries?: number;
    pollLimit?: number;
    pollInterval?: number;
    lockTimeout?: number;
    lockRecoveryInterval?: number;
    retryBaseDelay?: number;
    retryMaxDelay?: number;
    metricsSampleInterval?: number;
    metricsMaxSamples?: number;
    adapter?: 'pg' | 'kafka';
    kafka?: unknown;
    topicPrefix?: string;
    kafkaGroupId?: string;
    getAuthenticate?: (type: string) => Array<unknown>;
    getMessageModel?: () => unknown;
  }

  interface MQNamespace {
    services: MQServices;
    models: {
      message: unknown;
      deadLetter: unknown;
      messageTrace: unknown;
    };
    controllers: {
      message: unknown;
      deadLetter: unknown;
      dashboard: unknown;
    };
  }

  type FastifyPluginCallback<Options> = (
    instance: unknown,
    opts: Options,
    done: (err?: Error) => void
  ) => void;

  const fastifyMQ: FastifyPluginCallback<MQPluginOptions>;
  export default fastifyMQ;
  export { fastifyMQ, MQPluginOptions, MQServices, MQNamespace, MQMessage, MQDeadLetter, MQTrace };
}

declare module 'fastify' {
  interface FastifyInstance {
    mq: import('@kne/fastify-mq').MQNamespace;
  }
}
