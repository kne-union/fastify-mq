// 响应 schema 只用于 OpenAPI 文档：字段可空、对象允许额外字段，避免序列化改变实际输出
const str = description => ({ type: 'string', nullable: true, description });
const num = description => ({ type: 'number', nullable: true, description });
const bool = description => ({ type: 'boolean', nullable: true, description });
// ID（BIGINT）、日期、JSON 列可能是 number / string / Date / 任意结构，不声明类型，原样输出
const any = description => ({ description });
const obj = (description, properties = {}) => ({ type: 'object', nullable: true, additionalProperties: true, description, properties });
const list = (description, items) => ({ type: 'array', nullable: true, description, items });

const success = (description, schema) => ({ 200: Object.assign({}, schema || { type: 'object', additionalProperties: true }, { description }) });

const pageResult = (description, item) => ({
  type: 'object',
  additionalProperties: true,
  properties: {
    pageData: list(`当前页${description}列表，按创建时间倒序`, item),
    totalCount: num('符合条件的总条数')
  }
});

const MESSAGE_STATUS = '消息状态：PENDING 待消费，PROCESSING 消费中，COMPLETED 已完成，FAILED 已失败（已进入死信）';
const TRACE_EVENT = '事件类型：PUBLISHED 已发布，PROCESSING 开始消费，COMPLETED 已完成，FAILED 失败待重试，MOVED_TO_DLQ 进入死信，REPLAYED 死信重放，LOCK_RECOVERED 锁超时恢复';

const timestamps = {
  createdAt: any('创建时间'),
  updatedAt: any('更新时间')
};

const message = obj('消息', {
  id: any('消息 ID'),
  topic: str('消息主题'),
  payload: any('消息内容'),
  status: str(MESSAGE_STATUS),
  retryCount: num('已重试次数'),
  maxRetries: num('最大重试次数'),
  priority: num('优先级，数值越大越先被消费'),
  executeAt: any('定时执行时间，未设置为 null'),
  nextRetryAt: any('下次重试时间'),
  consumerId: str('当前持有消息的消费者标识'),
  lockedAt: any('锁定时间'),
  traceId: str('追踪 ID'),
  options: any('扩展元数据（发布时的 meta）'),
  ...timestamps
});

const deadLetter = obj('死信', {
  id: any('死信 ID'),
  originalId: any('原始消息 ID'),
  topic: str('消息主题'),
  payload: any('消息内容'),
  errorMessage: str('最后一次失败的错误信息'),
  replayed: bool('是否已重放'),
  replayedAt: any('重放时间'),
  options: any('扩展字段'),
  ...timestamps
});

const trace = obj('轨迹事件', {
  id: any('轨迹 ID'),
  traceId: str('追踪 ID'),
  topic: str('消息主题'),
  event: str(TRACE_EVENT),
  messageId: any('关联消息 ID'),
  detail: any('事件详情，如重试次数、错误信息、消费者标识'),
  options: any('扩展字段'),
  ...timestamps
});

module.exports = { str, num, bool, any, obj, list, success, pageResult, MESSAGE_STATUS, TRACE_EVENT, message, deadLetter, trace };
