const { MESSAGE_STATUS } = require('../constants');

module.exports = ({ DataTypes, options }) => {
  return {
    model: {
      topic: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '消息主题'
      },
      payload: {
        type: DataTypes.JSONB,
        allowNull: false,
        comment: '消息内容'
      },
      status: {
        type: DataTypes.STRING,
        allowNull: false,
        defaultValue: MESSAGE_STATUS.PENDING,
        comment: `消息状态: ${Object.values(MESSAGE_STATUS).join('/')}`
      },
      retryCount: {
        type: DataTypes.INTEGER,
        allowNull: false,
        defaultValue: 0,
        comment: '已重试次数'
      },
      maxRetries: {
        type: DataTypes.INTEGER,
        allowNull: false,
        defaultValue: 3,
        comment: '最大重试次数'
      },
      priority: {
        type: DataTypes.INTEGER,
        allowNull: false,
        defaultValue: 0,
        comment: '优先级, 数值越大优先级越高'
      },
      executeAt: {
        type: DataTypes.DATE,
        comment: '定时执行时间'
      },
      nextRetryAt: {
        type: DataTypes.DATE,
        comment: '下次重试时间'
      },
      consumerId: {
        type: DataTypes.STRING,
        comment: '当前消费者标识'
      },
      lockedAt: {
        type: DataTypes.DATE,
        comment: '锁定时间'
      },
      traceId: {
        type: DataTypes.STRING,
        comment: '追踪ID'
      },
      options: {
        type: DataTypes.JSONB,
        comment: '扩展字段'
      }
    },
    associate: ({ message, messageTrace }) => {
      message.hasMany(messageTrace, {
        foreignKey: 'messageId',
        as: 'traces',
        onDelete: 'CASCADE'
      });
    },
    options: {
      comment: 'MQ消息表',
      indexes: [
        { fields: ['topic', 'status', 'priority', 'created_at'] },
        { fields: ['topic', 'status', 'execute_at', 'next_retry_at', 'priority', 'created_at'] },
        { fields: ['trace_id'] },
        { fields: ['consumer_id'] },
        { fields: ['status', 'next_retry_at'] }
      ]
    }
  };
};
