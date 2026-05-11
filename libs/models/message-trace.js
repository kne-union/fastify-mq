module.exports = ({ DataTypes, options }) => {
  return {
    name: 'messageTrace',
    model: {
      traceId: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '追踪ID'
      },
      topic: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '消息主题'
      },
      event: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '事件类型: PUBLISHED/PROCESSING/COMPLETED/FAILED/MOVED_TO_DLQ/REPLAYED'
      },
      messageId: {
        type: DataTypes.STRING,
        comment: '关联消息ID'
      },
      detail: {
        type: DataTypes.JSONB,
        comment: '事件详情'
      },
      options: {
        type: DataTypes.JSONB,
        comment: '扩展字段'
      }
    },
    associate: ({ messageTrace }) => {
      messageTrace.belongsTo(options.getMessageModel(), {
        foreignKey: 'messageId',
        as: 'message'
      });
    },
    options: {
      comment: 'MQ消息轨迹表',
      indexes: [{ fields: ['trace_id', 'created_at'] }, { fields: ['message_id'] }, { fields: ['topic'] }]
    }
  };
};
