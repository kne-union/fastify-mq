module.exports = ({ DataTypes, options }) => {
  return {
    name: 'deadLetter',
    model: {
      originalId: {
        type: DataTypes.STRING,
        allowNull: false,
        comment: '原始消息ID'
      },
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
      errorMessage: {
        type: DataTypes.TEXT,
        comment: '错误信息'
      },
      replayed: {
        type: DataTypes.BOOLEAN,
        allowNull: false,
        defaultValue: false,
        comment: '是否已重放'
      },
      replayedAt: {
        type: DataTypes.DATE,
        comment: '重放时间'
      },
      options: {
        type: DataTypes.JSONB,
        comment: '扩展字段'
      }
    },
    options: {
      comment: 'MQ死信表',
      indexes: [{ fields: ['original_id'] }, { fields: ['topic'] }, { fields: ['replayed'] }, { fields: ['created_at'] }]
    }
  };
};
