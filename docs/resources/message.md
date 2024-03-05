# 消息 (Message)

为了使用的方便, Yutori 将 Message 实体类的 content 属性由 String 类型替换为 MessageSegment 类型

## 动作 (Action)

### 发送消息

> message.create

### 获取消息

> message.get

### 撤回消息

> message.delete

### 编辑消息

> message.update

### 获取消息列表

> message.list

## 事件 (Event)

### 消息被创建

> message.created

### 消息被编辑

> message.updated

### 消息被删除

> message.deleted