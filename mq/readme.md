# 基于Mysql设计消息队列

### 主要模块
- Producer (生产者)
- Broker
  - Topic
  - Partition
- Consumer
  - Consumer Group

### 功能
- Producer
  - 批量异步发送
  - 数据压缩
- Consumer
  - 异步提交
- 支持延时消息

## 详细设计
- topic对应库, partition对应表
- leader partition 应该尽量分散不同的数据库上

### Producer
- 推模式，进一步优化，拉取元信息本地存储，直连Mysql
- 异步批量发送/定期刷
- 消息压缩
- 消息Ack/重试

### Broker
- 元信息存储，Broker和topic = 数据库和topic、partition的关系
- 消息存储
- 消息读取

### Consumer
- 异步提交
- 进一步优化，拉取元信息本地存储，直连Mysql

### 表设计
```
# 元信息存储 broker和topic关系
CREATE TABLE `tbl_meta` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topic` varchar(20) not null,
  `partition` int(10) NOT NULL,
  `node` varchar(20) NOT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


# 消息存储 partition
CREATE TABLE `tbl_{topic}_{partition}` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `topic` varchar(20) not null,
  `partition` int(10) NOT NULL,
  `msg` varchar(10000) NOT NULL,
  `send_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


# 消费者 offset 管理表 
CREATE TABLE `tbl_consumer_offset` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group` varchar(100) NOT NULL,
  `partition` int(10) NOT NULL,
  `offset` bigint(20) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `group` (`group`,`partition`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

```