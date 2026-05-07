# Trending

面向推荐系统的热度召回服务：根据用户行为计算创作热度，并将结果写入 KVrocks（Redis 协议，ZSet），供召回层读取。

## 业务架构

- **输入数据**
  - 创作流（Creation）：创作的基础信息（creationId、userId、category、status），来自 Kafka。仅 status=0 的创作为有效创作。
  - 行为流（ActionBatch）：用户行为事件（actionId、actionType、userId、timestamp、creationList），来自 Kafka。creationList 为逗号分隔的 creationId。
- **核心处理**
  - **创作状态管理**：以 creationId 为 key 将创作信息写入 Flink State，仅保留 status=0 的创作，供后续行为计算时补全分类信息。
  - **行为拆分**：将 ActionBatch 中的 creationList 拆分成单条 Action。
  - **热度计算**：对单条行为按权重累加，并按半衰期进行指数衰减。
  - **窗口聚合**：以处理时间窗口计算窗口末端的衰减热度，稳定输出。
  - **Top-K 聚合**：按类目和全局分别提取 Top100，减少 Redis 写入频率。
- **输出结果**
  - 写入 KVrocks ZSet：`trend:{category}` 按类目排行、`trend:all` 全局排行，member 为 creationId，score 为热度分数；保留 Top100。

### 主要功能模块

- **数据接入**：Kafka Source（Creation 与 ActionBatch）。
- **状态管理**：Creation state（KeyedProcessFunction），仅保留 status=0 的创作。
- **热度计算**：Action 权重 + 指数衰减。
- **窗口化**：Tumbling Processing Time Window + window end 衰减。
- **Top-K 聚合**：按类目和全局分别提取 Top100，每窗口每类目仅批量写入一次 Redis。
- **热度写入**：KVrocks ZSet（Redis 协议，`zadd` + `zremrangebyrank`）。

## 技术架构

### 技术栈

- **计算框架**：Apache Flink 2.x（DataStream）
- **消息队列**：Kafka 4.0（KRaft）
- **存储**：KVrocks（Redis 协议）
- **序列化**：fastjson2
- **运行环境**：Java 21、Maven

### 核心实现说明

- **入口与配置**
  - `src/main/java/violet/trending/flink/JobMain.java`
  - 读取环境变量并构建 `TrendingJobOptions`：
    - `KAFKA_BOOTSTRAP_SERVERS`
    - `CREATION_TOPICS`
    - `ACTION_TOPICS`
    - `CREATION_GROUP_ID`
    - `ACTION_GROUP_ID`
    - `WINDOW_SIZE`（处理时间窗口）
    - `CALCULATOR_HALF_LIFE`（热度衰减半衰期）
    - `WINDOW_DECAY_HALF_LIFE`（窗口末端衰减半衰期）
    - `TRENDING_REDIS_URI`
- **Kafka 读取**
  - `src/main/java/violet/trending/flink/connectors/kafka/KafkaSourceFactory.java`
  - 统一处理 offset 策略、分区发现、bounded/unbounded 语义。
- **行为拆分**
  - `src/main/java/violet/trending/flink/processing/functions/ActionBatchSplitter.java`
  - 将 ActionBatch 中的 creationList（逗号分隔）拆成 Action 流，并做异常保护。
- **热度计算**
  - `src/main/java/violet/trending/flink/processing/processors/TrendingCalculator.java`
  - Action 权重：点击=1、点赞=2、评论=3、转发=3；按半衰期进行指数衰减。
  - 基于 `creationId` 进行 KeyedProcess，读取 creation state 补全 category。
- **窗口衰减**
  - `src/main/java/violet/trending/flink/processing/aggregators/TrendingWindowAggregator.java`
  - 每个窗口保留最后一次行为的热度结果，在窗口结束时对 score 进行衰减再输出。
- **Top-K 聚合**
  - `src/main/java/violet/trending/flink/processing/aggregators/CategoryTopKAggregator.java`
  - 按类目和全局（key="all"）分别提取 Top100，每窗口每类目一批。
- **KVrocks 写入**
  - `src/main/java/violet/trending/flink/connectors/redis/RedisHotRankingSink.java`
  - `trend:{category}` 和 `trend:all` ZSet，批量 zadd + 单次 trim，保留 Top100。

### 数据流

1. Kafka Creation -> Flink State (creationId -> Creation, status=0 only)
2. Kafka ActionBatch -> 拆分为 Action
3. Action + Creation State -> 计算热度 (指数衰减)
4. Window 聚合 -> 窗口末端衰减
5. Top-K 聚合 -> 按类目 Top100 + 全局 Top100
6. KVrocks `trend:{category}` / `trend:all` -> 推荐系统热度召回
