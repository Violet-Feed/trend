# Trending

面向推荐系统的热度召回服务：根据用户行为计算创作热度，并将结果写入 KVrocks（Redis 协议，ZSet），供召回层读取。

## 业务架构

- **输入数据**
  - 创作流（Creation）：creationId、userId、category、status，来自 Kafka。
  - 行为流（ActionBatch）：actionType、userId、timestamp、creationList，来自 Kafka。creationList 为逗号分隔的 creationId。
- **核心处理**
  - **双流 Join**：Creation 和 Action 通过 creationId 做 connect + keyBy，在一个算子内完成状态管理和热度计算。
  - **创作状态管理**：status=0 的创作写入 Flink ValueState；status≠0 时清除所有状态并向下游发送移除信号。
  - **行为拆分**：将 ActionBatch 中的 creationList 拆分成单条 Action。
  - **热度计算**：对单条行为按权重叠加，按半衰期指数衰减旧分数。
  - **类目排行管理**：每个类目维护 MapState（creationId→热度），定时器周期性对全部创作统一衰减、淘汰低分，输出 Top100。
- **输出结果**
  - 写入 KVrocks ZSet：`trend:{category}` 按类目排行、`trend:all` 全局排行，member 为 creationId，score 为热度分数；通过 RENAME 原子替换，保证读取不空窗。

### 主要功能模块

- **数据接入**：Kafka Source（Creation 与 ActionBatch）。
- **双流 Join + 热度计算**：TrendingCalculator（KeyedCoProcessFunction），通过 connect + keyBy 接收 Creation 和 Action 双流；status=0 时存 state 供后续使用，status≠0 时清除 state 并发移除信号；Action 到达时查 state 计算热度。
- **行为拆分**：ActionBatchSplitter 将 creationList 拆成单条 Action。
- **类目排行管理**：CategoryRankingManager 用 MapState 记录所有活跃创作，定时器周期触发统一衰减并输出 Top100，收到移除信号时从 MapState 删除。
- **热度写入**：RedisHotRankingSink，ZADD 到临时 key 后 RENAME 原子替换。

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
    - `WINDOW_SIZE`（定时器间隔）
    - `CALCULATOR_HALF_LIFE`（热度衰减半衰期）
    - `WINDOW_DECAY_HALF_LIFE`（排行榜衰减半衰期）
    - `TRENDING_REDIS_URI`
- **Kafka 读取**
  - `src/main/java/violet/trending/flink/connectors/kafka/KafkaSourceFactory.java`
  - 统一处理 offset 策略、分区发现、bounded/unbounded 语义。
- **行为拆分**
  - `src/main/java/violet/trending/flink/processing/functions/ActionBatchSplitter.java`
  - 将 ActionBatch 中的 creationList（逗号分隔）拆成 Action 流，并做异常保护。
- **双流 Join + 热度计算**
  - `src/main/java/violet/trending/flink/processing/processors/TrendingCalculator.java`
  - 继承 `KeyedCoProcessFunction<Long, Creation, Action, TrendingResult>`，通过 `connect` + `keyBy` 接收 Creation 和 Action。
  - `processElement1(Creation)`：status=0 时写入 creationState；status≠0 时清空所有 state（creation/score/lastUpdateTs）并输出 `removed=true` 的移除信号。
  - `processElement2(Action)`：从 creationState 读取 Creation，如果 state 为空（未到或已移除）则跳过；否则按权重+指数衰减计算热度，输出 `removed=false` 的 TrendingResult。
  - Action 权重：点击=1、点赞=2、点赞评论=1、评论=3、回复=1、转发=3。
- **类目排行管理**
  - `src/main/java/violet/trending/flink/processing/processors/CategoryRankingManager.java`
  - 每个类目（及全局 "all"）维护 `MapState<creationId, TrendingResult>`，记录所有活跃创作。
  - `processElement`：收到 `removed=true` 时从 MapState 删除；否则更新 MapState 并注册下一个窗口边界定时器。
  - `onTimer`：遍历 MapState 中所有创作，按 `score * exp(-λ * (now - lastActionTs))` 统一衰减，低于 0.01 淘汰，剩余排序取 Top100 输出；MapState 非空时续注册下一个定时器。
  - 双半衰期：`calculatorHalfLifeMillis` 控制行为间衰减，`windowDecayHalfLifeMillis` 控制定时器衰减。
- **KVrocks 写入**
  - `src/main/java/violet/trending/flink/connectors/redis/RedisHotRankingSink.java`
  - 每次批量 ZADD 到临时 key `trend:{key}:new`，然后 `RENAME` 到正式 key `trend:{key}`，原子替换、无空窗。

### 数据流

1. Kafka Creation + Kafka Action -> `connect` + `keyBy(creationId)` -> TrendingCalculator
   - status=0: 存储 state，供后续 Action 使用
   - status≠0: 清除 state + 发出移除信号
   - Action: 查 state，有则计算热度输出
2. TrendingResult -> CategoryRankingManager (keyBy category 和 keyBy "all")
   - `removed=true`: 从 MapState 删除
   - `removed=false`: 更新 MapState，注册定时器
3. 定时器触发 -> 统一衰减 -> 淘汰低分 -> 取 Top100 输出
4. KVrocks `trend:{category}` / `trend:all`（ZADD 到 `:new` -> RENAME 原子替换）
