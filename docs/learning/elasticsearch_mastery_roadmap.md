# Elasticsearch 精通学习路线图

> **学习理念**：理论 + 源码 + 实操 三位一体，遵循二八法则，掌握20%核心内容覆盖80%应用场景

## 📊 学习进度追踪

- [ ] 阶段一：基础架构与核心概念 (2-3周)
- [ ] 阶段二：数据写入与存储 (2-3周)
- [ ] 阶段三：搜索与查询 (3-4周)
- [ ] 阶段四：集群管理与高可用 (2-3周)
- [ ] 阶段五：性能优化与生产实践 (持续)

---

## 🎯 学习方法论

### 三维学习法
1. **理论学习**：理解概念、原理、设计思想（WHY）
2. **源码分析**：掌握实现细节、关键流程（HOW）
3. **实操验证**：动手实践、验证理论、积累经验（DO）

### 学习节奏
- **每天投入**：2-3小时
- **学习顺序**：理论 → 源码 → 实操 → 总结输出
- **验收方式**：完成实操任务 + 回答验收问题 + 输出学习笔记

### 输出方式
- 每个模块完成后写一篇技术博客
- 记录关键源码路径和核心逻辑
- 整理常见问题和解决方案

---

## 阶段一：基础架构与核心概念 (2-3周)

> **目标**：建立ES的整体认知，理解集群架构、节点角色、分片机制等核心概念

### 模块 1.1：集群架构与节点角色 ⭐⭐⭐

#### 学习目标
- 理解ES集群的分布式架构设计
- 掌握各种节点角色及其职责
- 理解分片（Shard）和副本（Replica）机制

#### 理论学习
1. **集群架构**
    - 集群（Cluster）、节点（Node）、索引（Index）的关系
    - 主分片（Primary Shard）和副本分片（Replica Shard）
    - 分片分配策略和路由机制

2. **节点角色** (重点掌握)
    - Master Node：集群元数据管理、分片分配决策
    - Data Node：数据存储和查询
    - Coordinating Node：请求路由和结果聚合
    - Ingest Node：数据预处理
    - 其他角色：ML Node、Transform Node等

3. **核心概念**
    - Document、Field、Mapping
    - Index、Type（7.x后废弃）、Shard
    - Cluster State：集群元数据

#### 源码分析
1. **启动流程** (已有基础)
    - `server/src/main/java/org/elasticsearch/bootstrap/Bootstrap.java`
    - `server/src/main/java/org/elasticsearch/node/Node.java`
    - 关注：节点初始化、模块加载、服务启动

2. **节点角色定义**
    - `server/src/main/java/org/elasticsearch/cluster/node/DiscoveryNode.java`
    - `server/src/main/java/org/elasticsearch/cluster/node/DiscoveryNodeRole.java`
    - 关注：角色定义、角色判断逻辑

3. **集群状态**
    - `server/src/main/java/org/elasticsearch/cluster/ClusterState.java`
    - `server/src/main/java/org/elasticsearch/cluster/metadata/Metadata.java`
    - 关注：元数据结构、版本管理

#### 实操任务
1. **搭建3节点集群**
   ```bash
   # 1个Master节点 + 2个Data节点
   # 配置不同的node.roles
   ```
    - 观察节点发现和加入过程
    - 查看集群状态：`GET /_cluster/health`
    - 查看节点信息：`GET /_cat/nodes?v`

2. **分片分配实验**
    - 创建索引，指定5个主分片，1个副本
    - 观察分片在节点间的分配
    - 停止一个节点，观察分片重新分配

3. **角色隔离实验**
    - 配置专用Master节点（不存储数据）
    - 配置专用Data节点（不参与Master选举）
    - 配置Coordinating节点（只做请求路由）

#### 验收标准
- [ ] 能画出ES集群架构图，标注各组件关系
- [ ] 能解释分片路由公式：`shard = hash(routing) % number_of_primary_shards`
- [ ] 能说出为什么主分片数创建后不能修改
- [ ] 能独立搭建多节点集群并配置不同角色
- [ ] 能通过API查看集群状态并解读关键指标

#### 常见问题
1. **为什么需要副本分片？**
    - 高可用：主分片故障时副本可以提升为主分片
    - 高性能：副本可以分担查询压力

2. **分片数如何规划？**
    - 单个分片建议20-50GB
    - 分片数 = 数据总量 / 单分片大小
    - 考虑节点数和扩展性

---

### 模块 1.2：索引的CRUD操作 ⭐⭐⭐

#### 学习目标
- 掌握索引和文档的基本操作
- 理解Mapping和Settings的作用
- 理解文档的版本控制和并发控制

#### 理论学习
1. **索引管理**
    - 创建索引：Settings（分片数、副本数、刷新间隔等）
    - Mapping定义：字段类型、分词器、索引选项
    - 索引模板（Index Template）和组件模板

2. **文档操作**
    - Index：创建或更新文档
    - Get：根据ID获取文档
    - Update：部分更新文档
    - Delete：删除文档
    - Bulk：批量操作

3. **版本控制**
    - 内部版本号（_version）
    - 外部版本号（version_type=external）
    - 乐观锁并发控制（if_seq_no + if_primary_term）

#### 源码分析
1. **REST层** (已有基础)
    - `server/src/main/java/org/elasticsearch/rest/action/document/RestIndexAction.java`
    - `server/src/main/java/org/elasticsearch/rest/action/document/RestGetAction.java`
    - 关注：请求解析、参数验证

2. **Transport层** (已有基础)
    - `server/src/main/java/org/elasticsearch/action/index/TransportIndexAction.java`
    - `server/src/main/java/org/elasticsearch/action/get/TransportGetAction.java`
    - 关注：路由计算、请求转发

3. **Mapping管理**
    - `server/src/main/java/org/elasticsearch/cluster/metadata/MappingMetadata.java`
    - `server/src/main/java/org/elasticsearch/index/mapper/DocumentMapper.java`
    - 关注：动态Mapping、字段类型推断

#### 实操任务
1. **索引创建与配置**
   ```json
   PUT /my_index
   {
     "settings": {
       "number_of_shards": 3,
       "number_of_replicas": 1,
       "refresh_interval": "1s"
     },
     "mappings": {
       "properties": {
         "title": {"type": "text", "analyzer": "ik_max_word"},
         "price": {"type": "double"},
         "created_at": {"type": "date"}
       }
     }
   }
   ```

2. **文档CRUD操作**
    - 插入10000条测试数据（使用Bulk API）
    - 根据ID查询文档
    - 更新部分字段
    - 删除文档并验证

3. **并发控制实验**
    - 模拟两个客户端同时更新同一文档
    - 使用if_seq_no和if_primary_term避免冲突
    - 观察版本号变化

#### 验收标准
- [ ] 能编写完整的索引创建语句（包含Settings和Mappings）
- [ ] 能使用Bulk API批量导入数据
- [ ] 能解释_version、_seq_no、_primary_term的区别
- [ ] 能处理文档更新冲突
- [ ] 能通过源码追踪一个Index请求的完整流程

#### 常见问题
1. **动态Mapping的优缺点？**
    - 优点：灵活，无需预定义
    - 缺点：可能推断错误类型，影响查询性能

2. **什么时候用Update，什么时候用Index？**
    - Update：部分字段更新，使用脚本
    - Index：全量替换文档

---

### 模块 1.3：REST与Action框架 ⭐⭐

#### 学习目标
- 理解ES的请求处理框架
- 掌握REST层到Transport层的调用链路
- 理解Action的注册和执行机制

#### 理论学习
1. **请求处理流程**
    - REST层：HTTP请求解析
    - Transport层：节点间通信
    - Action执行：业务逻辑处理

2. **核心组件**
    - RestController：REST请求路由
    - ActionModule：Action注册
    - TransportService：节点间通信

#### 源码分析 (已有基础，深化理解)
1. **REST框架**
    - `server/src/main/java/org/elasticsearch/rest/RestController.java`
    - `server/src/main/java/org/elasticsearch/rest/RestHandler.java`

2. **Action框架**
    - `server/src/main/java/org/elasticsearch/action/ActionModule.java`
    - `server/src/main/java/org/elasticsearch/action/support/TransportAction.java`

3. **Transport通信**
    - `server/src/main/java/org/elasticsearch/transport/TransportService.java`

#### 实操任务
1. **开发简单的REST插件**
    - 实现一个自定义的REST Handler
    - 注册到RestController
    - 测试HTTP请求

2. **追踪请求链路**
    - 使用调试器追踪一个Index请求
    - 记录关键方法调用栈
    - 绘制调用时序图

#### 验收标准
- [ ] 能画出REST请求的完整处理流程图
- [ ] 能开发简单的REST插件
- [ ] 能解释Coordinating Node的作用

---

## 阶段二：数据写入与存储 (2-3周)

> **目标**：深入理解数据写入流程、存储机制、持久化策略

### 模块 2.1：文档写入流程 ⭐⭐⭐

#### 学习目标
- 掌握文档写入的完整流程
- 理解Translog的作用和刷盘机制
- 理解Refresh、Flush、Merge的区别

#### 理论学习
1. **写入流程**
   ```
   Client → Coordinating Node → Primary Shard → Replica Shard
   ```
    - 路由计算：确定目标分片
    - 主分片写入：写Translog + 写内存Buffer
    - 副本同步：并行写入所有副本
    - 响应客户端：等待副本确认

2. **持久化机制**
    - **Translog**：预写日志，保证数据不丢失
    - **Refresh**：内存Buffer → Segment（可搜索）
    - **Flush**：Segment → 磁盘 + 清空Translog
    - **Merge**：合并小Segment，删除标记删除的文档

3. **关键参数**
    - `refresh_interval`：默认1s
    - `translog.durability`：request（每次请求刷盘）/ async（异步刷盘）
    - `translog.sync_interval`：默认5s

#### 源码分析
1. **写入入口**
    - `server/src/main/java/org/elasticsearch/action/bulk/TransportBulkAction.java`
    - `server/src/main/java/org/elasticsearch/action/bulk/TransportShardBulkAction.java`

2. **IndexShard写入**
    - `server/src/main/java/org/elasticsearch/index/shard/IndexShard.java`
        - `applyIndexOperationOnPrimary()` 方法
        - `applyIndexOperationOnReplica()` 方法

3. **Engine层**
    - `server/src/main/java/org/elasticsearch/index/engine/InternalEngine.java`
        - `index()` 方法：写入文档
        - `refresh()` 方法：刷新Segment
        - `flush()` 方法：持久化

4. **Translog**
    - `server/src/main/java/org/elasticsearch/index/translog/Translog.java`
    - 关注：写入、刷盘、恢复逻辑

#### 实操任务
1. **写入性能测试**
   ```bash
   # 使用esrally或自己编写脚本
   # 测试不同批量大小的写入性能
   ```
    - Bulk size: 100, 500, 1000, 5000
    - 记录TPS、延迟、资源占用

2. **Refresh机制验证**
   ```json
   # 设置refresh_interval=-1（禁用自动刷新）
   PUT /test_index/_settings
   {
     "refresh_interval": "-1"
   }

   # 写入数据后立即查询（查不到）
   # 手动refresh后查询（能查到）
   POST /test_index/_refresh
   ```

3. **Translog故障恢复**
    - 写入数据但不Flush
    - 强制kill ES进程
    - 重启ES，验证数据是否恢复

4. **观察Segment合并**
   ```bash
   # 查看Segment信息
   GET /test_index/_segments

   # 写入大量数据，观察Segment数量变化
   # 手动触发合并
   POST /test_index/_forcemerge?max_num_segments=1
   ```

#### 验收标准
- [ ] 能画出文档写入的完整流程图（包含主副分片同步）
- [ ] 能解释Refresh、Flush、Merge的区别和触发时机
- [ ] 能说出Translog的作用和刷盘策略
- [ ] 能通过源码追踪一个文档从写入到持久化的全过程
- [ ] 能根据业务场景调优写入性能参数

#### 常见问题
1. **为什么写入后不能立即搜索到？**
    - 需要等待Refresh（默认1s）
    - 可以设置`refresh=true`强制刷新（影响性能）

2. **如何保证数据不丢失？**
    - Translog持久化（默认每5s或每次请求）
    - 副本机制（主分片故障时副本提升）

3. **写入性能优化建议？**
    - 使用Bulk API批量写入
    - 增大refresh_interval
    - 调整translog刷盘策略
    - 增加副本数前先写入数据

---

### 模块 2.2：Lucene封装与存储 ⭐⭐

#### 学习目标
- 理解ES如何封装Lucene
- 掌握IndexShard和Engine的职责
- 理解Segment的存储结构

#### 理论学习
1. **Lucene基础**
    - Index、Segment、Document、Field
    - 倒排索引结构
    - 正排索引（DocValues）

2. **ES的封装层次**
   ```
   Index (ES) → Shard → IndexShard → Engine → IndexWriter (Lucene)
   ```

3. **存储文件**
    - `.si`：Segment信息
    - `.tim/.tip`：倒排索引（Term Dictionary + Term Index）
    - `.doc/.pos/.pay`：文档号、位置、Payload
    - `.dvd/.dvm`：DocValues（列式存储）
    - `.fdt/.fdx`：存储字段（Store Fields）

#### 源码分析 (已有基础，深化理解)
1. **IndexShard**
    - `server/src/main/java/org/elasticsearch/index/shard/IndexShard.java`
    - 关注：生命周期管理、恢复流程

2. **Engine**
    - `server/src/main/java/org/elasticsearch/index/engine/Engine.java`
    - `server/src/main/java/org/elasticsearch/index/engine/InternalEngine.java`
    - 关注：Lucene IndexWriter的封装

3. **Store**
    - `server/src/main/java/org/elasticsearch/index/store/Store.java`
    - 关注：文件管理、校验和

#### 实操任务
1. **查看Lucene文件**
   ```bash
   # 进入数据目录
   cd data/nodes/0/indices/{index_uuid}/0/index/
   ls -lh

   # 使用Luke工具查看索引内容
   ```

2. **分析Segment结构**
    - 使用`_segments` API查看Segment信息
    - 观察Segment大小、文档数、删除文档数
    - 理解Segment不可变性

#### 验收标准
- [ ] 能说出ES的分层架构（Index → Shard → Engine → Lucene）
- [ ] 能解释倒排索引和正排索引的区别
- [ ] 能列举Lucene的主要文件类型及作用
- [ ] 能通过Luke工具查看索引内容

---

### 模块 2.3：副本同步与一致性 ⭐⭐

#### 学习目标
- 理解主副分片的同步机制
- 掌握Sequence Number和Checkpoint
- 理解全局检查点和本地检查点

#### 理论学习
1. **副本同步模型**
    - 主副同步：Primary-Backup模型
    - 写入流程：Primary写入成功 → 并行写入所有Replica → 响应客户端
    - 一致性保证：等待多数副本确认（wait_for_active_shards）

2. **Sequence Number**
    - 每个操作分配唯一的序列号
    - 用于副本同步、故障恢复、跨集群复制
    - Global Checkpoint：所有副本都已持久化的序列号
    - Local Checkpoint：当前分片已持久化的序列号

3. **故障恢复**
    - Primary故障：副本提升为Primary
    - Replica故障：从Primary恢复缺失的操作

#### 源码分析
1. **副本同步**
    - `server/src/main/java/org/elasticsearch/action/support/replication/TransportReplicationAction.java`
    - 关注：主副分片的写入协调

2. **Sequence Number**
    - `server/src/main/java/org/elasticsearch/index/seqno/SequenceNumbers.java`
    - `server/src/main/java/org/elasticsearch/index/seqno/ReplicationTracker.java`

#### 实操任务
1. **观察副本同步**
    - 创建1主2副的索引
    - 写入数据，观察主副分片的_seq_no
    - 停止一个副本，继续写入，重启副本观察恢复过程

2. **一致性参数测试**
   ```json
   # 设置wait_for_active_shards
   PUT /test_index/_doc/1?wait_for_active_shards=2
   {
     "field": "value"
   }
   ```

#### 验收标准
- [ ] 能解释Primary-Backup模型
- [ ] 能说出Sequence Number的作用
- [ ] 能解释Global Checkpoint和Local Checkpoint的区别
- [ ] 能处理副本故障恢复场景

---

## 阶段三：搜索与查询 (3-4周)

> **目标**：掌握ES的搜索能力，理解查询DSL、相关性评分、聚合分析

### 模块 3.1：Query DSL与查询类型 ⭐⭐⭐

#### 学习目标
- 掌握常用的查询类型
- 理解Query Context和Filter Context的区别
- 能编写复杂的组合查询

#### 理论学习
1. **查询分类**
    - **全文检索**：match、match_phrase、multi_match
    - **精确匹配**：term、terms、range、exists
    - **复合查询**：bool（must/should/must_not/filter）
    - **嵌套查询**：nested、parent-child

2. **Query vs Filter**
    - Query：计算相关性评分，结果可缓存
    - Filter：只过滤，不计算评分，性能更好

3. **查询优化**
    - 使用Filter Context减少评分计算
    - 合理使用bool查询
    - 避免深度分页

#### 源码分析
1. **查询解析**
    - `server/src/main/java/org/elasticsearch/index/query/QueryBuilder.java`
    - `server/src/main/java/org/elasticsearch/index/query/BoolQueryBuilder.java`
    - `server/src/main/java/org/elasticsearch/index/query/MatchQueryBuilder.java`

2. **查询执行**
    - `server/src/main/java/org/elasticsearch/search/SearchService.java`
    - `server/src/main/java/org/elasticsearch/search/query/QueryPhase.java`

#### 实操任务
1. **基础查询练习**
   ```json
   # 全文检索
   GET /products/_search
   {
     "query": {
       "match": {
         "title": "Elasticsearch权威指南"
       }
     }
   }

   # 精确匹配
   GET /products/_search
   {
     "query": {
       "term": {
         "status": "published"
       }
     }
   }

   # 范围查询
   GET /products/_search
   {
     "query": {
       "range": {
         "price": {
           "gte": 100,
           "lte": 500
         }
       }
     }
   }
   ```

2. **复合查询练习**
   ```json
   GET /products/_search
   {
     "query": {
       "bool": {
         "must": [
           {"match": {"title": "Elasticsearch"}}
         ],
         "filter": [
           {"term": {"status": "published"}},
           {"range": {"price": {"lte": 500}}}
         ],
         "should": [
           {"match": {"category": "技术"}}
         ],
         "minimum_should_match": 1
       }
     }
   }
   ```

3. **性能对比实验**
    - 对比Query Context和Filter Context的性能差异
    - 使用Profile API分析查询性能
   ```json
   GET /products/_search
   {
     "profile": true,
     "query": {...}
   }
   ```

#### 验收标准
- [ ] 能熟练使用match、term、range、bool等查询
- [ ] 能解释Query Context和Filter Context的区别
- [ ] 能使用Profile API分析查询性能
- [ ] 能编写复杂的多条件组合查询

#### 常见问题
1. **match和term的区别？**
    - match：全文检索，会分词
    - term：精确匹配，不分词

2. **为什么Filter更快？**
    - 不计算评分
    - 结果可以缓存

---

### 模块 3.2：相关性评分与排序 ⭐⭐⭐

#### 学习目标
- 理解TF-IDF和BM25算法
- 掌握自定义评分方法
- 理解排序机制

#### 理论学习
1. **评分算法**
    - **TF-IDF**：词频-逆文档频率（ES 5.x之前）
    - **BM25**：改进的TF-IDF（ES 5.x之后默认）
    - 评分因素：词频、文档频率、字段长度、boost

2. **自定义评分**
    - Function Score Query
    - Script Score Query
    - Boosting Query

3. **排序**
    - 按评分排序（_score）
    - 按字段排序（需要DocValues）
    - 多字段排序

#### 源码分析
1. **评分计算**
    - `server/src/main/java/org/elasticsearch/index/similarity/SimilarityService.java`
    - Lucene的Similarity实现

2. **排序**
    - `server/src/main/java/org/elasticsearch/search/sort/SortBuilder.java`

#### 实操任务
1. **查看评分详情**
   ```json
   GET /products/_search
   {
     "explain": true,
     "query": {
       "match": {"title": "Elasticsearch"}
     }
   }
   ```

2. **自定义评分**
   ```json
   GET /products/_search
   {
     "query": {
       "function_score": {
         "query": {"match": {"title": "Elasticsearch"}},
         "functions": [
           {
             "filter": {"term": {"category": "技术"}},
             "weight": 2
           },
           {
             "field_value_factor": {
               "field": "sales",
               "factor": 0.1
             }
           }
         ],
         "score_mode": "sum",
         "boost_mode": "multiply"
       }
     }
   }
   ```

3. **排序实验**
   ```json
   GET /products/_search
   {
     "query": {"match_all": {}},
     "sort": [
       {"sales": {"order": "desc"}},
       {"_score": {"order": "desc"}},
       {"price": {"order": "asc"}}
     ]
   }
   ```

#### 验收标准
- [ ] 能解释BM25算法的基本原理
- [ ] 能使用explain查看评分详情
- [ ] 能使用function_score自定义评分
- [ ] 能实现复杂的排序需求

---

### 模块 3.3：聚合分析 ⭐⭐⭐

#### 学习目标
- 掌握Bucket、Metric、Pipeline聚合
- 理解聚合的执行流程
- 能实现复杂的统计分析需求

#### 理论学习
1. **聚合分类**
    - **Bucket聚合**：分组（terms、date_histogram、range）
    - **Metric聚合**：统计（sum、avg、max、min、stats、cardinality）
    - **Pipeline聚合**：基于其他聚合的结果再聚合

2. **聚合执行**
    - 基于DocValues（列式存储）
    - 内存消耗：取决于唯一值数量
    - 精度控制：size、shard_size

#### 源码分析
1. **聚合框架**
    - `server/src/main/java/org/elasticsearch/search/aggregations/Aggregator.java`
    - `server/src/main/java/org/elasticsearch/search/aggregations/bucket/terms/TermsAggregator.java`

2. **聚合执行**
    - `server/src/main/java/org/elasticsearch/search/aggregations/AggregationPhase.java`

#### 实操任务
1. **Bucket聚合**
   ```json
   # 按类别分组统计
   GET /products/_search
   {
     "size": 0,
     "aggs": {
       "category_stats": {
         "terms": {
           "field": "category.keyword",
           "size": 10
         },
         "aggs": {
           "avg_price": {
             "avg": {"field": "price"}
           },
           "total_sales": {
             "sum": {"field": "sales"}
           }
         }
       }
     }
   }
   ```

2. **时间序列聚合**
   ```json
   # 按天统计销售额
   GET /orders/_search
   {
     "size": 0,
     "aggs": {
       "sales_over_time": {
         "date_histogram": {
           "field": "order_date",
           "calendar_interval": "day"
         },
         "aggs": {
           "daily_revenue": {
             "sum": {"field": "amount"}
           }
         }
       }
     }
   }
   ```

3. **Pipeline聚合**
   ```json
   # 计算移动平均
   GET /orders/_search
   {
     "size": 0,
     "aggs": {
       "sales_per_day": {
         "date_histogram": {
           "field": "order_date",
           "calendar_interval": "day"
         },
         "aggs": {
           "daily_sales": {
             "sum": {"field": "amount"}
           },
           "moving_avg": {
             "moving_avg": {
               "buckets_path": "daily_sales"
             }
           }
         }
       }
     }
   }
   ```

#### 验收标准
- [ ] 能使用terms、date_histogram等Bucket聚合
- [ ] 能使用sum、avg、cardinality等Metric聚合
- [ ] 能实现嵌套聚合（Bucket中嵌套Metric）
- [ ] 能使用Pipeline聚合实现复杂统计

#### 常见问题
1. **聚合为什么需要DocValues？**
    - DocValues是列式存储，适合聚合和排序
    - 不需要反序列化整个文档

2. **如何优化聚合性能？**
    - 使用Filter减少聚合的文档数
    - 调整size和shard_size参数
    - 使用Composite聚合处理大量唯一值

---

### 模块 3.4：搜索执行流程 ⭐⭐

#### 学习目标
- 理解Query Then Fetch的两阶段搜索
- 掌握搜索的分布式执行流程
- 理解深度分页问题

#### 理论学习
1. **两阶段搜索**
    - **Query阶段**：各分片查询并返回文档ID和评分
    - **Fetch阶段**：Coordinating Node获取完整文档

2. **分布式搜索**
    - Coordinating Node：接收请求，分发到各分片，聚合结果
    - 每个分片独立执行查询
    - 结果合并和排序

3. **深度分页问题**
    - from + size的问题：每个分片返回from+size条数据
    - 解决方案：Scroll API、Search After

#### 源码分析
1. **搜索入口**
    - `server/src/main/java/org/elasticsearch/action/search/TransportSearchAction.java`
    - `server/src/main/java/org/elasticsearch/action/search/SearchPhaseController.java`

2. **Query阶段**
    - `server/src/main/java/org/elasticsearch/search/query/QueryPhase.java`

3. **Fetch阶段**
    - `server/src/main/java/org/elasticsearch/search/fetch/FetchPhase.java`

#### 实操任务
1. **追踪搜索流程**
    - 使用Profile API查看Query和Fetch阶段的耗时
    - 分析各分片的执行情况

2. **深度分页实验**
   ```json
   # 普通分页（性能差）
   GET /products/_search
   {
     "from": 10000,
     "size": 10,
     "query": {"match_all": {}}
   }

   # Search After（推荐）
   GET /products/_search
   {
     "size": 10,
     "query": {"match_all": {}},
     "search_after": [1234567890],
     "sort": [{"timestamp": "asc"}]
   }

   # Scroll API（遍历全量数据）
   POST /products/_search?scroll=1m
   {
     "size": 1000,
     "query": {"match_all": {}}
   }
   ```

#### 验收标准
- [ ] 能画出Query Then Fetch的流程图
- [ ] 能解释深度分页的性能问题
- [ ] 能使用Search After和Scroll API
- [ ] 能通过源码追踪搜索的完整流程

---

## 阶段四：集群管理与高可用 (2-3周)

> **目标**：掌握集群管理、Master选举、分片分配、故障恢复等核心机制

### 模块 4.1：Master选举与集群协调 ⭐⭐⭐

#### 学习目标
- 理解Master选举算法（Zen Discovery → Raft）
- 掌握集群状态的发布机制
- 理解脑裂问题及解决方案

#### 理论学习
1. **Master职责**
    - 管理集群元数据（Cluster State）
    - 分片分配决策
    - 索引创建/删除
    - 节点加入/离开

2. **选举算法**
    - ES 7.x之前：Zen Discovery（基于Bully算法）
    - ES 7.x之后：基于Raft的选举
    - 选举条件：多数派原则（quorum）

3. **脑裂问题**
    - 原因：网络分区导致多个Master
    - 解决：`discovery.zen.minimum_master_nodes`（7.x之前）
    - 7.x之后：自动计算quorum

#### 源码分析 (已有基础，深化理解)
1. **Master Service**
    - `server/src/main/java/org/elasticsearch/cluster/service/MasterService.java`
    - 关注：任务队列、批量处理、状态发布

2. **Coordinator（Raft实现）**
    - `server/src/main/java/org/elasticsearch/cluster/coordination/Coordinator.java`
    - `server/src/main/java/org/elasticsearch/cluster/coordination/ElectionSchedulerFactory.java`

3. **集群状态发布**
    - `server/src/main/java/org/elasticsearch/cluster/coordination/PublicationTransportHandler.java`

#### 实操任务
1. **Master选举实验**
    - 搭建3节点集群（都是Master-eligible）
    - 停止当前Master，观察选举过程
    - 查看日志中的选举信息

2. **集群状态观察**
   ```bash
   # 查看集群状态
   GET /_cluster/state

   # 查看Master节点
   GET /_cat/master?v

   # 查看待处理任务
   GET /_cluster/pending_tasks
   ```

3. **脑裂模拟**（7.x之前版本）
    - 配置错误的minimum_master_nodes
    - 制造网络分区
    - 观察脑裂现象

#### 验收标准
- [ ] 能解释Master选举的流程和条件
- [ ] 能说出脑裂的原因和解决方案
- [ ] 能通过API查看集群状态和Master信息
- [ ] 能通过源码理解MasterService的任务处理机制

---

### 模块 4.2：分片分配与路由 ⭐⭐⭐

#### 学习目标
- 理解分片分配策略
- 掌握分片路由机制
- 理解分片再平衡（Rebalance）

#### 理论学习
1. **分片分配**
    - 初始分配：索引创建时
    - 副本分配：主分片就绪后
    - 再平衡：节点加入/离开时

2. **分配策略**
    - SameShardAllocationDecider：同一分片的主副本不在同一节点
    - DiskThresholdDecider：磁盘水位控制
    - AwarenessAllocationDecider：机架感知

3. **分片路由**
    - 路由公式：`shard = hash(routing) % number_of_primary_shards`
    - 自定义routing：控制文档分布

#### 源码分析
1. **分配服务**
    - `server/src/main/java/org/elasticsearch/cluster/routing/allocation/AllocationService.java`
    - `server/src/main/java/org/elasticsearch/cluster/routing/allocation/allocator/BalancedShardsAllocator.java`

2. **分配决策器**
    - `server/src/main/java/org/elasticsearch/cluster/routing/allocation/decider/AllocationDeciders.java`
    - 各种Decider实现

3. **路由表**
    - `server/src/main/java/org/elasticsearch/cluster/routing/RoutingTable.java`
    - `server/src/main/java/org/elasticsearch/cluster/routing/IndexRoutingTable.java`

#### 实操任务
1. **分片分配观察**
   ```bash
   # 查看分片分配情况
   GET /_cat/shards?v

   # 查看分配解释
   GET /_cluster/allocation/explain
   {
     "index": "my_index",
     "shard": 0,
     "primary": true
   }
   ```

2. **手动分片控制**
   ```json
   # 禁用分片分配
   PUT /_cluster/settings
   {
     "transient": {
       "cluster.routing.allocation.enable": "none"
     }
   }

   # 手动移动分片
   POST /_cluster/reroute
   {
     "commands": [
       {
         "move": {
           "index": "my_index",
           "shard": 0,
           "from_node": "node1",
           "to_node": "node2"
         }
       }
     ]
   }
   ```

3. **自定义routing实验**
   ```json
   # 使用routing写入
   PUT /my_index/_doc/1?routing=user123
   {
     "user_id": "user123",
     "content": "test"
   }

   # 使用routing查询
   GET /my_index/_search?routing=user123
   {
     "query": {"term": {"user_id": "user123"}}
   }
   ```

#### 验收标准
- [ ] 能解释分片分配的决策过程
- [ ] 能使用allocation/explain API排查分片未分配问题
- [ ] 能手动控制分片分配
- [ ] 能使用自定义routing优化查询性能

---

### 模块 4.3：故障恢复与数据恢复 ⭐⭐

#### 学习目标
- 理解节点故障的恢复流程
- 掌握分片恢复机制
- 理解Snapshot和Restore

#### 理论学习
1. **故障类型**
    - 节点故障：Master故障、Data节点故障
    - 分片故障：主分片故障、副本故障
    - 网络分区

2. **恢复流程**
    - 主分片故障：副本提升为主分片
    - 副本故障：从主分片恢复
    - 节点重启：从Translog恢复

3. **快照与恢复**
    - Snapshot：全量备份
    - Restore：恢复到指定时间点
    - 增量快照

#### 源码分析
1. **恢复流程**
    - `server/src/main/java/org/elasticsearch/indices/recovery/RecoverySource.java`
    - `server/src/main/java/org/elasticsearch/indices/recovery/RecoveryTarget.java`

2. **快照**
    - `server/src/main/java/org/elasticsearch/snapshots/SnapshotsService.java`

#### 实操任务
1. **故障演练**
    - 停止一个Data节点，观察分片恢复
    - 停止Master节点，观察选举和恢复
    - 查看恢复进度：`GET /_cat/recovery?v`

2. **快照与恢复**
   ```json
   # 注册快照仓库
   PUT /_snapshot/my_backup
   {
     "type": "fs",
     "settings": {
       "location": "/mount/backups/my_backup"
     }
   }

   # 创建快照
   PUT /_snapshot/my_backup/snapshot_1
   {
     "indices": "my_index",
     "ignore_unavailable": true
   }

   # 恢复快照
   POST /_snapshot/my_backup/snapshot_1/_restore
   {
     "indices": "my_index"
   }
   ```

#### 验收标准
- [ ] 能处理各种节点故障场景
- [ ] 能配置和使用快照备份
- [ ] 能监控分片恢复进度
- [ ] 能通过源码理解恢复流程

---

## 阶段五：性能优化与生产实践 (持续)

> **目标**：掌握性能调优技巧，积累生产环境最佳实践

### 模块 5.1：写入性能优化 ⭐⭐⭐

#### 学习目标
- 掌握写入性能调优参数
- 理解写入瓶颈和优化方法
- 能根据业务场景制定写入策略

#### 理论学习
1. **写入瓶颈**
    - CPU：JSON解析、分词、Lucene写入
    - 磁盘IO：Translog刷盘、Segment写入
    - 网络：副本同步

2. **优化策略**
    - **批量写入**：使用Bulk API，批量大小5-15MB
    - **减少Refresh**：增大refresh_interval或设置为-1
    - **调整Translog**：异步刷盘（durability=async）
    - **副本策略**：先写入后增加副本
    - **硬件优化**：SSD、增加内存、多核CPU

3. **关键参数**
   ```json
   {
     "refresh_interval": "30s",
     "number_of_replicas": 0,
     "translog.durability": "async",
     "translog.sync_interval": "30s"
   }
   ```

#### 实操任务
1. **基准测试**
    - 使用esrally进行基准测试
    - 记录默认配置下的写入性能

2. **参数调优对比**
    - 调整refresh_interval：1s vs 30s vs -1
    - 调整bulk size：100 vs 1000 vs 5000
    - 调整副本数：0 vs 1 vs 2
    - 记录每种配置的TPS和延迟

3. **监控写入性能**
   ```bash
   # 查看索引统计
   GET /my_index/_stats

   # 查看线程池
   GET /_cat/thread_pool/write?v

   # 查看节点统计
   GET /_nodes/stats
   ```

#### 验收标准
- [ ] 能列举5种以上写入优化方法
- [ ] 能根据业务场景选择合适的参数
- [ ] 能使用监控API定位写入瓶颈
- [ ] 能将写入性能提升3倍以上

#### 常见问题
1. **写入时CPU高怎么办？**
    - 减少分词器复杂度
    - 增加节点数，分散压力
    - 使用更快的JSON库

2. **写入时磁盘IO高怎么办？**
    - 使用SSD
    - 增大refresh_interval
    - 调整Translog刷盘策略

---

### 模块 5.2：查询性能优化 ⭐⭐⭐

#### 学习目标
- 掌握查询性能调优技巧
- 理解查询缓存机制
- 能优化慢查询

#### 理论学习
1. **查询优化原则**
    - 使用Filter Context（可缓存）
    - 避免深度分页
    - 合理使用分片数
    - 使用routing减少查询分片数

2. **缓存机制**
    - **Query Cache**：缓存Filter结果（Segment级别）
    - **Request Cache**：缓存整个查询结果（size=0的聚合）
    - **Fielddata Cache**：缓存字段数据（用于排序和聚合）

3. **慢查询优化**
    - 使用Profile API分析
    - 优化Mapping（禁用不需要的功能）
    - 使用更精确的查询

#### 实操任务
1. **慢查询分析**
   ```json
   # 开启慢查询日志
   PUT /my_index/_settings
   {
     "index.search.slowlog.threshold.query.warn": "10s",
     "index.search.slowlog.threshold.query.info": "5s",
     "index.search.slowlog.threshold.fetch.warn": "1s"
   }

   # 使用Profile分析
   GET /my_index/_search
   {
     "profile": true,
     "query": {...}
   }
   ```

2. **缓存效果测试**
    - 执行相同的Filter查询多次，对比耗时
    - 查看缓存命中率：`GET /_stats/query_cache`

3. **查询优化实战**
    - 找出一个慢查询
    - 使用Profile分析瓶颈
    - 优化查询语句或Mapping
    - 对比优化前后的性能

#### 验收标准
- [ ] 能使用Profile API分析慢查询
- [ ] 能解释三种缓存的区别和使用场景
- [ ] 能将慢查询优化到可接受的范围
- [ ] 能配置和监控缓存

---

### 模块 5.3：容量规划与监控 ⭐⭐

#### 学习目标
- 掌握容量规划方法
- 理解关键监控指标
- 能搭建监控告警系统

#### 理论学习
1. **容量规划**
    - **数据量评估**：单条文档大小 × 文档数 × 副本数
    - **分片规划**：单分片20-50GB，总分片数 < 节点数 × 20
    - **硬件选型**：CPU、内存、磁盘、网络

2. **关键指标**
    - **集群健康**：green/yellow/red
    - **节点资源**：CPU、内存、磁盘、JVM堆
    - **索引性能**：写入TPS、查询QPS、延迟
    - **分片状态**：未分配分片、恢复中分片

3. **监控方案**
    - Elasticsearch自带监控
    - Prometheus + Grafana
    - Elastic Stack（Metricbeat + Kibana）

#### 实操任务
1. **容量规划练习**
    - 假设场景：每天1亿条日志，每条1KB，保留30天
    - 计算：数据总量、分片数、节点数、磁盘容量

2. **搭建监控**
    - 使用Metricbeat采集ES指标
    - 在Kibana中创建监控Dashboard
    - 配置告警规则（磁盘使用率、JVM堆使用率）

3. **压力测试**
    - 使用esrally进行压测
    - 监控各项指标
    - 找出性能瓶颈

#### 验收标准
- [ ] 能根据业务需求进行容量规划
- [ ] 能搭建完整的监控系统
- [ ] 能配置关键指标的告警
- [ ] 能通过监控定位性能问题

---

### 模块 5.4：故障排查与最佳实践 ⭐⭐⭐

#### 学习目标
- 掌握常见故障的排查方法
- 积累生产环境最佳实践
- 能快速定位和解决问题

#### 理论学习
1. **常见故障**
    - 集群Red：分片未分配
    - OOM：JVM堆溢出
    - 磁盘满：写入失败
    - 查询慢：资源不足或查询不合理

2. **排查工具**
    - `_cat` API：快速查看集群状态
    - `_cluster/allocation/explain`：分片未分配原因
    - `_nodes/hot_threads`：CPU高的线程
    - 日志分析：elasticsearch.log

3. **最佳实践**
    - **索引设计**：合理的Mapping、分片数、副本数
    - **写入优化**：Bulk API、调整refresh_interval
    - **查询优化**：使用Filter、避免深度分页
    - **容量规划**：预留30%的资源余量
    - **监控告警**：关键指标实时监控
    - **备份恢复**：定期快照、异地备份

#### 实操任务
1. **故障演练**
    - 模拟磁盘满：写入大量数据直到磁盘水位告警
    - 模拟OOM：执行大量聚合查询
    - 模拟分片未分配：手动删除分片数据文件

2. **故障排查**
    - 使用`_cat` API快速定位问题
    - 使用`allocation/explain`查看分片未分配原因
    - 分析日志找出根因
    - 制定解决方案并执行

3. **编写运维手册**
    - 常见故障及解决方案
    - 日常巡检清单
    - 应急预案

#### 验收标准
- [ ] 能快速定位集群Red的原因
- [ ] 能处理OOM问题
- [ ] 能处理磁盘满问题
- [ ] 能编写完整的运维手册

#### 常见问题及解决方案

| 问题 | 原因 | 解决方案 |
|------|------|----------|
| 集群Red | 主分片未分配 | 检查节点状态、磁盘空间、分配策略 |
| OOM | JVM堆不足 | 增加堆内存、优化查询、限流 |
| 磁盘满 | 数据增长过快 | 清理旧数据、扩容、调整保留策略 |
| 查询慢 | 资源不足或查询不合理 | 优化查询、增加节点、使用缓存 |
| 写入慢 | 刷新频繁、副本多 | 调整refresh_interval、减少副本 |

---

## 📚 学习资源推荐

### 官方文档
- [Elasticsearch官方文档](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Elasticsearch源码](https://github.com/elastic/elasticsearch)

### 书籍
- 《Elasticsearch权威指南》
- 《Elasticsearch核心技术与实战》
- 《深入理解Elasticsearch》

### 博客与社区
- Elastic官方博客
- Elasticsearch中文社区
- Stack Overflow

### 工具
- **Kibana**：可视化管理和查询
- **Cerebro**：集群管理工具
- **esrally**：性能测试工具
- **Luke**：Lucene索引查看工具

---

## 🎓 学习建议

### 时间安排
- **每天2-3小时**，保持连续性
- **周末集中实操**，完成实验任务
- **每周总结**，输出学习笔记

### 学习方法
1. **理论先行**：先理解概念和原理
2. **源码验证**：通过源码加深理解
3. **实操巩固**：动手实践，验证理论
4. **输出倒逼**：写博客、做分享

### 进度追踪
- 每完成一个模块，在文档开头的进度追踪中打勾
- 记录学习笔记和心得
- 整理常见问题和解决方案

### 验收方式
- 完成所有实操任务
- 回答验收标准中的问题
- 输出一篇技术博客

---

## 🚀 预期效果

完成本学习路线图后，你将：

1. **理论扎实**：深入理解ES的核心概念和设计原理
2. **源码熟悉**：能快速定位和阅读关键源码
3. **实操能力**：能独立搭建、优化、运维ES集群
4. **问题解决**：能快速定位和解决生产环境问题
5. **架构设计**：能根据业务需求设计合理的ES方案

**预计学习时间**：
- 核心内容：12-16周（每天2-3小时）
- 达到精通：6-12个月（包含实践积累）

---

## 📝 学习日志

### 第1周
- [ ] 完成模块1.1：集群架构与节点角色
- [ ] 完成模块1.2：索引的CRUD操作
- 学习笔记：
- 遇到的问题：

### 第2周
- [ ] 完成模块1.3：REST与Action框架
- [ ] 完成模块2.1：文档写入流程
- 学习笔记：
- 遇到的问题：

（后续周次自行添加）

---

**最后更新时间**：2025-12-20
**版本**：v1.0
