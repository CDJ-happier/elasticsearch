# Elasticsearch IndexRouting 路由机制分析

## 1. 概述

`IndexRouting` 是 Elasticsearch 中负责文档路由的核心组件，它决定了文档应该被索引到哪个分片（shard）。该类位于 `org.elasticsearch.cluster.routing` 包中，提供了多种路由策略以适应不同的索引配置。

## 2. 核心概念

### 2.1 路由的作用
- **写入路由**：确定文档应该写入到哪个分片
- **读取路由**：确定应该从哪个分片读取文档
- **搜索路由**：确定搜索请求应该发送到哪些分片

### 2.2 关键参数
- `routingNumShards`：路由分片数（用于计算哈希）
- `routingFactor`：路由因子（用于分片分裂）
- `routing`：可选的路由值，用于控制文档分片分配
- `id`：文档ID

## 3. 路由策略类型

Elasticsearch 根据索引的配置提供了三种主要的路由策略：

### 3.1 Unpartitioned（非分区索引）

**适用场景**：标准索引，没有特殊的路由配置

**核心算法**：
```java
int shardId = hashToShardId(effectiveRoutingToHash(routing == null ? id : routing));
```

**路由逻辑**：
1. 如果提供了 `routing` 参数，使用 `routing` 值计算哈希
2. 如果没有提供 `routing`，使用文档 `id` 计算哈希
3. 通过 Murmur3 哈希算法计算哈希值
4. 将哈希值映射到分片ID

**操作支持**：
- ✅ `indexShard`：索引文档
- ✅ `updateShard`：更新文档
- ✅ `deleteShard`：删除文档
- ✅ `getShard`：获取文档
- ✅ `collectSearchShards`：搜索时收集分片

**示例**：
```
文档ID: "doc123"
routing: null
哈希计算: hash("doc123") → 1234567890
分片计算: 1234567890 % routingNumShards / routingFactor → shard 2
```

### 3.2 Partitioned（分区索引）

**适用场景**：配置了 `routing_partition_size` 的索引

**核心算法**：
```java
int offset = Math.floorMod(effectiveRoutingToHash(id), routingPartitionSize);
int shardId = hashToShardId(effectiveRoutingToHash(routing) + offset);
```

**路由逻辑**：
1. **必须提供 routing 参数**（否则抛出异常）
2. 使用文档 `id` 计算偏移量（offset）
3. 使用 `routing` 值计算基础哈希
4. 将基础哈希加上偏移量，得到最终的分片ID

**特点**：
- 相同 `routing` 值的文档会分布在多个分片上（由 `routingPartitionSize` 决定）
- 提高了数据分布的均匀性
- 搜索时需要查询多个分片

**操作支持**：
- ✅ `indexShard`：索引文档（必须提供 routing）
- ✅ `updateShard`：更新文档（必须提供 routing）
- ✅ `deleteShard`：删除文档（必须提供 routing）
- ✅ `getShard`：获取文档（必须提供 routing）
- ✅ `collectSearchShards`：收集 `routingPartitionSize` 个分片

**示例**：
```
文档ID: "doc123"
routing: "user456"
routingPartitionSize: 5

offset = hash("doc123") % 5 = 3
baseHash = hash("user456") = 987654321
finalHash = 987654321 + 3
shardId = finalHash % routingNumShards / routingFactor → shard 7
```

### 3.3 ExtractFromSource（从源文档提取路由）

**适用场景**：时间序列索引（Time Series Index），配置了 `routing_path` 的索引

**核心算法**：
```java
int hash = hashSource(sourceType, source).buildHash();
int shardId = hashToShardId(hash);
```

**路由逻辑**：
1. **不允许显式指定 routing 参数**
2. 从文档源（source）中提取 `routing_path` 指定的字段
3. 对提取的字段值进行哈希计算
4. 将哈希值编码到文档ID中（Base64编码）
5. 使用哈希值确定分片

**字段提取规则**：
- 支持嵌套字段（使用点号分隔，如 `host.name`）
- 支持多值字段（数组）
- 支持多种数据类型：字符串、数字、布尔值
- 字段按名称排序后计算哈希（确保一致性）
- 数组值按源文档中的顺序保持

**哈希计算过程**：
```java
// 1. 提取所有匹配 routing_path 的字段
// 2. 对每个字段值计算哈希
hash(fieldName) = murmurhash3_x86_32(fieldName)
hash(fieldValue) = murmurhash3_x86_32(fieldValue)

// 3. 按字段名排序
// 4. 组合哈希
finalHash = 0
for each (name, valueHash) in sorted fields:
    finalHash = 31 * finalHash + (hash(name) ^ valueHash)
```

**文档ID生成**：
```java
// ID格式：[4字节哈希值][后缀] 的 Base64 编码
byte[] idBytes = new byte[4 + suffix.length];
ByteUtils.writeIntLE(hash, idBytes, 0);
System.arraycopy(suffix, 0, idBytes, 4, suffix.length);
String id = Base64.getUrlEncoder().withoutPadding().encodeToString(idBytes);
```

**操作支持**：
- ✅ `indexShard`：索引文档（从源提取路由）
- ❌ `updateShard`：不支持（抛出异常）
- ✅ `deleteShard`：删除文档（从ID解码哈希）
- ✅ `getShard`：获取文档（从ID解码哈希）
- ❌ `collectSearchShards`：不支持指定routing的搜索
- ❌ `checkIndexSplitAllowed`：不支持索引分裂

**限制**：
- 不支持 `routing_partition_size`（两者互斥）
- 不支持更新操作（因为无法从更新请求中提取完整的routing字段）
- 不支持索引分裂操作
- 不支持指定routing的搜索

**示例**：
```json
// 索引配置
{
  "settings": {
    "index.mode": "time_series",
    "index.routing_path": ["host.name", "container.id"]
  }
}

// 文档
{
  "host": {
    "name": "server-01"
  },
  "container": {
    "id": "container-123"
  },
  "metrics": {
    "cpu": 80
  }
}

// 路由计算
1. 提取字段：
   - "host.name" = "server-01"
   - "container.id" = "container-123"

2. 计算哈希：
   - hash("host.name") = 123456
   - hash("server-01") = 789012
   - hash("container.id") = 345678
   - hash("container-123") = 901234

3. 排序并组合：
   - "container.id" < "host.name" (字典序)
   - hash = 0
   - hash = 31 * 0 + (345678 ^ 901234) = ...
   - hash = 31 * ... + (123456 ^ 789012) = finalHash

4. 生成ID：
   - idBytes = [finalHash的4字节] + [随机后缀]
   - id = Base64(idBytes)

5. 计算分片：
   - shardId = finalHash % routingNumShards / routingFactor
```

## 4. 哈希算法

### 4.1 Murmur3 哈希函数

Elasticsearch 使用 **Murmur3 x86_32** 哈希算法，这是一个非加密哈希函数，具有以下特点：

**特性**：
- 快速计算
- 良好的分布特性
- 低碰撞率
- 确定性（相同输入总是产生相同输出）

**实现**（`Murmur3HashFunction.java`）：
```java
public static int hash(String routing) {
    final int strLen = routing.length();
    final byte[] bytesToHash = strLen * 2 <= MAX_SCRATCH_SIZE
        ? scratch.get()
        : new byte[strLen * 2];

    // 将字符串转换为字节数组（小端序）
    for (int i = 0; i < strLen; ++i) {
        ByteUtils.LITTLE_ENDIAN_CHAR.set(bytesToHash, 2 * i, routing.charAt(i));
    }

    // 调用 Lucene 的 Murmur3 实现
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, 0);
}
```

**优化**：
- 使用 ThreadLocal 缓存字节数组（避免频繁分配）
- 最大缓存大小：1024 字节
- 超过缓存大小时动态分配

### 4.2 哈希到分片的映射

```java
protected final int hashToShardId(int hash) {
    return Math.floorMod(hash, routingNumShards) / routingFactor;
}
```

**参数说明**：
- `hash`：Murmur3 计算的哈希值（32位整数）
- `routingNumShards`：路由分片数（通常是实际分片数的倍数）
- `routingFactor`：路由因子 = routingNumShards / numberOfShards

**计算步骤**：
1. 使用 `Math.floorMod` 确保结果为非负数
2. 对 `routingNumShards` 取模，得到虚拟分片ID
3. 除以 `routingFactor`，映射到实际分片ID

**示例**：
```
numberOfShards = 5
routingNumShards = 1024
routingFactor = 1024 / 5 = 204.8 ≈ 204

hash = -123456789
virtualShardId = Math.floorMod(-123456789, 1024) = 789
actualShardId = 789 / 204 = 3
```

## 5. 文档操作流程

### 5.1 索引文档（Index）

**流程**：
```
1. IndexRequest.process(indexRouting)
   ↓
2. indexRouting.process(indexRequest)
   - 生成文档ID（如果未提供）
   - 检查ID是否为空
   ↓
3. indexRouting.indexShard(id, routing, sourceType, source, routingHashSetter)
   - 检查routing是否必需
   - 计算分片ID
   - 设置routing hash（时间序列索引）
   ↓
4. 返回分片ID
```

**ID生成策略**：
- **标准模式**：`UUIDs.base64UUID()` - 随机UUID的Base64编码
- **LogsDB模式**（IndexVersion >= TIME_BASED_K_ORDERED_DOC_ID_BACKPORT）：
  - `UUIDs.base64TimeBasedKOrderedUUID()` - 基于时间的K-ordered UUID
  - 优势：时间局部性，提高写入性能

**Routing检查**：
```java
if (routingRequired && routing == null) {
    throw new RoutingMissingException(indexName, id);
}
```

### 5.2 更新文档（Update）

**流程**：
```
1. updateShard(id, routing)
   ↓
2. 检查routing是否必需
   ↓
3. 计算分片ID（与索引相同的算法）
   ↓
4. 返回分片ID
```

**注意**：
- ExtractFromSource 策略不支持更新操作
- 必须提供文档ID
- 如果索引要求routing，必须提供相同的routing值

### 5.3 删除文档（Delete）

**流程**：
```
1. deleteShard(id, routing)
   ↓
2. 检查routing是否必需
   ↓
3. 计算分片ID
   ↓
4. 返回分片ID
```

**时间序列索引的特殊处理**：
```java
// 从Base64编码的ID中提取哈希值
byte[] idBytes = Base64.getUrlDecoder().decode(id);
int hash = ByteUtils.readIntLE(idBytes, 0);
return hashToShardId(hash);
```

### 5.4 获取文档（Get）

**流程**：
```
1. getShard(id, routing)
   ↓
2. 检查routing是否必需
   ↓
3. 计算分片ID
   ↓
4. 返回分片ID
```

**与删除操作相同的逻辑**

### 5.5 搜索操作（Search）

**流程**：
```
1. collectSearchShards(routing, consumer)
   ↓
2. 根据策略收集分片ID
   - Unpartitioned: 单个分片
   - Partitioned: routingPartitionSize 个分片
   - ExtractFromSource: 不支持（抛出异常）
   ↓
3. 通过consumer回调返回分片ID
```

**分区索引的搜索**：
```java
int hash = effectiveRoutingToHash(routing);
for (int i = 0; i < routingPartitionSize; i++) {
    consumer.accept(hashToShardId(hash + i));
}
```

## 6. 时间序列索引特性

### 6.1 Routing Hash字段

**字段名**：`_ts_routing_hash`

**用途**：
- 存储路由哈希值
- 用于重建文档ID
- 支持按ID的Get和Delete操作

**编码方式**：
```java
public static String encode(int routingId) {
    byte[] bytes = new byte[4];
    ByteUtils.writeIntLE(routingId, bytes, 0);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
}
```

**解码方式**：
```java
public static int decode(String routingId) {
    byte[] bytes = Base64.getUrlDecoder().decode(routingId);
    return ByteUtils.readIntLE(bytes, 0);
}
```

### 6.2 文档ID结构

**格式**：`[4字节路由哈希][N字节后缀]` 的 Base64 URL-safe 编码

**优势**：
- 包含路由信息，无需额外的routing参数
- 支持高效的Get和Delete操作
- 保持ID的唯一性

**示例**：
```
路由哈希: 0x12345678
后缀: 随机字节
ID字节: [0x78, 0x56, 0x34, 0x12, ...后缀...]
Base64编码: "eFY0Ei..."
```

### 6.3 特性标志

**BOOLEAN_ROUTING_PATH**：
- 支持布尔类型的routing_path字段

**MULTI_VALUE_ROUTING_PATH**：
- 支持多值（数组）routing_path字段

## 7. 路由策略选择

### 7.1 决策树

```
IndexMetadata
    ↓
是否配置了 routing_path?
    ├─ 是 → ExtractFromSource
    └─ 否 → 是否配置了 routing_partition_size?
              ├─ 是 → Partitioned
              └─ 否 → Unpartitioned
```

### 7.2 工厂方法

```java
public static IndexRouting fromIndexMetadata(IndexMetadata metadata) {
    if (false == metadata.getRoutingPaths().isEmpty()) {
        return new ExtractFromSource(metadata);
    }
    if (metadata.isRoutingPartitionedIndex()) {
        return new Partitioned(metadata);
    }
    return new Unpartitioned(metadata);
}
```

## 8. 最佳实践

### 8.1 选择合适的路由策略

**Unpartitioned（默认）**：
- ✅ 适用于大多数场景
- ✅ 简单直接
- ✅ 支持所有操作
- ❌ 可能导致数据倾斜（如果routing值分布不均）

**Partitioned**：
- ✅ 改善数据分布
- ✅ 避免热点分片
- ✅ 适合有明确routing需求的场景
- ❌ 搜索需要查询多个分片
- ❌ 必须提供routing参数

**ExtractFromSource**：
- ✅ 适合时间序列数据
- ✅ 自动从文档提取路由
- ✅ 无需显式routing参数
- ❌ 不支持更新操作
- ❌ 不支持索引分裂
- ❌ 配置复杂

### 8.2 Routing参数使用建议

1. **明确routing需求**：
   - 如果数据有自然的分组（如用户ID、租户ID），考虑使用routing
   - 评估数据分布是否均匀

2. **一致性**：
   - 索引、更新、删除、获取操作必须使用相同的routing值
   - 建议在应用层封装routing逻辑

3. **搜索优化**：
   - 如果查询总是针对特定routing值，指定routing可以减少查询的分片数
   - 注意分区索引会查询多个分片

4. **避免热点**：
   - 避免使用分布不均的routing值（如时间戳）
   - 考虑使用routing_partition_size改善分布

### 8.3 时间序列索引配置

```json
{
  "settings": {
    "index": {
      "mode": "time_series",
      "routing_path": ["host.name", "container.id"],
      "number_of_shards": 5,
      "number_of_replicas": 1
    }
  },
  "mappings": {
    "properties": {
      "host": {
        "properties": {
          "name": {
            "type": "keyword",
            "time_series_dimension": true
          }
        }
      },
      "container": {
        "properties": {
          "id": {
            "type": "keyword",
            "time_series_dimension": true
          }
        }
      },
      "metrics": {
        "properties": {
          "cpu": {
            "type": "double",
            "time_series_metric": "gauge"
          }
        }
      }
    }
  }
}
```

**注意事项**：
- `routing_path` 字段必须是 `time_series_dimension`
- 不能同时配置 `routing_partition_size`
- 索引创建后不能修改 `routing_path`

## 9. 常见问题

### 9.1 RoutingMissingException

**原因**：索引配置要求routing，但请求未提供

**解决**：
```java
// 检查mapping配置
GET /my-index/_mapping

// 确保请求包含routing
POST /my-index/_doc/1?routing=user123
{
  "field": "value"
}
```

### 9.2 时间序列索引不支持更新

**原因**：ExtractFromSource策略无法从更新请求中提取完整的routing字段

**解决**：
- 使用删除+重新索引的方式
- 或者重新考虑是否真的需要时间序列模式

### 9.3 分片分布不均

**原因**：routing值分布不均匀

**解决**：
- 使用 `routing_partition_size` 改善分布
- 选择更均匀的routing字段
- 增加分片数量

### 9.4 ID解码失败

**错误**：`ResourceNotFoundException: invalid id [xxx] for index [yyy] in time series mode`

**原因**：
- ID不是有效的Base64编码
- ID长度小于4字节
- 尝试在时间序列索引中使用非时间序列生成的ID

**解决**：
- 确保使用正确的ID格式
- 不要手动构造时间序列索引的ID

## 10. 性能考虑

### 10.1 哈希计算性能

- Murmur3 是快速的非加密哈希算法
- ThreadLocal缓存减少内存分配
- 字符串转字节使用小端序（与大多数现代CPU一致）

### 10.2 路由策略性能对比

| 策略 | 计算复杂度 | 内存开销 | 搜索效率 |
|------|-----------|---------|---------|
| Unpartitioned | O(1) | 低 | 高（单分片） |
| Partitioned | O(1) | 低 | 中（多分片） |
| ExtractFromSource | O(n) | 中（需要解析源） | 高（单分片） |

### 10.3 优化建议

1. **避免频繁解析源文档**：
   - ExtractFromSource 需要解析JSON，相对较慢
   - 适合写入密集型场景，而非实时查询

2. **合理设置分片数**：
   - 过多分片增加协调开销
   - 过少分片可能导致单分片过大

3. **使用routing优化搜索**：
   - 指定routing可以减少查询的分片数
   - 但要确保routing值的正确性

## 11. 总结

Elasticsearch 的 IndexRouting 提供了灵活而强大的路由机制：

1. **三种策略**：Unpartitioned（默认）、Partitioned（分区）、ExtractFromSource（时间序列）
2. **核心算法**：Murmur3 哈希 + 模运算
3. **关键参数**：id、routing、routingNumShards、routingFactor
4. **操作支持**：索引、更新、删除、获取、搜索（根据策略有所不同）
5. **特殊特性**：时间序列索引的自动路由提取和ID编码

选择合适的路由策略需要考虑：
- 数据访问模式
- 数据分布特征
- 操作类型需求
- 性能要求

理解路由机制对于优化 Elasticsearch 的性能和数据分布至关重要。
