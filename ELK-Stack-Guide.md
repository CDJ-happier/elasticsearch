# ELK Stack 环境说明文档

> **文档创建时间**: 2025-12-30
>
> **环境位置**: `/data1/elk`
>
> **维护者**: jasondjcai

---

## 📋 目录

- [1. 环境概览](#1-环境概览)
- [2. Elasticsearch 集群](#2-elasticsearch-集群)
- [3. Cerebro 管理工具](#3-cerebro-管理工具)
- [4. Kibana 可视化平台](#4-kibana-可视化平台)
- [5. 组件关系与架构](#5-组件关系与架构)
- [6. 快速启动指南](#6-快速启动指南)
- [7. 常见问题与故障排查](#7-常见问题与故障排查)

---

## 1. 环境概览

### 1.1 安装目录结构

```shell
/data1/elk/
├── elasticsearch/              # Elasticsearch 源码目录
│   └── dev-utils/             # 开发工具集
│       ├── es-cluster-manager.sh  # 集群管理脚本
│       ├── config/            # 集群配置目录
│       │   └── clusters/
│       │       └── es-debug/  # es-debug 集群配置
│       ├── data/              # 数据存储目录
│       └── logs/              # 日志目录
├── cerebro-0.9.4/             # Cerebro Web 管理界面
└── kibana-8.17.10/            # Kibana 数据可视化平台
    ├── config/                # 配置目录
    ├── data/                  # 数据目录
    └── logs/                  # 日志目录
```

### 1.2 组件版本信息

| 组件 | 版本 | 端口 | 状态 |
|------|------|------|------|
| **Elasticsearch** | 8.17.11-SNAPSHOT | 31920, 31921, 31922 | ✅ 运行中 |
| **Cerebro** | 0.9.4 | 31900 | ✅ 运行中 |
| **Kibana** | 8.17.10 | 31560 | ✅ 运行中 |

不知道什么原因，在mac上访问开发机上的es、cerebro这些服务时，如果端口是9200之类的，访问不了。
将端口修改较大值时，就可以，貌似被k3s的配置影响了？

---

## 2. Elasticsearch 集群

### 2.1 集群信息

- **集群名称**: `es-debug`
- **集群状态**: 🟢 Green (健康)
- **节点数量**: 3 个数据节点
- **版本**: 8.17.11-SNAPSHOT
- **认证**: ❌ 未启用 (开发环境)

### 2.2 节点配置

#### Node 1 (es-debug-node1)
- **HTTP 端口**: 31920
- **Transport 端口**: 9300
- **配置文件**: `/data1/elk/elasticsearch/dev-utils/config/clusters/es-debug/es-debug-node1.yml`
- **数据目录**: `/data1/elk/elasticsearch/dev-utils/data/es-debug/es-debug-node1`
- **日志目录**: `/data1/elk/elasticsearch/dev-utils/logs/es-debug/es-debug-node1`

#### Node 2 (es-debug-node2)
- **HTTP 端口**: 31921
- **Transport 端口**: 9301
- **配置文件**: `/data1/elk/elasticsearch/dev-utils/config/clusters/es-debug/es-debug-node2.yml`
- **数据目录**: `/data1/elk/elasticsearch/dev-utils/data/es-debug/es-debug-node2`
- **日志目录**: `/data1/elk/elasticsearch/dev-utils/logs/es-debug/es-debug-node2`

#### Node 3 (es-debug-node3)
- **HTTP 端口**: 31922
- **Transport 端口**: 9302
- **配置文件**: `/data1/elk/elasticsearch/dev-utils/config/clusters/es-debug/es-debug-node3.yml`
- **数据目录**: `/data1/elk/elasticsearch/dev-utils/data/es-debug/es-debug-node3`
- **日志目录**: `/data1/elk/elasticsearch/dev-utils/logs/es-debug/es-debug-node3`

### 2.3 管理命令

#### 使用 esctl 工具管理集群

```bash
# 进入 Elasticsearch 目录
cd /data1/elk/elasticsearch

# 启动集群
esctl start es-debug

# 停止集群
esctl stop es-debug

# 强制停止集群（解决锁文件问题）
esctl stop es-debug --force

# 重启集群
esctl restart es-debug

# 查看集群状态
esctl status es-debug

# 查看集群列表
esctl list
```

#### 直接使用 API 查询

```bash
# 查看集群健康状态
curl http://localhost:31920/_cluster/health?pretty

# 查看节点信息
curl http://localhost:31920/_cat/nodes?v

# 查看所有索引
curl http://localhost:31920/_cat/indices?v

# 查看集群设置
curl http://localhost:31920/_cluster/settings?pretty
```

### 2.4 集群健康指标

当前集群状态：
```json
{
  "cluster_name": "es-debug",
  "status": "green",
  "number_of_nodes": 3,
  "number_of_data_nodes": 3,
  "active_primary_shards": 28,
  "active_shards": 56,
  "active_shards_percent_as_number": 100.0
}
```

---

## 3. Cerebro 管理工具

### 3.1 简介

Cerebro 是一个轻量级的 Elasticsearch Web 管理界面，提供：
- 集群监控和健康检查
- 索引管理（创建、删除、优化）
- 节点信息查看
- REST API 查询工具
- 快照和恢复管理

### 3.2 访问信息

- **访问地址**: `http://localhost:31900` 或 `http://<服务器IP>:31900`
- **版本**: 0.9.4
- **安装目录**: `/data1/elk/cerebro-0.9.4`

### 3.3 启动命令

```bash
# 前台启动
cd /data1/elk/cerebro-0.9.4
./bin/cerebro

# 后台启动
cd /data1/elk/cerebro-0.9.4
nohup ./bin/cerebro > logs/cerebro.log 2>&1 &

# 查看进程
ps aux | grep cerebro | grep -v grep

# 停止服务
pkill -f cerebro
```

### 3.4 连接 Elasticsearch

在 Cerebro Web 界面中输入：
```
http://localhost:31920
```
或者任意一个 ES 节点地址：
- `http://localhost:31921`
- `http://localhost:31922`

---

## 4. Kibana 可视化平台

### 4.1 简介

Kibana 是 Elasticsearch 的官方可视化平台，提供：
- 数据探索和可视化
- 仪表板创建
- 日志分析
- 机器学习功能
- 告警和监控
- Dev Tools (REST API 控制台)

### 4.2 访问信息

- **访问地址**: `http://localhost:31560` 或 `http://<服务器IP>:31560`
- **版本**: 8.17.10
- **安装目录**: `/data1/elk/kibana-8.17.10`
- **配置文件**: `/data1/elk/kibana-8.17.10/config/kibana.yml`

### 4.3 配置详情

```yaml
# 服务器配置
server.port: 31560
server.host: "0.0.0.0"
server.name: "kibana-dev"

# Elasticsearch 连接配置
elasticsearch.hosts:
  - "http://localhost:31920"
  - "http://localhost:31921"
  - "http://localhost:31922"

# 数据和日志路径
path.data: /data1/elk/kibana-8.17.10/data
pid.file: /data1/elk/kibana-8.17.10/kibana.pid

# 日志配置
logging.root.level: info

# 国际化
i18n.locale: "zh-CN"
```

### 4.4 启动命令

```bash
# 前台启动（用于调试）
/data1/elk/kibana-8.17.10/bin/kibana

# 后台启动（推荐）
nohup /data1/elk/kibana-8.17.10/bin/kibana > /data1/elk/kibana-8.17.10/logs/kibana.log 2>&1 &

# 查看启动日志
tail -f /data1/elk/kibana-8.17.10/logs/kibana.log

# 查看进程
ps aux | grep kibana | grep -v grep

# 停止服务
pkill -f kibana

# 或者使用 PID 文件停止
kill \$(cat /data1/elk/kibana-8.17.10/kibana.pid)
```

### 4.5 验证连接

```bash
# 检查 Kibana 状态
curl http://localhost:31560/api/status | jq .

# 检查 Elasticsearch 连接
curl http://localhost:31560/api/status | jq '.status.statuses[] | select(.id=="elasticsearch")'
```

---

## 5. 组件关系与架构

### 5.1 架构图

```
┌─────────────────────────────────────────────────────────────┐
│                         用户/开发者                          │
└────────────┬──────────────────┬─────────────────┬───────────┘
             │                  │                 │
             │ HTTP:31560       │ HTTP:31900      │ HTTP:31920-31922
             ▼                  ▼                 ▼
    ┌────────────────┐  ┌──────────────┐  ┌─────────────────┐
    │    Kibana      │  │   Cerebro    │  │  Elasticsearch  │
    │   (8.17.10)    │  │   (0.9.4)    │  │  (8.17.11)      │
    │                │  │              │  │                 │
    │  - 数据可视化   │  │  - 集群管理   │  │  - 数据存储      │
    │  - 仪表板      │  │  - 索引管理   │  │  - 搜索引擎      │
    │  - Dev Tools   │  │  - 监控工具   │  │  - 分布式集群    │
    └────────┬───────┘  └──────┬───────┘  └─────────────────┘
             │                  │                 ▲
             │ REST API         │ REST API        │
             └──────────────────┴─────────────────┘
                                │
                    ┌───────────┴───────────┐
                    │                       │
              ┌─────▼─────┐  ┌──────▼──────┐  ┌──────▼──────┐
              │  Node 1   │  │   Node 2    │  │   Node 3    │
              │ :31920    │  │  :31921     │  │  :31922     │
              └───────────┘  └─────────────┘  └─────────────┘
                    │                │                │
                    └────────────────┴────────────────┘
                           Transport: 9300-9302
```

### 5.2 组件关系说明

#### Elasticsearch (核心数据层)
- **角色**: 分布式搜索和分析引擎
- **功能**:
  - 存储和索引数据
  - 提供 RESTful API
  - 处理搜索和聚合查询
  - 集群自动管理和故障转移

#### Kibana (可视化层)
- **角色**: 数据可视化和探索平台
- **依赖**: Elasticsearch
- **连接方式**: HTTP REST API
- **功能**:
  - 通过 REST API 与 ES 通信
  - 读取和写入 ES 数据
  - 创建可视化图表和仪表板
  - 提供 Dev Tools 控制台

#### Cerebro (管理层)
- **角色**: 轻量级集群管理工具
- **依赖**: Elasticsearch
- **连接方式**: HTTP REST API
- **功能**:
  - 监控集群健康状态
  - 管理索引和分片
  - 执行集群操作
  - 提供简洁的 Web 界面

### 5.3 数据流向

```
用户操作 → Kibana/Cerebro → HTTP Request → Elasticsearch → 处理请求 → 返回结果
```

1. **写入流程**: 用户 → Kibana → ES 集群 → 数据分片存储
2. **查询流程**: 用户 → Kibana/Cerebro → ES 集群 → 聚合结果 → 返回展示
3. **管理流程**: 管理员 → Cerebro → ES 集群 → 执行管理操作

### 5.4 网络连接

| 源组件 | 目标组件 | 协议 | 端口          | 用途 |
|--------|---------|------|-------------|------|
| Kibana | Elasticsearch | HTTP | 31920-31922 | 数据查询和写入 |
| Cerebro | Elasticsearch | HTTP | 31920-31922 | 集群管理 |
| 用户浏览器 | Kibana | HTTP | 31560       | Web 访问 |
| 用户浏览器 | Cerebro | HTTP | 31900       | Web 访问 |
| ES Node 间 | ES Node | TCP | 9300-9302   | 集群内部通信 |

---

## 6. 快速启动指南

### 6.1 完整启动流程

```bash
# 1. 启动 Elasticsearch 集群
cd /data1/elk/elasticsearch
esctl start es-debug

# 2. 验证 ES 集群状态
curl http://localhost:31920/_cluster/health?pretty

# 3. 启动 Cerebro（可选）
cd /data1/elk/cerebro-0.9.4
nohup ./bin/cerebro > logs/cerebro.log 2>&1 &

# 4. 启动 Kibana
nohup /data1/elk/kibana-8.17.10/bin/kibana > /data1/elk/kibana-8.17.10/logs/kibana.log 2>&1 &

# 5. 等待 Kibana 启动完成（约 30-60 秒）
tail -f /data1/elk/kibana-8.17.10/logs/kibana.log

# 6. 验证所有服务
curl http://localhost:31920  # Elasticsearch
curl http://localhost:31900  # Cerebro
curl http://localhost:31560/api/status  # Kibana
```

### 6.2 快速停止流程

```bash
# 1. 停止 Kibana
pkill -f kibana

# 2. 停止 Cerebro
pkill -f cerebro

# 3. 停止 Elasticsearch 集群
cd /data1/elk/elasticsearch
esctl stop es-debug

# 如果遇到锁文件问题，使用强制停止
esctl stop es-debug --force
```

### 6.3 一键启动脚本

创建启动脚本 `/data1/elk/start-all.sh`:

```bash
#!/bin/bash

echo "=== 启动 ELK Stack ==="

# 1. 启动 Elasticsearch
echo "[1/3] 启动 Elasticsearch 集群..."
cd /data1/elk/elasticsearch
esctl start es-debug

# 等待 ES 启动
sleep 10

# 2. 启动 Cerebro
echo "[2/3] 启动 Cerebro..."
cd /data1/elk/cerebro-0.9.4
nohup ./bin/cerebro > logs/cerebro.log 2>&1 &

# 3. 启动 Kibana
echo "[3/3] 启动 Kibana..."
nohup /data1/elk/kibana-8.17.10/bin/kibana > /data1/elk/kibana-8.17.10/logs/kibana.log 2>&1 &

echo ""
echo "=== 启动完成 ==="
echo "Elasticsearch: http://localhost:31920"
echo "Cerebro:       http://localhost:31900"
echo "Kibana:        http://localhost:31560"
echo ""
echo "等待 Kibana 完全启动（约 30-60 秒）..."
```

创建停止脚本 `/data1/elk/stop-all.sh`:

```bash
#!/bin/bash

echo "=== 停止 ELK Stack ==="

# 1. 停止 Kibana
echo "[1/3] 停止 Kibana..."
pkill -f kibana

# 2. 停止 Cerebro
echo "[2/3] 停止 Cerebro..."
pkill -f cerebro

# 3. 停止 Elasticsearch
echo "[3/3] 停止 Elasticsearch..."
cd /data1/elk/elasticsearch
esctl stop es-debug --force

echo ""
echo "=== 停止完成 ==="
```

使用方法：
```bash
# 赋予执行权限
chmod +x /data1/elk/start-all.sh
chmod +x /data1/elk/stop-all.sh

# 启动所有服务
/data1/elk/start-all.sh

# 停止所有服务
/data1/elk/stop-all.sh
```

---

## 7. 常见问题与故障排查

### 7.1 Elasticsearch 启动失败

#### 问题：锁文件错误
```
java.lang.IllegalStateException: environment is not locked
java.nio.file.NoSuchFileException: .../node.lock
```

**解决方案**:
```bash
# 强制停止集群
esctl stop es-debug --force

# 重新启动
esctl start es-debug
```

#### 问题：端口被占用
```bash
# 检查端口占用
netstat -tlnp | grep -E "31920|31921|31922"

# 或使用 lsof
lsof -i :31920
```

### 7.2 Kibana 连接失败

#### 问题：版本不兼容
```
This version of Kibana (v9.2.3) is incompatible with Elasticsearch v8.17.11
```

**解决方案**: 确保 Kibana 主版本号与 ES 一致（8.x 对 8.x）

#### 问题：权限错误
```
Error: EACCES: permission denied, mkdir '/data1/elk/kibana-x.x.x'
```

**解决方案**: 检查配置文件中的路径是否正确
```bash
# 检查 kibana.yml 中的路径
grep -E "path.data|pid.file" /data1/elk/kibana-8.17.10/config/kibana.yml

# 确保路径指向正确的 Kibana 版本目录
```

### 7.3 服务状态检查

```bash
# 检查所有 ELK 进程
ps aux | grep -E "elasticsearch|kibana|cerebro" | grep -v grep

# 检查端口监听
netstat -tlnp | grep -E "31920|31921|31922|31900|31560"

# 检查 ES 集群健康
curl http://localhost:31920/_cluster/health?pretty

# 检查 Kibana 状态
curl http://localhost:31560/api/status | jq '.status.overall.state'
```

### 7.4 日志查看

```bash
# Elasticsearch 日志
tail -f /data1/elk/elasticsearch/dev-utils/logs/es-debug/es-debug-node1/es-debug.log

# Kibana 日志
tail -f /data1/elk/kibana-8.17.10/logs/kibana.log

# Cerebro 日志
tail -f /data1/elk/cerebro-0.9.4/logs/cerebro.log
```

### 7.5 性能优化建议

1. **JVM 堆内存调整**
   ```bash
   # 编辑 ES JVM 配置
   vim /data1/elk/elasticsearch/dev-utils/config/clusters/es-debug/es-debug-node1.jvm.options

   # 设置堆内存（建议为物理内存的 50%，不超过 32GB）
   -Xms4g
   -Xmx4g
   ```

2. **索引刷新间隔**
   ```bash
   # 对于批量导入，可以临时增加刷新间隔
   curl -X PUT "localhost:31920/my_index/_settings" -H 'Content-Type: application/json' -d'
   {
     "index": {
       "refresh_interval": "30s"
     }
   }'
   ```

3. **分片数量优化**
   - 单个分片大小建议：20-40GB
   - 每个节点分片数建议：不超过 1000

---

### 调试

在使用`esctl init`集群时，*.jvm.options中配置了如下内容：
```text
# for debugging
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
```
后续可以在idea中增加节点对应的调试配置，Debugger mode 选择 Attach to remote JVM, port填写对应节点*.jvm.option中配置的端口。

---

## 📚 附录

### A. 常用 API 端点

#### Elasticsearch
```bash
# 集群信息
GET http://localhost:31920/

# 集群健康
GET http://localhost:31920/_cluster/health

# 节点信息
GET http://localhost:31920/_cat/nodes?v

# 索引列表
GET http://localhost:31920/_cat/indices?v

# 集群统计
GET http://localhost:31920/_cluster/stats
```

#### Kibana
```bash
# 状态检查
GET http://localhost:31560/api/status

# 版本信息
GET http://localhost:31560/api/status
```

### B. 参考资源

- **Elasticsearch 官方文档**: https://www.elastic.co/guide/en/elasticsearch/reference/8.17/index.html
- **Kibana 官方文档**: https://www.elastic.co/guide/en/kibana/8.17/index.html
- **Cerebro GitHub**: https://github.com/lmenezes/cerebro

### C. 版本兼容性

| Kibana 版本 | 兼容的 ES 版本 |
|------------|---------------|
| 8.17.x     | 8.17.x        |
| 8.16.x     | 8.16.x        |
| 9.x.x      | 9.x.x         |

**重要**: Kibana 和 Elasticsearch 的主版本号必须相同！

---

## 📝 更新日志

- **2025-12-30**: 初始文档创建
  - 添加 Elasticsearch 8.17.11 集群配置
  - 添加 Kibana 8.17.10 配置
  - 添加 Cerebro 0.9.4 配置
  - 添加启动脚本和故障排查指南

---

**文档维护**: 请在每次重大配置变更后更新此文档。
