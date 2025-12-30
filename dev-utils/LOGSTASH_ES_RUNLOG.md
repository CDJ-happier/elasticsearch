# 使用Logstash收集Elasticsearch运行日志

本文档介绍如何使用Logstash将Elasticsearch运行日志收集并存储到Elasticsearch本身，使用ILM策略和索引模板进行管理。

## 1. 准备工作

确保您已安装Logstash，并了解以下信息：
- Logstash安装路径：`/Users/cdj/Downloads/app/logstash-9.1.2/`
- Elasticsearch运行在9200端口
- Elasticsearch日志文件路径：`/Users/cdj/code/work/elasticsearch/dev-utils/logs/`
- 日志文件匹配模式：`es*.log`

## 2. 创建ILM策略

首先，我们需要创建一个ILM策略来管理日志索引的生命周期。执行以下REST API命令：

```bash
curl -X PUT "localhost:9200/_ilm/policy/es_runlog_ilm_policy" \
-H 'Content-Type: application/json' -d'
{
  "policy": {
    "phases": {
      "hot": {
        "min_age": "0ms",
        "actions": {
          "rollover": {
            "max_age": "1d",
            "max_size": "512mb",
            "max_docs": 100000
          },
          "set_priority": {
            "priority": 100
          }
        }
      },
      "delete": {
        "min_age": "2d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}'
```

## 3. 创建索引模板

接下来，创建一个索引模板，指定索引的设置、映射和关联的ILM策略：

```bash
curl -X POST "localhost:9200/_index_template/es_runlog_template" \
-H 'Content-Type: application/json' -d'
{
  "index_patterns": ["es-runlog-*"],
  "template": {
    "settings": {
      "number_of_shards": 2,
      "number_of_replicas": 1,
      "index.lifecycle.name": "es_runlog_ilm_policy",
      "index.lifecycle.rollover_alias": "es-runlog"
    },
    "mappings": {
      "properties": {
        "@timestamp": {
          "type": "date"
        },
        "timestamp": {
          "type": "date",
          "format": "yyyy-MM-dd'\''T'\''HH:mm:ss,SSS"
        },
        "loglevel": {
          "type": "keyword"
        },
        "logger": {
          "type": "keyword"
        },
        "node": {
          "type": "keyword"
        },
        "message": {
          "type": "text"
        },
        "component": {
          "type": "keyword"
        }
      }
    }
  }
}'
```

## 4. 配置Logstash

创建或修改Logstash配置文件`/Users/cdj/code/work/elasticsearch/dev-utils/config/logstash.conf`，内容如下：

```ruby
input {
  file {
    path => [
      "/Users/cdj/code/work/elasticsearch/dev-utils/logs/es*/es*.log"
    ]
    start_position => "beginning"
    codec => multiline {
      pattern => "^\["
      negate => true
      what => "previous"
    }
    tags => ["es_runlog"]
  }
}

filter {
  # 解析ES日志格式
  grok {
    match => {
      "message" => "\[%{TIMESTAMP_ISO8601:timestamp}\]\[%{LOGLEVEL:loglevel}%{SPACE}\]\[%{DATA:logger}%{SPACE}\] \[%{DATA:node}\]%{GREEDYDATA:message}"
    }
    overwrite => ["message"]
  }

  # 提取组件信息（如o.e.n.Node）
  if [logger] {
    grok {
      match => { "logger" => "%{WORD:component}\.%{DATA:subcomponent}" }
    }
  }

  # 将解析出的时间戳设置为 @timestamp
  date {
    match => ["timestamp", "ISO8601"]
    target => "@timestamp"
  }

  # 移除临时字段
  mutate {
    remove_field => ["timestamp"]
  }
}

output {
  elasticsearch {
    hosts => ["http://localhost:9200"]
    index => "es-runlog"
    # 如果启用了安全特性，请取消注释以下行并提供凭据
    # user => "your_username"
    # password => "your_password"
  }

  # 调试时可以取消注释以下行以在控制台查看输出
  # stdout { codec => rubydebug }
}
```

## 5. 创建初始索引

在启动Logstash之前，需要手动创建第一个索引，以便ILM可以正确管理索引生命周期：

```bash
curl -X PUT "localhost:9200/es-runlog-000001" \
-H 'Content-Type: application/json' -d'
{
  "aliases": {
    "es-runlog": {
      "is_write_index": true
    }
  }
}'
```

## 6. 启动Logstash

使用以下命令启动Logstash：

```bash
cd /Users/cdj/Downloads/app/logstash-9.1.2/
bin/logstash -f /Users/cdj/code/work/elasticsearch/dev-utils/config/logstash.conf
```

## 7. 验证配置

创建一个测试索引来验证映射是否正确：

```bash
curl -X PUT "localhost:9200/es-runlog-test" \
-H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 2,
    "number_of_replicas": 1,
    "index.lifecycle.name": "es_runlog_ilm_policy",
    "index.lifecycle.rollover_alias": "es-runlog"
  }
}'
```

检查索引映射：

```bash
curl -X GET "localhost:9200/es-runlog-test/_mapping?pretty"
```

## 8. 监控和故障排除

1. 检查ILM策略是否创建成功：
```bash
curl -X GET "localhost:9200/_ilm/policy/es_runlog_ilm_policy?pretty"
```

2. 查看索引模板：
```bash
curl -X GET "localhost:9200/_index_template/es_runlog_template?pretty"
```

3. 检查是否有数据进入：
```bash
curl -X GET "localhost:9200/es-runlog*/_search?pretty"
```

4. 检查初始索引是否创建成功：
```bash
curl -X GET "localhost:9200/_cat/indices/es-runlog*?v"
```

5. 检查别名是否正确设置：
```bash
curl -X GET "localhost:9200/_cat/aliases/es-runlog?v"
```

## 9. 常见问题排查

如果Logstash启动后没有数据写入，请按以下步骤排查：

### 9.1 检查日志文件路径
确认日志文件是否存在以及路径是否正确：
```bash
ls -la /Users/cdj/code/work/elasticsearch/dev-utils/logs/es*/es*.log
```

### 9.2 检查Logstash配置语法
验证Logstash配置文件语法是否正确：
```bash
cd /Users/cdj/Downloads/app/logstash-9.1.2/
bin/logstash -t -f /Users/cdj/code/work/elasticsearch/dev-utils/config/logstash.conf
```

### 9.3 检查Logstash日志
查看Logstash运行日志，检查是否有错误信息：
```bash
tail -f /Users/cdj/Downloads/app/logstash-9.1.2/logs/logstash-plain.log
```

### 9.4 测试文件读取
使用简单的Logstash配置测试文件读取是否正常：
```ruby
input {
  file {
    path => "/Users/cdj/code/work/elasticsearch/dev-utils/logs/es*/es*.log"
    start_position => "beginning"
    sincedb_path => "/dev/null"
  }
}

output {
  stdout { codec => rubydebug }
}
```

### 9.5 检查Elasticsearch连接
确认Elasticsearch是否正常运行并可以访问：
```bash
curl -X GET "localhost:9200/_cluster/health?pretty"
```

### 9.6 检查索引是否存在写入权限
确认索引和别名是否正确设置，是否有写入权限：
```bash
curl -X GET "localhost:9200/es-runlog*/_settings?pretty"
```

### 9.7 手动测试数据写入
尝试手动向索引写入测试数据：
```bash
curl -X POST "localhost:9200/es-runlog/_doc" \
-H 'Content-Type: application/json' -d'
{
  "@timestamp": "2023-01-01T12:00:00Z",
  "message": "Test message",
  "loglevel": "INFO"
}'
```

## 10. 注意事项

1. **日志轮转**：确保Logstash配置正确处理日志轮转。
2. **性能调优**：根据日志量调整Logstash的批处理大小和工作线程数。
3. **安全设置**：如果Elasticsearch启用了安全功能，需要在Logstash配置中提供相应的认证信息。
4. **索引生命周期**：根据实际需求调整ILM策略中的保留时间和存储大小限制。
