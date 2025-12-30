# 开发环境及操作搭建

## 环境要求

- **JDK版本**: JDK 23 (已使用 sdkman 安装 Zulu 23.0.2)
- **构建工具**: Gradle 8.14.2 (项目自带 wrapper)
- **操作系统**: Linux

## 环境安装

### 1. 安装 JDK 23

使用 sdkman 安装 JDK 23：

```bash
# 如果还没有安装 sdkman
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# 安装 JDK 23
sdk install java 23.0.2-zulu
sdk use java 23.0.2-zulu
```

### 2. 配置环境变量

TODO：都使用sdk了，为什么还需要手动设置这两个环境变量。
```bash
export JAVA_HOME=$HOME/.sdkman/candidates/java/23.0.2-zulu
export PATH=$JAVA_HOME/bin:$PATH
```

## 编译配置

### 问题解决

原始配置使用 Oracle OpenJDK 下载源，由于网络问题（SSL 错误）无法下载。

**解决方案**: 修改 `build-tools-internal/version.properties` 文件，将 JDK vendor 从 `openjdk` 改为 `adoptium`：

```properties
# 修改前
bundled_jdk_vendor = openjdk
bundled_jdk = 23+37@3c5b90190c68498b986a97f276efd28a

# 修改后
bundled_jdk_vendor = adoptium
bundled_jdk = 23.0.2+7
```

Adoptium (Eclipse Temurin) 提供更稳定的下载源。

## 编译命令

### 清理构建

```bash
./gradlew clean
```

### 编译本地发行版

```bash
# 前台编译（可以看到实时输出）
export JAVA_HOME=$HOME/.sdkman/candidates/java/23.0.2-zulu
./gradlew localDistro

# 后台编译（推荐用于长时间编译）
export JAVA_HOME=$HOME/.sdkman/candidates/java/23.0.2-zulu
nohup ./gradlew localDistro > build.log 2>&1 &

# 查看编译进度
tail -f build.log
```

### 其他常用命令

```bash
# 查看所有可用任务
./gradlew tasks

# 查看 Gradle 守护进程状态
./gradlew --status

# 停止所有 Gradle 守护进程
./gradlew --stop

# 编译特定模块
./gradlew :server:assemble

# 运行测试
./gradlew test
```

## 编译结果

编译成功后，产物位于：

```
build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/
```

目录结构：

```
elasticsearch-8.17.11-SNAPSHOT/
├── bin/              # 可执行文件
├── config/           # 配置文件
├── lib/              # 依赖库
├── modules/          # ES 模块
├── plugins/          # 插件目录
└── jdk/              # 打包的 JDK（如果使用 localDistro）
```

### 启动 Elasticsearch

```bash
cd build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/
./bin/elasticsearch
```

## 编译时间

- **首次编译**: 约 15-30 分钟（取决于机器性能和网络速度）
- **增量编译**: 约 2-5 分钟

## 常见问题

### 1. 网络问题导致 JDK 下载失败

**错误**: `Unsupported or unrecognized SSL message`

**解决**: 修改 `build-tools-internal/version.properties`，使用 adoptium 作为 JDK vendor（见上文）

### 2. 多个 Gradle 进程冲突

**错误**: `Unable to delete directory`

**解决**:
```bash
./gradlew --stop
./gradlew clean
```

### 3. 内存不足

**错误**: `OutOfMemoryError`

**解决**: 增加 Gradle 内存配置，编辑 `gradle.properties`：
```properties
org.gradle.jvmargs=-Xmx4g -XX:MaxMetaspaceSize=1g
```

## 监控编译进度

```bash
# 查看编译日志
tail -f build.log

# 查看编译进程
ps aux | grep gradle

# 查看编译任务
tail -f build.log | grep "Task"
```

## 编译成功验证

编译成功后，可以通过以下命令验证：

```bash
# 查看版本信息
build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/bin/elasticsearch --version

# 输出示例：
# Version: 8.17.11-SNAPSHOT, Build: tar/bfad1a8cd7e966c2d7f523fdf9f8a34284f6b930/2025-12-30T02:28:50.117009291Z, JVM: 23.0.2
```

### 实际编译记录

- **编译时间**: 2025-12-30
- **编译结果**: ✅ BUILD SUCCESSFUL in 47s (增量编译)
- **产物路径**: `build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/`
- **版本信息**: 8.17.11-SNAPSHOT
- **JDK版本**: 23.0.2 (Adoptium)
- **关键修改**:
  - 修改 `build-tools-internal/version.properties` 使用 Adoptium JDK
  - 添加 `gradle/verification-metadata.xml` 中 adoptium_23 的 SHA256 校验和

### 已解决的问题

1. ✅ **网络问题**: 将 JDK vendor 从 openjdk 改为 adoptium，避免 Oracle 下载源的 SSL 错误
2. ✅ **校验和验证**: 手动添加 adoptium_23 的 SHA256 校验和到验证元数据文件
3. ✅ **编译成功**: 使用本地 JDK 23 (Zulu) 成功编译，产物包含打包的 Adoptium JDK 23.0.2
```





















