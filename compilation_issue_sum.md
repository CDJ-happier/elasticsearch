# Elasticsearch 编译问题总结

## 📋 完整开发环境

### 硬件与系统
- **操作系统**: Linux 6.6.47 (x86_64)
- **内存建议**: 至少 4GB 可用内存
- **磁盘空间**: 至少 5GB 可用空间

### 软件环境
| 组件 | 版本 | 安装方式 |
|------|------|---------|
| JDK | OpenJDK 23.0.2 (Zulu) | sdkman |
| Gradle | 8.14.2 | 项目自带 wrapper |
| Elasticsearch | 8.17.11-SNAPSHOT | 源码编译 |

### 环境安装命令
```bash
# 1. 安装 sdkman
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"

# 2. 安装 JDK 23
sdk install java 23.0.2-zulu
sdk use java 23.0.2-zulu

# 3. 配置环境变量
export JAVA_HOME=$HOME/.sdkman/candidates/java/23.0.2-zulu
export PATH=$JAVA_HOME/bin:$PATH
```

---

## 🚨 编译过程中遇到的问题

### 问题 1: JDK 下载失败 - SSL 握手错误

#### ❌ 错误信息
```
> Task :distribution:archives:darwin-tar:buildDarwinTar FAILED
Execution failed for task ':distribution:archives:darwin-tar:buildDarwinTar'.
> Could not resolve all files for configuration ':distribution:archives:darwin-tar:darwinJdk'.
   > Could not download openjdk-23+37_macos-x64_bin.tar.gz
      > Could not get resource 'https://download.oracle.com/java/23/archive/openjdk-23+37_macos-x64_bin.tar.gz'
         > Unsupported or unrecognized SSL message
```

#### 🔍 问题原因
1. **网络限制**: Oracle JDK 下载源在国内网络环境下存在 SSL 握手失败问题
2. **下载源不稳定**: Oracle 官方服务器可能存在访问限制
3. **配置问题**: 项目默认使用 `openjdk` vendor，指向 Oracle 下载源

#### 💡 为什么会出现这个问题？
Elasticsearch 构建时需要下载多个平台的 JDK（Linux、macOS、Windows）用于打包发行版。默认配置文件 `build-tools-internal/version.properties` 中设置：
```properties
bundled_jdk_vendor = openjdk
bundled_jdk = 23+37@3c5b90190c68498b986a97f276efd28a
```
这导致 Gradle 尝试从 Oracle 官方源下载，在网络受限环境下会失败。

#### ✅ 解决方案
**修改 `build-tools-internal/version.properties` 文件：**
```properties
# 修改前
bundled_jdk_vendor = openjdk
bundled_jdk = 23+37@3c5b90190c68498b986a97f276efd28a

# 修改后
bundled_jdk_vendor = adoptium
bundled_jdk = 23.0.2+7
```

**为什么选择 Adoptium？**
- ✅ Eclipse Temurin (Adoptium) 是 OpenJDK 的官方认证构建版本
- ✅ 下载源更稳定，无地域限制
- ✅ 完全兼容 OpenJDK 规范
- ✅ 社区活跃，长期维护

---

### 问题 2: Gradle 依赖校验失败

#### ❌ 错误信息
```
> Dependency verification failed for configuration ':buildSrc:classpath'
   One artifact failed verification: adoptium_23-linux-23.0.2-x64.tar.gz (adoptium_23:linux:23.0.2)
   This can indicate that a dependency has been compromised.
```

#### 🔍 问题原因
1. **校验和缺失**: 切换到 Adoptium JDK 后，`gradle/verification-metadata.xml` 中缺少对应的 SHA256 校验和
2. **安全机制**: Gradle 的依赖验证机制要求所有下载的依赖必须有对应的校验和
3. **元数据不完整**: 原始配置只包含 OpenJDK 的校验和

#### 💡 为什么会出现这个问题？
Gradle 使用 `verification-metadata.xml` 文件来验证所有下载依赖的完整性和安全性。当我们修改 JDK vendor 后，必须添加新 vendor 的校验和信息，否则 Gradle 会拒绝下载。

#### ✅ 解决方案
**方法 1: 手动添加校验和（推荐）**

编辑 `gradle/verification-metadata.xml`，在 `<components>` 标签内添加：
```xml
<component group="adoptium_23" name="linux" version="23.0.2">
   <artifact name="linux-23.0.2-x64.tar.gz">
      <sha256 value="870ac8c05c6fe563e7a3878a47d0234b83c050e83651d2c47e8b822ec74512dd" origin="Manual verification"/>
   </artifact>
</component>
```

**方法 2: 自动生成校验和**
```bash
# Gradle 会自动下载并生成校验和
./gradlew --write-verification-metadata sha256 help
```

**如何获取正确的 SHA256 值？**
```bash
# 从 Adoptium 官网下载 JDK
wget https://github.com/adoptium/temurin23-binaries/releases/download/jdk-23.0.2%2B7/OpenJDK23U-jdk_x64_linux_hotspot_23.0.2_7.tar.gz

# 计算 SHA256
sha256sum OpenJDK23U-jdk_x64_linux_hotspot_23.0.2_7.tar.gz
```

---

## 🔨 完整解决流程

### 步骤 1: 修改 JDK Vendor
```bash
# 编辑 build-tools-internal/version.properties
vim build-tools-internal/version.properties

# 修改以下两行
bundled_jdk_vendor = adoptium
bundled_jdk = 23.0.2+7
```

### 步骤 2: 添加依赖校验和
```bash
# 编辑 gradle/verification-metadata.xml
vim gradle/verification-metadata.xml

# 在 <components> 标签内添加 adoptium_23 的校验和信息
```

### 步骤 3: 清理并重新编译
```bash
# 清理之前的构建
./gradlew clean

# 设置环境变量
export JAVA_HOME=$HOME/.sdkman/candidates/java/23.0.2-zulu

# 开始编译
./gradlew localDistro
```

### 步骤 4: 验证编译结果
```bash
# 查看编译产物
ls -lh build/distribution/local/

# 验证版本
build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/bin/elasticsearch --version
```

---

## 📊 编译结果

### 成功编译信息
```
✅ BUILD SUCCESSFUL in 47s (增量编译)
📦 产物路径: build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/
📏 产物大小: ~800 MB (包含 JDK)
🔧 JDK 版本: 23.0.2 (Adoptium)
📅 编译日期: 2025-12-30
```

### 目录结构
```
elasticsearch-8.17.11-SNAPSHOT/
├── bin/              # 可执行文件 (elasticsearch, elasticsearch-cli)
├── config/           # 配置文件 (elasticsearch.yml, jvm.options)
├── jdk/              # 打包的 Adoptium JDK 23.0.2
├── lib/              # 核心依赖库
├── logs/             # 日志目录
├── modules/          # ES 模块 (94 个模块)
├── plugins/          # 插件目录
├── LICENSE.txt       # 许可证
├── NOTICE.txt        # 第三方依赖声明
└── README.asciidoc   # 说明文档
```

---

## 🎯 关键修改总结

| 文件 | 修改内容 | 修改原因 |
|------|---------|---------|
| `build-tools-internal/version.properties` | `bundled_jdk_vendor = adoptium` | 解决 Oracle JDK 下载失败 |
| `build-tools-internal/version.properties` | `bundled_jdk = 23.0.2+7` | 匹配 Adoptium 版本号格式 |
| `gradle/verification-metadata.xml` | 添加 adoptium_23 的 SHA256 校验和 | 通过 Gradle 依赖校验 |

---

## 💡 经验总结

### 问题诊断思路
1. **查看完整错误日志**: 不要只看最后几行，要找到根本原因
2. **识别网络问题**: SSL 错误通常与网络访问限制有关
3. **理解构建流程**: 了解 Gradle 如何下载和验证依赖

### 解决方案选择
1. **优先使用稳定源**: Adoptium 比 Oracle 更适合国内环境
2. **保持版本一致**: JDK 版本号格式要与 vendor 匹配
3. **完整性验证**: 添加所有平台的校验和，确保构建可重复

### 最佳实践
1. ✅ 使用 sdkman 管理 JDK 版本
2. ✅ 首次编译使用 `clean` 清理
3. ✅ 增量编译提高效率
4. ✅ 记录所有修改和原因

---

## 🚀 快速启动

### 编译命令
```bash
# 完整编译（包含 JDK）
./gradlew localDistro

# 快速编译（不包含 JDK）
./gradlew installDist

# 后台编译
nohup ./gradlew localDistro > build.log 2>&1 &
```

### 启动 Elasticsearch
```bash
# 前台启动
build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/bin/elasticsearch

# 后台启动
build/distribution/local/elasticsearch-8.17.11-SNAPSHOT/bin/elasticsearch -d -p pid

# 验证启动
curl -X GET "localhost:9200/"
```

---

## 📚 参考文档

- 详细编译指南: [`COMPILATION_GUIDE.md`](./COMPILATION_GUIDE.md)
- 开发环境配置: [`dev-desc.md`](./dev-desc.md)
- Elasticsearch 官方文档: https://www.elastic.co/guide/en/elasticsearch/reference/current/
- Adoptium 官网: https://adoptium.net/

---

**文档版本**: 1.0
**创建日期**: 2025-12-30
**适用版本**: Elasticsearch 8.17.11-SNAPSHOT
