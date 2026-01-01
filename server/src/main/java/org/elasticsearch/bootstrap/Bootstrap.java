/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.NodeValidationException;

import java.io.PrintStream;

/**
 * A container for transient state during bootstrap of the Elasticsearch process.
 *
 * <h1>类职责与设计分析</h1>
 *
 * <h2>主要职责</h2>
 * <p>
 * Bootstrap 类是 Elasticsearch 启动过程中的核心状态容器，负责管理和协调节点启动的整个生命周期。
 * 它封装了启动过程中所有的临时状态信息，包括命令行参数、安全设置、环境配置以及子进程管理等关键组件。
 * </p>
 *
 * <h2>工作机制</h2>
 * <p>
 * 该类采用<b>状态容器模式</b>，通过以下机制协调启动流程：
 * </p>
 * <ul>
 *   <li><b>阶段性初始化</b>：使用 SetOnce 确保关键配置（secureSettings、nodeEnv）只能设置一次，
 *       体现了启动过程的阶段性特征（Phase 1: 基础初始化 → Phase 2: 加载配置）</li>
 *   <li><b>资源管理</b>：持有原始的标准输出/错误流，用于在日志系统初始化前后的输出控制</li>
 *   <li><b>进程协调</b>：通过 Spawner 管理组件子进程的生命周期</li>
 *   <li><b>异常处理</b>：提供统一的异常退出机制，确保错误信息正确输出并优雅关闭</li>
 * </ul>
 *
 * <h2>在架构中的位置</h2>
 * <p>
 * <b>所属模块</b>：bootstrap 模块（org.elasticsearch.bootstrap）<br>
 * <b>创建者</b>：由 Elasticsearch 主类在 JVM 启动时创建<br>
 * <b>使用者</b>：主要被启动流程的各个阶段使用，包括：
 * </p>
 * <ul>
 *   <li>Elasticsearch 主类：协调整体启动流程</li>
 *   <li>Security 模块：访问 secureSettings 进行安全初始化</li>
 *   <li>Node 类：获取 environment 和 secureSettings 进行节点构建</li>
 *   <li>各启动阶段：通过 spawner 启动必要的子进程</li>
 * </ul>
 *
 * <h2>关键设计点</h2>
 * <ul>
 *   <li><b>不可变性保证</b>：使用 SetOnce 包装关键状态，确保配置一旦加载就不可更改，
 *       防止启动过程中的状态污染</li>
 *   <li><b>职责分离</b>：将子进程管理（Spawner）、参数解析（ServerArgs）等职责委托给专门的类，
 *       Bootstrap 仅作为协调者</li>
 *   <li><b>优雅退出</b>：提供 gracefullyExit 机制，确保在任何异常情况下都能：
 *       <ol>
 *         <li>输出有用的错误信息和日志位置提示</li>
 *         <li>刷新错误流</li>
 *         <li>使用正确的退出码（遵循 ExitCodes 规范）</li>
 *       </ol>
 *   </li>
 *   <li><b>CLI 通信</b>：通过 sendCliMarker 方法向 CLI 进程发送特定标记，
 *       实现启动过程中的进程间通信</li>
 *   <li><b>资源清理</b>：提供 closeStreams 方法确保流资源的正确释放</li>
 * </ul>
 *
 * <h2>生命周期</h2>
 * <p>
 * Bootstrap 实例的生命周期贯穿整个启动过程，从 JVM 启动到 Node 完全初始化。
 * 一旦节点成功启动，该实例的使命即完成，其持有的临时状态将被释放或转移到 Node 实例中。
 * </p>
 */
class Bootstrap {
    // original stdout stream
    private final PrintStream out;

    // original stderr stream
    private final PrintStream err;

    // arguments from the CLI process
    private final ServerArgs args;

    // controller for spawning component subprocesses
    private final Spawner spawner = new Spawner();

    // the loaded keystore, not valid until after phase 2 of initialization
    private final SetOnce<SecureSettings> secureSettings = new SetOnce<>();

    // the loaded settings for the node, not valid until after phase 2 of initialization
    private final SetOnce<Environment> nodeEnv = new SetOnce<>();

    Bootstrap(PrintStream out, PrintStream err, ServerArgs args) {
        this.out = out;
        this.err = err;
        this.args = args;
    }

    ServerArgs args() {
        return args;
    }

    Spawner spawner() {
        return spawner;
    }

    void setSecureSettings(SecureSettings secureSettings) {
        this.secureSettings.set(secureSettings);
    }

    SecureSettings secureSettings() {
        return secureSettings.get();
    }

    void setEnvironment(Environment environment) {
        this.nodeEnv.set(environment);
    }

    Environment environment() {
        return nodeEnv.get();
    }

    void exitWithNodeValidationException(NodeValidationException e) {
        Logger logger = LogManager.getLogger(Elasticsearch.class);
        logger.error("node validation exception\n{}", e.getMessage());
        gracefullyExit(ExitCodes.CONFIG);
    }

    void exitWithUnknownException(Throwable e) {
        Logger logger = LogManager.getLogger(Elasticsearch.class);
        logger.error("fatal exception while booting Elasticsearch", e);
        gracefullyExit(1); // mimic JDK exit code on exception
    }

    private void gracefullyExit(int exitCode) {
        printLogsSuggestion();
        err.flush();
        exit(exitCode);
    }

    @SuppressForbidden(reason = "main exit path")
    static void exit(int exitCode) {
        System.exit(exitCode);
    }

    /**
     * Prints a message directing the user to look at the logs. A message is only printed if
     * logging has been configured.
     */
    private void printLogsSuggestion() {
        final String basePath = System.getProperty("es.logs.base_path");
        assert basePath != null : "logging wasn't initialized";
        err.println(
            "ERROR: Elasticsearch did not exit normally - check the logs at "
                + basePath
                + System.getProperty("file.separator")
                + System.getProperty("es.logs.cluster_name")
                + ".log"
        );
    }

    void sendCliMarker(char marker) {
        err.println(marker);
        err.flush();
    }

    void closeStreams() {
        out.close();
        err.close();
    }
}
