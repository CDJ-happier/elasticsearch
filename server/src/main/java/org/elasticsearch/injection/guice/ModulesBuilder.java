/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.injection.guice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * <h2>模块构建器类 - ModulesBuilder</h2>
 *
 * <p><strong>主要职责</strong>：作为Elasticsearch依赖注入框架的模块管理核心，负责收集、组织和配置Guice模块，
 * 并提供便捷的API来构建和初始化依赖注入容器。</p>
 *
 * <p><strong>工作机制</strong>：这是一个构建器模式的实现，通过链式调用方式逐步添加模块配置，最终创建完整的注入器实例。
 * 它封装了Guice模块的复杂配置过程，提供了更简洁的API接口。</p>
 *
 * <p><strong>在架构中的位置</strong>：位于Elasticsearch依赖注入框架的Guice兼容层（injection.guice包），
 * 是连接Elasticsearch自定义注入框架与标准Guice框架的桥梁。通常被NodeConstruction等核心启动类使用，
 * 用于构建Elasticsearch节点的依赖注入环境。</p>
 *
 * <p><strong>关键设计点</strong>：</p>
 * <ul>
 *   <li><strong>构建器模式</strong>：支持链式调用，提供流畅的API体验</li>
 *   <li><strong>模块集合管理</strong>：内部维护模块列表，支持批量添加和迭代访问</li>
 *   <li><strong>实例绑定简化</strong>：通过bindToInstance方法简化常见绑定场景</li>
 *   <li><strong>Elasticsearch优化</strong>：在创建注入器时应用特定的内存优化策略（急切单例模式）</li>
 * </ul>
 */
public class ModulesBuilder implements Iterable<Module> {

    private final List<Module> modules = new ArrayList<>();

    public ModulesBuilder add(Module... newModules) {
        Collections.addAll(modules, newModules);
        return this;
    }

    public <T> T bindToInstance(Class<T> cls, T instance) {
        modules.add(b -> b.bind(cls).toInstance(instance));
        return instance;
    }

    @Override
    public Iterator<Module> iterator() {
        return modules.iterator();
    }

    public Injector createInjector() {
        Injector injector = Guice.createInjector(modules);
        ((InjectorImpl) injector).clearCache();
        // in ES, we always create all instances as if they are eager singletons
        // this allows for considerable memory savings (no need to store construction info) as well as cycles
        ((InjectorImpl) injector).readOnlyAllSingletons();
        return injector;
    }
}
