# RefCountingRunnable

## 一句话总结

**`RefCountingRunnable` 是一个基于引用计数的异步任务协调器，它允许动态地获取引用，并在所有引用释放后自动执行委托的 Runnable，无需预先声明子任务数量。**

## 问题背景（Why - 为什么需要它？）

### 现有方案的不足

在 Elasticsearch 的异步编程中，经常需要等待多个异步操作完成后再执行某个最终操作。传统方案存在以下问题：

**1. 使用 CountDown 的问题**：
```java
// ❌ 使用 CountDown 的痛点
var countDown = new CountDown(3);  // 必须预先知道数量
for (var item : collection) {
    runAsyncAction(item, ActionListener.wrap(
        result -> {
            if (countDown.countDown()) {
                finalAction.run();  // 需要手动检查并执行
            }
        },
        error -> {
            if (countDown.countDown()) {
                finalAction.run();  // 失败时也要检查
            }
        }
    ));
}
```

**问题**：
- **必须预先声明数量**：创建时必须知道有多少个子任务
- **手动检查完成**：每次 countDown 后都要检查是否归零
- **无法动态添加**：一旦创建就无法增加计数
- **代码重复**：成功和失败路径都要写检查逻辑

**2. 手动管理 ActionListener 的问题**：
```java
// ❌ 手动管理多个 listener
var completedCount = new AtomicInteger(0);
var totalCount = collection.size();
for (var item : collection) {
    runAsyncAction(item, ActionListener.wrap(
        result -> {
            if (completedCount.incrementAndGet() == totalCount) {
                finalAction.run();
            }
        },
        error -> {
            if (completedCount.incrementAndGet() == totalCount) {
                finalAction.run();
            }
        }
    ));
}
```

**问题**：
- **容易出错**：需要手动维护计数器和总数
- **竞态条件**：多线程环境下容易出现时序问题
- **代码冗余**：每个异步操作都要写相同的计数逻辑

### 典型痛点场景

**场景 1：动态数量的异步操作**
```java
// 需求：遍历集合，根据条件决定是否执行异步操作
for (var item : collection) {
    if (condition(item)) {  // 动态判断
        runAsyncAction(item, listener);  // 不知道会执行多少次
    }
}
// 问题：无法预先知道会执行多少个异步操作
```

**场景 2：嵌套的异步操作**
```java
// 需求：在异步操作中再启动新的异步操作
for (var item : collection) {
    executorService.execute(() -> {
        if (condition(item)) {
            runAsyncAction(item, listener);  // 在后台线程中动态添加
        }
    });
}
// 问题：无法在外层预知内层会启动多少个异步操作
```

**场景 3：零个异步操作的边界情况**
```java
// 需求：可能一个异步操作都不执行
for (var item : emptyCollection) {  // 集合可能为空
    runAsyncAction(item, listener);
}
// 问题：CountDown(0) 会立即完成，但可能不是期望的行为
```

### 不解决的后果

1. **代码复杂度高**：需要大量样板代码来管理计数和检查完成
2. **容易出 bug**：手动管理引用计数容易出现 off-by-one 错误
3. **可维护性差**：逻辑分散在多处，难以理解和修改
4. **无法应对动态场景**：预先声明数量的方案无法处理动态添加的情况

## 解决方案（What - 它是什么？）

### 核心定位

**`RefCountingRunnable` 是一个支持动态引用获取的异步任务协调器，它通过 try-with-resources 和引用计数机制，优雅地解决了"等待多个异步操作完成"的问题。**

### 核心特性

**1. 动态引用获取**
- 无需预先声明子任务数量
- 可以在任何时候、任何线程获取新引用
- 只要有引用存在，就可以继续获取新引用

**2. 自动完成检测**
- 当所有引用释放时，自动执行委托的 Runnable
- 无需手动检查计数是否归零
- 支持零个子任务的情况（try 块结束时立即执行）

**3. 异常安全**
- 使用 try-with-resources 确保初始引用一定会被释放
- 委托 Runnable 的异常会被捕获并记录
- 不会因为某个子任务失败而影响引用计数

**4. 简洁的 API**
- `acquire()`：获取一个引用，返回 `Releasable`
- `acquireListener()`：获取一个引用，返回 `ActionListener<Void>`
- `close()`：释放初始引用（通常由 try-with-resources 自动调用）

**5. 防御性编程**
- 断言模式下检测重复释放
- 断言模式下检测在所有引用释放后获取新引用
- 生产环境下抛出 `IllegalStateException`

### 与相关模块的对比

| 特性 | RefCountingRunnable | CountDown | RefCountingListener |
|------|---------------------|-----------|---------------------|
| 预先声明数量 | ❌ 不需要 | ✅ 必须 | ❌ 不需要 |
| 动态添加子任务 | ✅ 支持 | ❌ 不支持 | ✅ 支持 |
| 自动完成检测 | ✅ 自动 | ❌ 手动检查 | ✅ 自动 |
| 零个子任务 | ✅ 正常工作 | ⚠️ 需要特殊处理 | ✅ 正常工作 |
| 结果收集 | ❌ 不支持 | ❌ 不支持 | ✅ 支持（List） |
| 失败处理 | ❌ 不支持 | ❌ 不支持 | ✅ 支持（任一失败则整体失败） |
| 使用场景 | 纯协调（无结果） | 简单计数 | 协调 + 结果收集 |

### 设计理念

**1. 资源管理模式（RAII）**
- 使用 try-with-resources 管理生命周期
- 初始引用在 try 块结束时自动释放
- 确保即使发生异常也能正确释放

**2. 引用计数模式**
- 每个 `acquire()` 增加引用计数
- 每个 `close()` 减少引用计数
- 引用计数归零时触发委托 Runnable

**3. 装饰器模式**
- 将 `Releasable` 包装为 `ActionListener`（`acquireListener()`）
- 将引用释放逻辑与业务逻辑解耦

## 使用指南（How - 如何使用它？）

### 基本使用流程

```java
// 步骤 1：创建 RefCountingRunnable，传入最终要执行的 Runnable
try (var refs = new RefCountingRunnable(() -> {
    System.out.println("所有异步操作完成！");
})) {

    // 步骤 2：遍历集合，为每个异步操作获取引用
    for (var item : collection) {
        var ref = refs.acquire();  // 获取引用（引用计数 +1）

        // 步骤 3：执行异步操作，完成时释放引用
        runAsyncAction(item, ActionListener.wrap(
            result -> {
                processResult(result);
                ref.close();  // 释放引用（引用计数 -1）
            },
            error -> {
                handleError(error);
                ref.close();  // 失败时也要释放
            }
        ));
    }

    // 步骤 4：退出 try 块，释放初始引用
}
// 当所有引用释放后，自动执行委托的 Runnable
```

### 典型场景示例

#### 场景 1：基本用法 - 等待多个异步操作完成

```java
public void processItems(List<Item> items, ActionListener<Void> finalListener) {
    try (var refs = new RefCountingRunnable(() -> {
        // 所有 item 处理完成后执行
        finalListener.onResponse(null);
    })) {
        for (var item : items) {
            // 为每个 item 获取一个引用
            var ref = refs.acquire();

            // 异步处理 item
            asyncProcessItem(item, ActionListener.wrap(
                result -> {
                    logger.info("Item {} processed", item);
                    ref.close();  // 处理完成，释放引用
                },
                error -> {
                    logger.error("Item {} failed", item, error);
                    ref.close();  // 失败也要释放引用
                }
            ));
        }
    }
}
```

#### 场景 2：使用 acquireListener() 简化代码

```java
public void processItems(List<Item> items, ActionListener<Void> finalListener) {
    try (var refs = new RefCountingRunnable(() -> finalListener.onResponse(null))) {
        for (var item : items) {
            // acquireListener() 返回一个 ActionListener<Void>
            // 当它被调用时（无论成功或失败），会自动释放引用
            var listener = refs.acquireListener();

            asyncProcessItem(item, ActionListener.runAfter(
                ActionListener.wrap(
                    result -> logger.info("Item {} processed", item),
                    error -> logger.error("Item {} failed", item, error)
                ),
                () -> listener.onResponse(null)  // 完成后通知 listener
            ));
        }
    }
}
```

#### 场景 3：使用 ActionListener.releaseAfter() 更简洁

```java
public void processItems(List<Item> items, ActionListener<Void> finalListener) {
    try (var refs = new RefCountingRunnable(() -> finalListener.onResponse(null))) {
        for (var item : items) {
            // releaseAfter 会在 listener 完成后自动释放引用
            asyncProcessItem(
                item,
                ActionListener.releaseAfter(
                    ActionListener.wrap(
                        result -> logger.info("Item {} processed", item),
                        error -> logger.error("Item {} failed", item, error)
                    ),
                    refs.acquire()  // 获取引用
                )
            );
        }
    }
}
```

#### 场景 4：动态条件判断

```java
public void processItemsConditionally(
    List<Item> items,
    Predicate<Item> condition,
    ActionListener<Void> finalListener
) {
    try (var refs = new RefCountingRunnable(() -> finalListener.onResponse(null))) {
        for (var item : items) {
            // 根据条件动态决定是否处理
            if (condition.test(item)) {
                asyncProcessItem(
                    item,
                    ActionListener.releaseAfter(
                        ActionListener.wrap(
                            result -> logger.info("Item {} processed", item),
                            error -> logger.error("Item {} failed", item, error)
                        ),
                        refs.acquire()
                    )
                );
            }
            // 不满足条件的 item 不获取引用，不影响计数
        }
    }
    // 如果没有任何 item 满足条件，finalListener 会立即被调用
}
```

#### 场景 5：嵌套异步操作（在后台线程中动态添加）

```java
public void processItemsInBackground(
    List<Item> items,
    Executor executor,
    ActionListener<Void> finalListener
) {
    try (var refs = new RefCountingRunnable(() -> finalListener.onResponse(null))) {
        for (var item : items) {
            // 获取引用，延迟到后台线程中使用
            var itemRef = refs.acquire();

            // 在后台线程中执行
            executor.execute(() -> {
                try (var ignored = itemRef) {  // 确保引用一定会被释放
                    if (shouldProcess(item)) {
                        // 在后台线程中动态获取新引用
                        asyncProcessItem(
                            item,
                            ActionListener.releaseAfter(
                                ActionListener.wrap(
                                    result -> logger.info("Item {} processed", item),
                                    error -> logger.error("Item {} failed", item, error)
                                ),
                                refs.acquire()  // 动态获取新引用
                            )
                        );
                    }
                }
                // itemRef 在 try 块结束时自动释放
            });
        }
    }
}
```

#### 场景 6：提前返回的情况

```java
public void processWithEarlyReturn(
    List<Item> items,
    boolean flag,
    ActionListener<Void> finalListener
) {
    try (var refs = new RefCountingRunnable(() -> finalListener.onResponse(null))) {
        for (var item : items) {
            if (shouldSkip(item)) {
                continue;  // 跳过某些 item
            }

            asyncProcessItem(
                item,
                ActionListener.releaseAfter(
                    ActionListener.wrap(
                        result -> logger.info("Item {} processed", item),
                        error -> logger.error("Item {} failed", item, error)
                    ),
                    refs.acquire()
                )
            );
        }

        if (flag) {
            // 提前返回，但引用计数仍然正常工作
            runOneOffAsyncAction(
                ActionListener.releaseAfter(
                    ActionListener.wrap(
                        result -> logger.info("One-off action completed"),
                        error -> logger.error("One-off action failed", error)
                    ),
                    refs.acquire()
                )
            );
            return;  // 提前返回
        }

        // 继续处理其他逻辑...
    }
    // 无论是否提前返回，引用计数都会正确工作
}
```

#### 场景 7：零个异步操作的边界情况

```java
public void processEmptyCollection(ActionListener<Void> finalListener) {
    try (var refs = new RefCountingRunnable(() -> {
        // 即使没有任何异步操作，这里也会被执行
        finalListener.onResponse(null);
    })) {
        for (var item : Collections.<Item>emptyList()) {
            // 循环体不会执行
            asyncProcessItem(item, ActionListener.releaseAfter(..., refs.acquire()));
        }
    }
    // 退出 try 块时，立即执行委托的 Runnable
}
```

### 核心 API 说明

#### 构造方法

```java
/**
 * 创建 RefCountingRunnable
 * @param delegate 所有引用释放后要执行的 Runnable（不能为 null）
 * @throws NullPointerException 如果 delegate 为 null
 */
public RefCountingRunnable(Runnable delegate)
```

**注意**：
- `delegate` 不能抛出异常，如果抛出会被捕获并记录日志
- `delegate` 只会被执行一次

#### acquire() - 获取引用

```java
/**
 * 获取一个引用，返回 Releasable
 * @return Releasable，调用 close() 释放引用
 * @throws IllegalStateException 如果所有引用已释放（生产环境）
 * @throws AssertionError 如果所有引用已释放（断言模式）
 */
public Releasable acquire()
```

**使用示例**：
```java
var ref = refs.acquire();
try {
    // 执行异步操作
    asyncOperation(ActionListener.wrap(
        result -> {
            processResult(result);
            ref.close();  // 成功时释放
        },
        error -> {
            handleError(error);
            ref.close();  // 失败时也释放
        }
    ));
} catch (Exception e) {
    ref.close();  // 异常时释放
    throw e;
}
```

**最佳实践**：
- 使用 `ActionListener.releaseAfter()` 自动管理释放
- 或者使用 try-with-resources 确保释放

#### acquireListener() - 获取 Listener

```java
/**
 * 获取一个 ActionListener<Void>，当它被调用时自动释放引用
 * @return ActionListener<Void>
 */
public ActionListener<Void> acquireListener()
```

**使用示例**：
```java
var listener = refs.acquireListener();
asyncOperation(ActionListener.runAfter(
    myListener,
    () -> listener.onResponse(null)  // 完成后通知，自动释放引用
));
```

#### close() - 释放引用

```java
/**
 * 释放引用，如果是最后一个引用，执行委托的 Runnable
 * @throws AssertionError 如果重复释放（断言模式）
 */
@Override
public void close()
```

**注意**：
- 通常由 try-with-resources 自动调用
- 不应该手动调用多次（会触发断言错误）

### 使用注意事项

#### ⚠️ 常见陷阱

**1. 忘记释放引用**
```java
// ❌ 错误：获取引用后忘记释放
try (var refs = new RefCountingRunnable(finalAction)) {
    var ref = refs.acquire();
    asyncOperation(listener);  // 忘记在 listener 中释放 ref
}
// finalAction 永远不会执行！
```

**解决方案**：
```java
// ✅ 正确：使用 releaseAfter 自动释放
try (var refs = new RefCountingRunnable(finalAction)) {
    asyncOperation(ActionListener.releaseAfter(listener, refs.acquire()));
}
```

**2. 在所有引用释放后获取新引用**
```java
// ❌ 错误：try 块结束后尝试获取引用
var refs = new RefCountingRunnable(finalAction);
refs.close();  // 释放初始引用，finalAction 执行
refs.acquire();  // 抛出 IllegalStateException！
```

**解决方案**：
- 确保在 try 块内获取所有需要的引用
- 或者在有引用存在时才获取新引用

**3. 重复释放引用**
```java
// ❌ 错误：重复释放同一个引用
var ref = refs.acquire();
ref.close();
ref.close();  // 断言模式下抛出 AssertionError
```

**解决方案**：
- 使用 try-with-resources 确保只释放一次
- 或者使用 `ActionListener.releaseAfter()` 自动管理

**4. 委托 Runnable 抛出异常**
```java
// ⚠️ 注意：异常会被捕获并记录，但不会传播
try (var refs = new RefCountingRunnable(() -> {
    throw new RuntimeException("Oops!");  // 异常被捕获
})) {
    // ...
}
// 异常不会传播到这里，只会记录日志
```

**解决方案**：
- 委托 Runnable 应该处理所有异常
- 或者使用 `ActionListener` 传播异常

**5. 在异步操作中捕获引用**
```java
// ❌ 错误：lambda 捕获了 refs，可能导致内存泄漏
try (var refs = new RefCountingRunnable(finalAction)) {
    for (var item : items) {
        asyncOperation(item, ActionListener.wrap(
            result -> {
                processResult(result);
                refs.acquire().close();  // 捕获了 refs！
            },
            error -> handleError(error)
        ));
    }
}
```

**解决方案**：
```java
// ✅ 正确：在循环外获取引用
try (var refs = new RefCountingRunnable(finalAction)) {
    for (var item : items) {
        var ref = refs.acquire();  // 在外层获取
        asyncOperation(item, ActionListener.wrap(
            result -> {
                processResult(result);
                ref.close();  // 只捕获 ref
            },
            error -> {
                handleError(error);
                ref.close();
            }
        ));
    }
}
```

#### ✅ 最佳实践

**1. 优先使用 ActionListener.releaseAfter()**
```java
// ✅ 推荐：自动管理引用释放
asyncOperation(ActionListener.releaseAfter(listener, refs.acquire()));
```

**2. 使用 try-with-resources 管理引用**
```java
// ✅ 推荐：确保引用一定会被释放
var ref = refs.acquire();
try (var ignored = ref) {
    // 执行可能抛出异常的操作
    riskyOperation();
}
// ref 自动释放
```

**3. 委托 Runnable 应该快速完成**
```java
// ✅ 推荐：委托 Runnable 只做轻量级操作
try (var refs = new RefCountingRunnable(() -> {
    finalListener.onResponse(result);  // 快速完成
})) {
    // ...
}

// ❌ 避免：委托 Runnable 执行耗时操作
try (var refs = new RefCountingRunnable(() -> {
    heavyComputation();  // 可能阻塞线程
    finalListener.onResponse(result);
})) {
    // ...
}
```

**4. 处理空集合的情况**
```java
// ✅ 推荐：RefCountingRunnable 自动处理空集合
try (var refs = new RefCountingRunnable(finalAction)) {
    for (var item : possiblyEmptyCollection) {
        asyncOperation(ActionListener.releaseAfter(listener, refs.acquire()));
    }
}
// 如果集合为空，finalAction 立即执行
```

**5. 在多线程环境中使用**
```java
// ✅ 推荐：RefCountingRunnable 是线程安全的
try (var refs = new RefCountingRunnable(finalAction)) {
    for (var item : items) {
        executor.execute(() -> {
            asyncOperation(ActionListener.releaseAfter(listener, refs.acquire()));
        });
    }
}
```

## 实现原理（How it works - 它如何实现？）

### 核心数据结构

```java
public final class RefCountingRunnable implements Releasable {
    // 内部使用 AbstractRefCounted 实现引用计数
    private final RefCounted refCounted;

    public RefCountingRunnable(Runnable delegate) {
        // 将 Runnable 包装为 RefCounted
        this.refCounted = AbstractRefCounted.of(delegate);
    }
}
```

**关键点**：
- `RefCountingRunnable` 是 `AbstractRefCounted` 的简单包装
- 所有引用计数逻辑由 `AbstractRefCounted` 实现
- `RefCountingRunnable` 提供了更友好的 API

### AbstractRefCounted 的实现原理

```java
public abstract class AbstractRefCounted implements RefCounted {
    // 引用计数（使用 AtomicInteger 保证线程安全）
    private final AtomicInteger refCount = new AtomicInteger(1);  // 初始计数为 1

    // 委托的 Runnable
    private final Runnable onClose;

    public static RefCounted of(Runnable onClose) {
        return new AbstractRefCounted() {
            @Override
            protected void closeInternal() {
                onClose.run();  // 引用计数归零时执行
            }
        };
    }
}
```

### 引用计数机制

#### 1. 初始状态

```java
try (var refs = new RefCountingRunnable(delegate)) {
    // 创建时，引用计数 = 1（初始引用）
}
```

**状态**：
```
refCount = 1
```

#### 2. 获取引用（acquire）

```java
public Releasable acquire() {
    refCounted.mustIncRef();  // 引用计数 +1
    return Releasables.assertOnce(this);  // 返回包装的 Releasable
}
```

**mustIncRef() 的实现**：
```java
public void mustIncRef() {
    int refCount;
    do {
        refCount = this.refCount.get();
        if (refCount == 0) {
            // 引用计数已归零，不能再获取引用
            throw new IllegalStateException(ALREADY_CLOSED_MESSAGE);
        }
    } while (this.refCount.compareAndSet(refCount, refCount + 1) == false);
    // 使用 CAS 操作增加引用计数，保证线程安全
}
```

**关键点**：
- 使用 CAS 操作保证线程安全
- 如果引用计数已归零，抛出异常
- 返回的 `Releasable` 被 `assertOnce` 包装，防止重复释放

#### 3. 释放引用（close）

```java
@Override
public void close() {
    try {
        refCounted.decRef();  // 引用计数 -1
    } catch (Exception e) {
        logger.error("exception in delegate", e);
        assert false : e;
    }
}
```

**decRef() 的实现**：
```java
public void decRef() {
    int refCount = this.refCount.decrementAndGet();  // 原子递减
    if (refCount < 0) {
        // 引用计数小于 0，说明重复释放
        throw new IllegalStateException(INVALID_DECREF_MESSAGE);
    }
    if (refCount == 0) {
        // 引用计数归零，执行委托的 Runnable
        closeInternal();
    }
}
```

**关键点**：
- 使用 `decrementAndGet()` 原子递减
- 引用计数归零时，执行 `closeInternal()`（即委托的 Runnable）
- 如果引用计数小于 0，抛出异常（防止重复释放）

### 状态转换图

```
┌─────────────────────────────────────────────────────────────────┐
│                    RefCountingRunnable 状态转换                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  创建                                                            │
│   │                                                             │
│   ▼                                                             │
│  refCount = 1 (初始引用)                                         │
│   │                                                             │
│   ├─ acquire() ──► refCount = 2                                │
│   │                  │                                          │
│   │                  ├─ acquire() ──► refCount = 3             │
│   │                  │                  │                       │
│   │                  │                  ├─ close() ──► refCount = 2 │
│   │                  │                  │                       │
│   │                  ├─ close() ──► refCount = 1                │
│   │                  │                                          │
│   ├─ close() ──► refCount = 0 ──► 执行 delegate.run()          │
│   │              (try-with-resources)                           │
│   │                                                             │
│   └─ acquire() ──► IllegalStateException (已关闭)               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 并发控制机制

#### 线程安全保证

**1. AtomicInteger 保证原子性**
```java
private final AtomicInteger refCount = new AtomicInteger(1);

// 增加引用计数（CAS 操作）
while (this.refCount.compareAndSet(refCount, refCount + 1) == false);

// 减少引用计数（原子操作）
int refCount = this.refCount.decrementAndGet();
```

**2. 防止重复释放**
```java
public Releasable acquire() {
    refCounted.mustIncRef();
    return Releasables.assertOnce(this);  // 包装，防止重复释放
}
```

**Releasables.assertOnce() 的实现**：
```java
public static Releasable assertOnce(Releasable releasable) {
    return new Releasable() {
        private final AtomicBoolean released = new AtomicBoolean(false);

        @Override
        public void close() {
            boolean alreadyReleased = released.getAndSet(true);
            assert alreadyReleased == false : "already released";
            releasable.close();
        }
    };
}
```

**关键点**：
- 使用 `AtomicBoolean` 记录是否已释放
- 断言模式下检测重复释放
- 生产环境下忽略重复释放（偏离 `Closeable` 的契约）

#### 并发场景示例

```java
// 线程 1
try (var refs = new RefCountingRunnable(delegate)) {
    // refCount = 1

    // 线程 2
    executor.execute(() -> {
        var ref1 = refs.acquire();  // refCount = 2 (CAS)
        asyncOp1(ActionListener.wrap(
            r -> ref1.close(),  // refCount = 1 (原子递减)
            e -> ref1.close()
        ));
    });

    // 线程 3
    executor.execute(() -> {
        var ref2 = refs.acquire();  // refCount = 3 (CAS)
        asyncOp2(ActionListener.wrap(
            r -> ref2.close(),  // refCount = 2 (原子递减)
            e -> ref2.close()
        ));
    });

}  // refCount = 2 (释放初始引用)

// 当 asyncOp1 和 asyncOp2 都完成时，refCount = 0，执行 delegate
```

### 设计亮点

#### 1. 使用 AbstractRefCounted 复用引用计数逻辑

**优点**：
- 避免重复实现引用计数
- `AbstractRefCounted` 经过充分测试，可靠性高
- 统一的引用计数语义

**实现**：
```java
public RefCountingRunnable(Runnable delegate) {
    this.refCounted = AbstractRefCounted.of(delegate);
}
```

#### 2. 返回 this 而不是新对象

```java
public Releasable acquire() {
    refCounted.mustIncRef();
    return Releasables.assertOnce(this);  // 返回包装的 this
}
```

**优点**：
- 节省对象分配
- 所有引用共享同一个 `RefCountingRunnable` 实例
- 简化内存管理

**注意**：
- 偏离了 `Closeable` 的契约（应该是幂等的）
- 但在断言模式下会检测重复释放

#### 3. 异常安全的 close()

```java
@Override
public void close() {
    try {
        refCounted.decRef();
    } catch (Exception e) {
        logger.error("exception in delegate", e);
        assert false : e;  // 断言模式下失败
    }
}
```

**优点**：
- 捕获委托 Runnable 的异常，避免影响调用者
- 记录日志，便于调试
- 断言模式下快速失败

#### 4. 支持零个子任务的情况

```java
try (var refs = new RefCountingRunnable(delegate)) {
    // 如果循环体不执行，不会获取任何引用
    for (var item : emptyCollection) {
        asyncOperation(ActionListener.releaseAfter(listener, refs.acquire()));
    }
}
// 退出 try 块时，refCount = 0，立即执行 delegate
```

**优点**：
- 无需特殊处理空集合
- 语义清晰：try 块结束时，如果没有其他引用，立即完成

#### 5. 防御性编程

```java
public void mustIncRef() {
    int refCount;
    do {
        refCount = this.refCount.get();
        if (refCount == 0) {
            throw new IllegalStateException(ALREADY_CLOSED_MESSAGE);
        }
    } while (this.refCount.compareAndSet(refCount, refCount + 1) == false);
}
```

**优点**：
- 检测在关闭后获取引用的错误
- 断言模式下抛出 `AssertionError`
- 生产环境下抛出 `IllegalStateException`

### 性能考虑

**1. 无锁设计**
- 使用 `AtomicInteger` 的 CAS 操作
- 避免锁竞争，适合高并发场景

**2. 对象分配优化**
- 返回 `this` 而不是新对象
- 减少 GC 压力

**3. 快速路径**
- `acquire()` 和 `close()` 都是 O(1) 操作
- 引用计数归零时才执行委托 Runnable

### 值得学习的技巧

**1. 资源管理模式（RAII）**
- 使用 try-with-resources 管理生命周期
- 确保资源一定会被释放

**2. 引用计数模式**
- 动态管理异步操作的生命周期
- 无需预先声明数量

**3. 装饰器模式**
- `Releasables.assertOnce()` 包装 `Releasable`
- `acquireListener()` 将 `Releasable` 转换为 `ActionListener`

**4. 防御性编程**
- 检测重复释放
- 检测在关闭后获取引用
- 捕获委托 Runnable 的异常

**5. 线程安全**
- 使用 `AtomicInteger` 保证原子性
- CAS 操作避免锁竞争

## 总结

### 适用场景

`RefCountingRunnable` 适用于以下场景：

1. **动态数量的异步操作**：无法预先知道会执行多少个异步操作
2. **嵌套异步操作**：在异步操作中动态启动新的异步操作
3. **条件异步操作**：根据条件决定是否执行异步操作
4. **零个异步操作**：可能一个异步操作都不执行
5. **提前返回**：需要在不同分支中启动异步操作

### 不适用场景

1. **需要收集结果**：使用 `RefCountingListener` 代替
2. **需要失败处理**：使用 `RefCountingListener` 代替
3. **预先知道数量**：使用 `CountDown` 可能更简单
4. **需要取消**：`RefCountingRunnable` 不支持取消

### 关键要点

1. **动态引用获取**：无需预先声明数量，可以在任何时候获取引用
2. **自动完成检测**：引用计数归零时自动执行委托 Runnable
3. **异常安全**：使用 try-with-resources 确保引用一定会被释放
4. **线程安全**：使用 `AtomicInteger` 保证并发安全
5. **防御性编程**：检测重复释放和在关闭后获取引用

### 与其他工具的对比

| 工具 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| RefCountingRunnable | 动态数量的异步操作 | 无需预先声明数量，自动完成检测 | 不支持结果收集和失败处理 |
| RefCountingListener | 动态数量 + 结果收集 | 支持结果收集和失败处理 | 稍微复杂一些 |
| CountDown | 固定数量的异步操作 | 简单直接 | 需要预先声明数量，手动检查完成 |
| SubscribableListener | 多订阅者场景 | 支持多订阅者，延迟订阅 | 不适合简单的计数场景 |

### 最佳实践总结

1. **优先使用 ActionListener.releaseAfter()**：自动管理引用释放
2. **使用 try-with-resources**：确保初始引用一定会被释放
3. **委托 Runnable 应该快速完成**：避免阻塞线程
4. **处理空集合**：RefCountingRunnable 自动处理，无需特殊逻辑
5. **在多线程环境中使用**：RefCountingRunnable 是线程安全的
