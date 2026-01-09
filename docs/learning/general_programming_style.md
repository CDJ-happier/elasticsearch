# 通用编程模式/思想

## ActionListener
Elasticsearch 中的 runBefore、runAfter、delegateFailure 等方法是基于
装饰器模式（Decorator Pattern）的函数式编程实践。它们的本质是：包装一个 ActionListener，
返回一个新的 ActionListener，在原有行为基础上增加额外的逻辑。
原始 Listener → 包装 → 新 Listener（增强功能）

runBefore(listener, runnable) - 前置执行
作用：在 listener 的 onResponse 或 onFailure 被调用之前，先执行 runnable。
实现原理（来自 RunBeforeActionListener）：
```java
@Override
public void onResponse(T response) {
    try {
        runBefore.run();  // ← 先执行前置逻辑
    } catch (Exception ex) {
        super.onFailure(ex);  // ← 如果前置逻辑失败，直接调用 onFailure
        return;
    }
    delegate.onResponse(response);  // ← 再调用原始 listener
}

@Override
public void onFailure(Exception e) {
    try {
        runBefore.run();  // ← 失败时也执行前置逻辑
    } catch (Exception ex) {
        e.addSuppressed(ex);  // ← 将前置逻辑的异常附加到原异常
    }
    super.onFailure(e);
}
```
典型使用场景：无论原本成功与否，调用onResponse或onFailure之前都先执行某个任务。

runAfter同理。

delegateFailure()：创建一个新 listener，它的 onResponse 由你自定义，
但 onFailure 会委托给原始 listener。
关键点：
成功时：执行你的自定义逻辑 (listener, response) -> { ... }
失败时：直接转发给原始 listener，不做任何处理
异常处理：如果你的自定义逻辑抛出异常，不会被捕获（需要你自己处理）

delegateFailureAndWrap(): 与 delegateFailure 类似，但会捕获你的自定义逻辑中的异常，
并转发给 onFailure。

## SubscribableListener

### 一句话总结

**`SubscribableListener` 是一个可以被多个 `ActionListener` 订阅的 `ActionListener`，用于让多个组件共享同一个异步操作的结果。**

### 什么是 SubscribableListener？

`SubscribableListener<T>` 本质上是一个**可以被多个 ActionListener 订阅的 ActionListener**。

#### 核心概念解释

**订阅什么？**
- 订阅的是一个**异步操作的结果**（成功或失败）

**谁是订阅者？**
- 订阅者是普通的 `ActionListener`，它们想要获得某个异步操作的结果

**如何订阅？**
- 通过调用 `subscribableListener.addListener(myListener)` 来订阅

**订阅后会发生什么？**
- 当 `SubscribableListener` 被完成时（调用 `onResponse` 或 `onFailure`），它会自动通知所有订阅者
- 如果订阅时 `SubscribableListener` 已经完成，订阅者会立即收到结果

#### 一个简单的例子理解订阅

```java
// 1. 创建一个 SubscribableListener（代表一个异步操作）
var asyncOperation = new SubscribableListener<String>();

// 2. 多个组件订阅这个异步操作的结果
asyncOperation.addListener(ActionListener.wrap(
    result -> System.out.println("订阅者A收到: " + result),
    e -> System.err.println("订阅者A收到错误: " + e)
));

asyncOperation.addListener(ActionListener.wrap(
    result -> System.out.println("订阅者B收到: " + result),
    e -> System.err.println("订阅者B收到错误: " + e)
));

// 3. 执行异步操作，将 SubscribableListener 作为回调传入
someAsyncMethod(asyncOperation);

// 4. 当异步操作完成时，调用 asyncOperation.onResponse("结果")
//    此时所有订阅者（A和B）都会收到通知
```

### 核心特性

1. **多订阅者支持**：多个 `ActionListener` 可以订阅同一个异步操作的结果
2. **延迟订阅**：可以在操作完成前或完成后订阅，订阅者都能收到结果
3. **结果缓存**：一旦完成（成功或失败），结果会被缓存，后续订阅者立即获得该结果
4. **链式调用**：支持类似 `CompletionStage` 的链式异步操作，但不捕获 `Throwable`
5. **线程安全**：使用 CAS 操作保证多线程环境下的安全性

### 内部实现原理

#### 状态管理
`SubscribableListener` 使用一个 `volatile Object state` 字段来管理状态：

- **未完成状态**：
  - `EMPTY`：没有订阅者
  - 单个 `ActionListener`：只有一个订阅者
  - `Cell` 链表头：多个订阅者（逆序链表）

- **完成状态**：
  - `SuccessResult<T>`：成功完成，包含结果
  - `FailureResult`：失败完成，包含异常

#### 订阅机制
```java
public final void addListener(ActionListener<T> listener, Executor executor, @Nullable ThreadContext threadContext)
```

- 如果已完成：立即在当前线程完成订阅者
- 如果未完成：将订阅者加入链表，等待完成时通知
- 使用 `VarHandle` 进行 CAS 操作，保证线程安全

#### 完成机制
```java
private void setResult(Object result)
```

- 使用 CAS 操作设置结果，保证只完成一次
- 将订阅者链表反转（恢复订阅顺序）
- 按订阅顺序依次完成所有订阅者

### 典型应用场景

#### 场景 1：避免重复执行昂贵的异步操作

**问题**：多个组件同时需要同一个资源，但加载资源的操作很昂贵（如网络请求、数据库查询）

**解决方案**：使用 `SubscribableListener` 让多个组件订阅同一个异步操作

```java
public class ResourceManager {
    // SubscribableListener 代表"加载资源"这个异步操作
    private final SubscribableListener<Resource> resourceLoader = new SubscribableListener<>();
    private final AtomicBoolean loading = new AtomicBoolean(false);

    public void getResource(ActionListener<Resource> listener) {
        // 多个调用者订阅同一个 resourceLoader
        resourceLoader.addListener(listener);

        // 只有第一个调用者会真正触发加载
        if (loading.compareAndSet(false, true)) {
            // 执行昂贵的异步加载操作，完成时会通知所有订阅者
            asyncLoadResource(resourceLoader);
        }
    }
}

// 使用示例：
// 组件A、B、C 同时调用 getResource，但只会执行一次 asyncLoadResource
resourceManager.getResource(listenerA);  // 订阅者A
resourceManager.getResource(listenerB);  // 订阅者B
resourceManager.getResource(listenerC);  // 订阅者C
// 当 asyncLoadResource 完成时，A、B、C 都会收到结果
```

#### 场景 2：构建清晰的异步操作链

**问题**：需要按顺序执行多个异步操作，每一步依赖前一步的结果

**解决方案**：使用 `andThen` 系列方法构建链式调用

```java
public void processOrder(String orderId, ActionListener<Receipt> finalListener) {
    SubscribableListener
        // 步骤1：验证订单（异步）
        .<Order>newForked(l -> validateOrder(orderId, l))

        // 步骤2：扣减库存（异步，使用步骤1的结果）
        .<Inventory>andThen((l, order) -> deductInventory(order, l))

        // 步骤3：创建支付（异步，使用步骤2的结果）
        .<Payment>andThen((l, inventory) -> createPayment(inventory, l))

        // 步骤4：生成收据（同步转换）
        .andThenApply(payment -> generateReceipt(payment))

        // 最终结果传递给调用者
        .addListener(finalListener);
}
```

#### 场景 3：扇出操作 - 一个结果通知多个处理器

**问题**：一个异步操作完成后，需要触发多个独立的后续操作（如缓存更新、日志记录、通知发送）

**解决方案**：多个 listener 订阅同一个 `SubscribableListener`

```java
public void processData(String dataId, ActionListener<Void> finalListener) {
    var dataListener = new SubscribableListener<Data>();

    // 订阅者1：更新缓存
    dataListener.addListener(ActionListener.wrap(
        data -> cacheService.update(data),
        e -> logger.error("Cache update failed", e)
    ));

    // 订阅者2：发送通知
    dataListener.addListener(ActionListener.wrap(
        data -> notificationService.send(data),
        e -> logger.error("Notification failed", e)
    ));

    // 订阅者3：记录审计日志
    dataListener.addListener(ActionListener.wrap(
        data -> auditService.log(data),
        e -> logger.error("Audit log failed", e)
    ));

    // 订阅者4：最终回调
    dataListener.addListener(finalListener.map(data -> null));

    // 执行异步数据加载，完成时所有订阅者都会被通知
    asyncLoadData(dataId, dataListener);
}
```

#### 场景 4：超时控制

**问题**：异步操作可能长时间不返回，需要设置超时

**解决方案**：使用 `addTimeout` 方法

```java
public void queryWithTimeout(String query, ActionListener<Result> listener) {
    var queryListener = new SubscribableListener<Result>();

    // 添加30秒超时，超时后会调用 onFailure(ElasticsearchTimeoutException)
    queryListener.addTimeout(
        TimeValue.timeValueSeconds(30),
        threadPool,
        threadPool.executor(ThreadPool.Names.GENERIC)
    );

    // 订阅最终结果
    queryListener.addListener(listener);

    // 执行可能很慢的查询
    slowAsyncQuery(query, queryListener);
}
```

### 使用方式详解

#### 步骤 1：创建 SubscribableListener

`SubscribableListener` 代表一个**异步操作**，有多种创建方式：

```java
// 方式1：创建未完成的 listener（最常用）
var listener = new SubscribableListener<String>();
// 此时 listener 处于"等待完成"状态，可以添加订阅者

// 方式2：创建已成功完成的 listener
var succeeded = SubscribableListener.newSucceeded("result");
// 任何订阅者都会立即收到 "result"

// 方式3：创建已失败的 listener
var failed = SubscribableListener.newFailed(new ElasticsearchException("error"));
// 任何订阅者都会立即收到异常

// 方式4：创建并立即启动异步操作（推荐用于链式调用）
var forked = SubscribableListener.newForked(l -> asyncMethod(args, l));
// 创建 listener 的同时，将其作为回调传给 asyncMethod
```

#### 步骤 2：将 SubscribableListener 传给异步方法

```java
// 创建 SubscribableListener
var listener = new SubscribableListener<String>();

// 将它作为 ActionListener 传给异步方法
// 当异步操作完成时，会调用 listener.onResponse() 或 listener.onFailure()
someAsyncMethod(args, listener);

// 此时 listener 代表"someAsyncMethod 的执行结果"
```

#### 步骤 3：添加订阅者（等待结果）

订阅者是普通的 `ActionListener`，通过 `addListener` 方法订阅：

```java
// 订阅方式1：简单订阅（在当前线程执行）
listener.addListener(ActionListener.wrap(
    result -> {
        // 成功时的处理逻辑
        System.out.println("收到结果: " + result);
    },
    exception -> {
        // 失败时的处理逻辑
        System.err.println("收到错误: " + exception);
    }
));

// 订阅方式2：指定执行器（在特定线程池执行）
listener.addListener(
    myListener,
    threadPool.executor(ThreadPool.Names.GENERIC),  // 指定执行器
    threadContext  // 传播线程上下文（可选）
);

// 可以添加多个订阅者，它们都会收到相同的结果
listener.addListener(listenerA);
listener.addListener(listenerB);
listener.addListener(listenerC);
```

#### 步骤 4：完成 SubscribableListener（通知所有订阅者）

```java
// 异步操作成功完成时
listener.onResponse(result);
// 此时所有订阅者的 onResponse 方法会被调用

// 或者异步操作失败时
listener.onFailure(exception);
// 此时所有订阅者的 onFailure 方法会被调用
```

#### 高级用法：链式操作

链式操作用于构建多步异步流程，每一步都返回一个新的 `SubscribableListener`：

```java
// andThen: 执行下一个异步操作（忽略前一步结果）
listener.andThen(l -> nextAsyncStep(l))
    .addListener(finalListener);

// andThen: 执行下一个异步操作（使用前一步结果）
listener.andThen((l, result) -> {
    // result 是前一步的结果
    // l 是新的 SubscribableListener，需要在异步操作完成时调用它
    nextAsyncStep(result, l);
})
.addListener(finalListener);

// andThenApply: 同步转换结果
listener.andThenApply(result -> {
    // 同步转换 result
    return transformedResult;
})
.addListener(finalListener);

// andThenAccept: 同步消费结果（无返回值）
listener.andThenAccept(result -> {
    // 同步处理 result，不返回值
    processResult(result);
})
.addListener(finalListener);
```

### 完整使用实例

#### 实例 1：多步异步操作链
```java
public void processRequest(String requestId, List<String> items, ActionListener<Boolean> finalListener) {
    SubscribableListener
        // 第一步：验证请求
        .<ValidationResult>newForked(l -> validateRequest(requestId, l))

        // 第二步：加载数据（使用验证结果）
        .<DataSet>andThen((l, validationResult) -> {
            if (!validationResult.isValid()) {
                throw new IllegalArgumentException("Invalid request");
            }
            loadData(validationResult.getDataId(), l);
        })

        // 第三步：并行处理多个项目
        .<List<ProcessedItem>>andThen((l, dataSet) -> {
            var results = new ArrayList<ProcessedItem>();
            try (var listeners = new RefCountingListener(l.map(v -> results))) {
                for (String item : items) {
                    processItem(dataSet, item, listeners.acquire().map(results::add));
                }
            }
        })

        // 第四步：同步聚合结果
        .andThenApply(processedItems -> {
            return processedItems.stream()
                .allMatch(ProcessedItem::isSuccessful);
        })

        // 第五步：同步记录日志
        .andThenAccept(success -> {
            if (success) {
                logger.info("Request {} processed successfully", requestId);
            } else {
                logger.warn("Request {} had failures", requestId);
            }
        })

        // 完成链式调用
        .addListener(finalListener);
}
```

#### 实例 2：带超时的资源加载
```java
public void loadResourceWithTimeout(String resourceId, ActionListener<Resource> listener) {
    var resourceListener = new SubscribableListener<Resource>();

    // 添加 30 秒超时
    resourceListener.addTimeout(
        TimeValue.timeValueSeconds(30),
        threadPool,
        threadPool.executor(ThreadPool.Names.GENERIC)
    );

    // 添加清理逻辑（超时时取消加载）
    var cancelled = new AtomicBoolean(false);
    resourceListener.addListener(ActionListener.running(() -> {
        if (resourceListener.isDone()) {
            cancelled.set(true);
        }
    }));

    // 执行异步加载
    asyncLoadResource(resourceId, new ActionListener<>() {
        @Override
        public void onResponse(Resource resource) {
            if (!cancelled.get()) {
                resourceListener.onResponse(resource);
            }
        }

        @Override
        public void onFailure(Exception e) {
            resourceListener.onFailure(e);
        }
    });

    // 订阅最终结果
    resourceListener.addListener(listener);
}
```

#### 实例 3：线程上下文传播
```java
public void processWithContext(String data, ActionListener<Result> listener) {
    var threadContext = threadPool.getThreadContext();
    var headerName = "request-id";

    var processingListener = new SubscribableListener<Result>();

    // 在特定线程上下文中添加订阅者
    try (var ignored = threadContext.stashContext()) {
        threadContext.putHeader(headerName, UUID.randomUUID().toString());

        processingListener.addListener(
            ActionListener.wrap(
                result -> {
                    // 这里能访问到设置的 header
                    String requestId = threadContext.getHeader(headerName);
                    logger.info("Request {} completed", requestId);
                    listener.onResponse(result);
                },
                listener::onFailure
            ),
            threadPool.executor(ThreadPool.Names.GENERIC),
            threadContext  // 传播线程上下文
        );
    }

    // 执行异步处理
    asyncProcess(data, processingListener);
}
```

### 线程模型和执行器

#### 订阅者的执行线程
订阅者的执行线程取决于两个因素：

1. **订阅时机**：
   - 如果 `SubscribableListener` 已完成：订阅者在**调用 `addListener` 的线程**上立即执行
   - 如果 `SubscribableListener` 未完成：订阅者在**完成 `SubscribableListener` 的线程**上执行

2. **指定的执行器**：
   - 如果传入 `EsExecutors.DIRECT_EXECUTOR_SERVICE`：直接在上述线程执行
   - 如果传入其他执行器：尝试在该执行器上执行（如果被拒绝，则在原线程上以异常完成）

#### 确保特定执行器执行的方法
要确保订阅者在特定执行器上执行，必须同时满足：
1. 调用 `addListener` 时传入该执行器
2. 确保 `SubscribableListener` 总是在该执行器上完成

### 与 CompletionStage 的对比

| 特性 | SubscribableListener | CompletionStage |
|------|---------------------|-----------------|
| 异常处理 | 只处理 `Exception` | 捕获所有 `Throwable` |
| 多订阅者 | 原生支持 | 需要额外处理 |
| 线程控制 | 精确控制（Executor + ThreadContext） | 仅 Executor |
| 订阅顺序 | 保证按订阅顺序完成 | 无保证 |
| 使用场景 | Elasticsearch 内部异步操作 | 通用 Java 异步编程 |

### 最佳实践

1. **异常安全**：`andThen`、`andThenApply`、`andThenAccept` 都是异常安全的，抛出的异常会自动传递给 listener
2. **避免阻塞**：订阅者不应执行阻塞操作，否则会阻塞完成线程
3. **线程上下文**：需要传播线程上下文时，显式传入 `ThreadContext` 参数
4. **超时处理**：长时间运行的操作应添加超时，并在超时时清理资源
5. **订阅顺序**：依赖订阅顺序时，确保在完成前添加所有订阅者

## CancellableFanOut

### 一句话总结

**`CancellableFanOut` 是一个支持取消的扇出操作框架，它使用 `SubscribableListener` 实现了对多个子任务的并发执行和结果聚合，并在任务取消时能够及时释放内存。**

### 什么是 CancellableFanOut？

`CancellableFanOut` 是一个抽象基类，用于处理"扇出"（Fan-out）模式的异步操作：
- **扇出**：将一个请求分发到多个子任务（如向多个节点发送请求）
- **聚合**：收集所有子任务的响应，最终生成一个聚合结果
- **可取消**：当任务被取消时，能够及时释放对结果的引用，避免内存泄漏

#### 核心问题

在分布式系统中，一个请求可能需要向多个节点发送子请求并聚合结果。这会带来以下问题：

1. **内存占用**：聚合的结果可能很大（如统计信息），如果客户端超时放弃，这些结果会占用大量内存
2. **慢节点问题**：某个节点响应很慢，导致整个请求长时间无法完成
3. **取消处理**：客户端取消请求后，服务端应该停止处理并释放资源

#### 解决方案

`CancellableFanOut` 通过以下机制解决这些问题：

1. **及时释放引用**：任务取消时，立即释放对 `this` 的引用，让 GC 回收聚合的结果
2. **使用 SubscribableListener**：利用其多订阅者特性管理子任务的生命周期
3. **引用计数**：使用 `RefCountingRunnable` 确保所有子任务完成后才完成最终 listener

### 核心设计

#### 类型参数

```java
public abstract class CancellableFanOut<Item, ItemResponse, FinalResponse>
```

- `Item`：每个子任务的输入（如节点信息）
- `ItemResponse`：每个子任务的响应
- `FinalResponse`：最终聚合的响应

#### 抽象方法（需要子类实现）

```java
// 1. 发送子任务请求
protected abstract void sendItemRequest(Item item, ActionListener<ItemResponse> listener);

// 2. 处理子任务成功响应
protected abstract void onItemResponse(Item item, ItemResponse itemResponse);

// 3. 处理子任务失败
protected abstract void onItemFailure(Item item, Exception e);

// 4. 所有子任务完成后，生成最终结果
protected abstract FinalResponse onCompletion() throws Exception;
```

### 内部机制详解

#### 1. 三个关键的 Listener

```java
// Listener 1: resultListener - 捕获最终结果
final var resultListener = new SubscribableListener<FinalResponse>();

// Listener 2: resultListenerCompleter - 完成 resultListener 的 Runnable
final var resultListenerCompleter = new AtomicReference<Runnable>(() -> {
    if (cancellableTask != null && cancellableTask.notifyIfCancelled(resultListener)) {
        return;  // 如果已取消，用取消异常完成
    }
    // 否则调用 onCompletion() 生成最终结果
    ActionListener.completeWith(resultListener, this::onCompletion);
});

// Listener 3: itemCancellationListener - 收集所有子任务 listener，用于取消通知
final var itemCancellationListener = new SubscribableListener<ItemResponse>();
```

**为什么需要三个 Listener？**

- `resultListener`：存储最终结果，但不立即完成外部 listener（等待所有子任务完成）
- `resultListenerCompleter`：控制何时完成 `resultListener`，可以被替换为空操作（取消时）
- `itemCancellationListener`：管理所有子任务的 listener，取消时统一通知

#### 2. 取消处理机制

```java
if (cancellableTask != null) {
    cancellableTask.addListener(() -> {
        // 步骤1：使用信号量阻止新的订阅者
        final var semaphore = new Semaphore(0);

        // 步骤2：替换 resultListenerCompleter 为空操作，并执行原来的 completer
        // 这会用取消异常完成 resultListener（此时还没有订阅者）
        resultListenerCompleter.getAndSet(semaphore::acquireUninterruptibly).run();

        // 步骤3：释放信号量
        semaphore.release();

        // 步骤4：通知所有子任务 listener 取消（用取消异常完成它们）
        cancellableTask.notifyIfCancelled(itemCancellationListener);
    });
}
```

**关键点**：
- 使用信号量确保 `resultListener` 在没有订阅者时完成（避免在 transport 线程上执行慢回调）
- 替换 `resultListenerCompleter` 为空操作，释放对 `this` 的引用
- 通知所有子任务 listener，让它们快速失败

##### 信号量机制的详细解释

这行代码使用Java的方法引用和原子引用操作，分解如下：

```java
final var resultListenerCompleter = new AtomicReference<Runnable>(() -> {
    if (cancellableTask != null && cancellableTask.notifyIfCancelled(resultListener)) {
        return; // 如果任务已取消，通知resultListener并返回
    }
    ActionListener.completeWith(resultListener, this::onCompletion); // 否则正常完成
});
```

在任务取消监听器中：
```java
resultListenerCompleter.getAndSet(semaphore::acquireUninterruptibly).run();
```

**执行过程：**

1. `getAndSet(semaphore::acquireUninterruptibly)` - 原子地获取当前值，并用新值替换
   - 获取：原来的Runnable（可能是完成resultListener的逻辑）
   - 设置：新的`semaphore::acquireUninterruptibly`（方法引用，等价于`() -> semaphore.acquireUninterruptibly()`）

2. `.run()` - 执行获取到的原来的Runnable

3. `semaphore.release()` - 释放信号量

**设计意图：**

这里是一个**并发同步机制**，用于处理取消和正常完成之间的竞态条件：

```
┌─────────────────────────────────────────────────────────────────┐
│                        竞态条件处理                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  正常完成路径                    取消路径                        │
│      │                             │                            │
│      │  getAndSet(新的Runnable)    │                            │
│      │  获取原Runnable并执行        │                            │
│      ├────────────────────────────►│                            │
│      │                             │                            │
│      │                    getAndSet(semaphore::acquire)        │
│      │                             │                            │
│      │                    获取原Runnable并执行                  │
│      │                             ├─────────────────────────► │
│      │                             │                            │
│      │                    acquireUninterruptibly               │
│      │                             │ (阻塞，等待permit)          │
│      │                             │                            │
│      │  getAndSet(semaphore::acquire)                           │
│      │  (此时已无原Runnable)        │                            │
│      ├────────────────────────────►│                            │
│      │                             │                            │
│      │  release()                  │                            │
│      │  (释放permit=1)             ├─────────────────────────► │
│      │                             │                            │
│      │                             │  acquireUninterruptibly返回 │
│      │                             │                            │
│      │                             │  release()                 │
│      │                             │                            │
└─────────────────────────────────────────────────────────────────┘
```

**核心目的：** 确保在并发情况下，`resultListener`只被完成一次，并且避免在传输线程上阻塞太久。信号量提供了一个短暂的同步点，让取消路径和正常完成路径协调好。

#### 3. 引用计数和完成处理

```java
try (var refs = new RefCountingRunnable(
    new SubtasksCompletionHandler<>(resultListenerCompleter, resultListener, listener)
)) {
    while (itemsIterator.hasNext()) {
        final var item = itemsIterator.next();

        // 为每个子任务创建 listener
        final ActionListener<ItemResponse> itemResponseListener = ...;

        // 注册到取消 listener
        if (cancellableTask != null) {
            itemCancellationListener.addListener(itemResponseListener);
        }

        // 发送请求，持有一个引用计数
        ActionListener.run(
            ActionListener.releaseAfter(itemResponseListener, refs.acquire()),
            l -> sendItemRequest(item, l)
        );
    }
}
// 退出 try 块时，释放初始引用
// 当所有子任务完成时，引用计数归零，触发 SubtasksCompletionHandler
```

**SubtasksCompletionHandler 的作用**：

```java
@Override
public void run() {
    // 1. 执行 resultListenerCompleter（完成 resultListener）
    resultListenerCompleter.getAndSet(() -> {}).run();

    // 2. 将 resultListener 的结果传递给外部 listener
    assert resultListener.isDone();
    resultListener.addListener(listener);
}
```

##### RefCountingRunnable 的工作原理

**执行流程：**

```
┌─────────────────────────────────────────────────────────────────┐
│                    RefCountingRunnable 引用计数流程              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  A. 创建 RefCountingRunnable                                    │
│     └── 引用计数 = 1（初始引用）                                 │
│                                                                 │
│  B. 进入 try-with-resources 块                                 │
│                                                                 │
│  C. 循环遍历 items                                              │
│     ┌─────────────────────────────────┐                         │
│     │ itemsIterator.hasNext()?        │                         │
│     └────────────┬────────────────────┘                         │
│                  │                                              │
│         是       │         否                                   │
│          │       │          │                                   │
│          │       │          │                                   │
│          ▼       │          ▼                                   │
│     ┌─────────┐ │    ┌─────────────┐                          │
│     │refs.acquire│ │    │退出循环     │                          │
│     └────┬────┘ │    └──────┬──────┘                          │
│          │     │           │                                  │
│          ▼     │           ▼                                  │
│     引用计数++ │     退出 try-with-resources                   │
│          │     │           │                                  │
│          ▼     │           ▼                                  │
│     创建item   │     refs.close()                              │
│     ResponseListener│        │                                │
│          │     │           │                                  │
│          ▼     │           ▼                                  │
│     releaseAfter│    引用计数--                                │
│     包装listener│        │                                  │
│          │     │           │                                  │
│          ▼     │           ▼                                  │
│     sendItemRequest│  引用计数 == 0?                           │
│     (异步)    │        │                                   │
│          │     │    ┌────┴────┐                              │
│          │     │    │   否    │ → 等待其他item完成              │
│          └─────┼────┴─────────┤                                │
│                │           │是                                │
│                │           ▼                                  │
│                │    ┌──────────────────────┐                  │
│                │    │SubtasksCompletionHandler.run()!          │
│                │    ├──────────────────────┤                  │
│                │    │1. 执行resultListenerCompleter           │
│                │    │2. 调用onCompletion()生成最终结果         │
│                │    │3. 完成resultListener                    │
│                │    │4. resultListener.addListener(外部listener)│
│                │    │5. 外部listener收到最终结果                │
│                │    └──────────────────────┘                  │
│                │                                                │
│  D. 某个item完成时：                                             │
│     listener.onResponse/onFailure 被调用                         │
│          │                                                      │
│          ▼                                                      │
│     自动调用 refs.release()（由releaseAfter包装）                │
│          │                                                      │
│          ▼                                                      │
│     引用计数--                                                  │
│          │                                                      │
│          └──→ 返回C，检查引用计数是否归零                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

**关键机制：**

1. **初始引用（计数=1）：** try-with-resources创建时，RefCountingRunnable内部持有一个初始引用

2. **refs.acquire()：** 每处理一个item，就获取一个引用（引用计数+1）

3. **ActionListener.releaseAfter：** 创建一个包装的listener，在onResponse或onFailure后自动释放引用

4. **refs.close()：** 退出try-with-resources时调用，释放初始引用（引用计数-1）

5. **触发条件：** 当**所有**引用都被释放（引用计数归零）时，执行`SubtasksCompletionHandler.run()`

**完整生命周期示例（3个items）：**

```java
// 1. 创建：引用计数 = 1（初始引用）
var refs = new RefCountingRunnable(...);

// 2. 处理item1：引用计数 = 2
refs.acquire();  // item1的引用
sendItemRequest(item1, listener1); // 异步

// 3. 处理item2：引用计数 = 3
refs.acquire();  // item2的引用
sendItemRequest(item2, listener2); // 异步

// 4. 处理item3：引用计数 = 4
refs.acquire();  // item3的引用
sendItemRequest(item3, listener3); // 异步

// 5. 退出try-with-resources：引用计数 = 3
refs.close(); // 释放初始引用

// 6. item1完成：引用计数 = 2
listener1.onResponse(...); // 自动释放item1的引用

// 7. item2完成：引用计数 = 1
listener2.onResponse(...); // 自动释放item2的引用

// 8. item3完成：引用计数 = 0 → 触发SubtasksCompletionHandler.run()！
listener3.onResponse(...); // 自动释放item3的引用
```

**为什么只看到refs.acquire()没有看到refs.release()？**

因为`ActionListener.releaseAfter`在内部处理了释放：

```java
ActionListener.releaseAfter(itemResponseListener, refs.acquire())
```

这等价于：
```java
ActionListener.runAfter(itemResponseListener, refs::release)
```

当`itemResponseListener`收到响应（成功或失败）时，会先调用`itemResponseListener`的方法，然后自动调用`refs.release()`。

**设计优势：**

1. **无需预知item数量：** 引用计数机制可以动态适应任意数量的item
2. **延迟完成：** 只有当所有item都完成时才触发最终回调
3. **异常安全：** 即使某个item失败，也会正确释放引用
4. **资源管理：** try-with-resources确保初始引用一定会被释放

### 执行流程图

```
开始
  │
  ├─ 创建 resultListener（存储最终结果）
  ├─ 创建 resultListenerCompleter（完成 resultListener 的逻辑）
  ├─ 创建 itemCancellationListener（管理子任务取消）
  │
  ├─ 如果是可取消任务
  │   └─ 注册取消回调：
  │       1. 用取消异常完成 resultListener
  │       2. 释放对 this 的引用
  │       3. 通知所有子任务取消
  │
  ├─ 创建 RefCountingRunnable（引用计数）
  │   └─ 完成时执行 SubtasksCompletionHandler
  │
  ├─ 遍历所有 item
  │   ├─ 创建 itemResponseListener
  │   │   ├─ onResponse: 调用 onItemResponse(item, response)
  │   │   └─ onFailure: 调用 onItemFailure(item, exception)
  │   │
  │   ├─ 注册到 itemCancellationListener（用于取消通知）
  │   │
  │   └─ 发送请求：sendItemRequest(item, listener)
  │       └─ 持有一个引用计数
  │
  └─ 所有子任务完成
      │
      ├─ 引用计数归零
      │
      ├─ SubtasksCompletionHandler.run()
      │   ├─ 执行 resultListenerCompleter
      │   │   └─ 调用 onCompletion() 生成最终结果
      │   │   └─ 完成 resultListener
      │   │
      │   └─ resultListener.addListener(外部listener)
      │       └─ 外部 listener 收到最终结果
      │
      └─ 结束
```

### 使用示例

#### 示例 1：向多个节点收集统计信息

```java
public class NodesStatsAction extends CancellableFanOut<DiscoveryNode, NodeStats, NodesStatsResponse> {

    private final Map<String, NodeStats> results = new ConcurrentHashMap<>();
    private final Map<String, Exception> failures = new ConcurrentHashMap<>();

    @Override
    protected void sendItemRequest(DiscoveryNode node, ActionListener<NodeStats> listener) {
        // 向节点发送统计请求
        transportService.sendRequest(
            node,
            NodesStatsAction.NAME,
            new NodesStatsRequest(),
            new ActionListenerResponseHandler<>(listener, NodeStats::new)
        );
    }

    @Override
    protected void onItemResponse(DiscoveryNode node, NodeStats stats) {
        // 收集成功的响应
        results.put(node.getId(), stats);
    }

    @Override
    protected void onItemFailure(DiscoveryNode node, Exception e) {
        // 记录失败的节点
        failures.put(node.getId(), e);
    }

    @Override
    protected NodesStatsResponse onCompletion() {
        // 聚合所有结果
        return new NodesStatsResponse(
            clusterName,
            new ArrayList<>(results.values()),
            failures
        );
    }

    // 使用
    public void execute(Task task, NodesStatsRequest request, ActionListener<NodesStatsResponse> listener) {
        Iterator<DiscoveryNode> nodes = clusterService.state().nodes().iterator();
        run(task, nodes, listener);
    }
}
```

#### 示例 2：并行验证多个分片

```java
public class ShardValidationFanOut extends CancellableFanOut<ShardId, ValidationResult, Boolean> {

    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);

    @Override
    protected void sendItemRequest(ShardId shardId, ActionListener<ValidationResult> listener) {
        // 异步验证分片
        shardValidator.validate(shardId, listener);
    }

    @Override
    protected void onItemResponse(ShardId shardId, ValidationResult result) {
        if (result.isValid()) {
            successCount.incrementAndGet();
        } else {
            failureCount.incrementAndGet();
            logger.warn("Shard {} validation failed: {}", shardId, result.getReason());
        }
    }

    @Override
    protected void onItemFailure(ShardId shardId, Exception e) {
        failureCount.incrementAndGet();
        logger.error("Failed to validate shard " + shardId, e);
    }

    @Override
    protected Boolean onCompletion() {
        // 只要有一个失败就返回 false
        return failureCount.get() == 0;
    }
}
```

### 与 SubscribableListener 的关系

`CancellableFanOut` 是 `SubscribableListener` 的高级应用，展示了如何：

1. **使用多个 SubscribableListener 协同工作**：
   - `resultListener`：管理最终结果
   - `itemCancellationListener`：管理子任务取消

2. **利用订阅机制实现取消传播**：
   - 所有子任务 listener 订阅 `itemCancellationListener`
   - 取消时，通过 `notifyIfCancelled` 统一通知所有订阅者

3. **延迟完成外部 listener**：
   - `resultListener` 先完成（存储结果）
   - 等待所有子任务完成后，才将结果传递给外部 listener

### 内存管理的关键技巧

#### 1. 及时释放对 `this` 的引用

```java
// ❌ 错误：直接使用 lambda 会捕获 this
itemResponseListener = new ActionListener<>() {
    @Override
    public void onResponse(ItemResponse response) {
        onItemResponse(item, response);  // 持有 this 引用
    }
};

// ✅ 正确：使用 notifyOnce 包装，完成后释放引用
itemResponseListener = ActionListener.notifyOnce(new ActionListener<>() {
    @Override
    public void onResponse(ItemResponse response) {
        onItemResponse(item, response);  // 完成后 notifyOnce 释放引用
    }
});
```

#### 2. 使用 AtomicReference 存储可清除的引用

```java
// resultListenerCompleter 持有对 this 的引用（通过 this::onCompletion）
final var resultListenerCompleter = new AtomicReference<Runnable>(() -> {
    ActionListener.completeWith(resultListener, this::onCompletion);
});

// 取消时，替换为不持有 this 引用的 Runnable
resultListenerCompleter.getAndSet(() -> {});  // 释放对 this 的引用
```

#### 3. 避免在子类实现中捕获额外引用

```java
// ❌ 错误：lambda 捕获了 this
@Override
protected void sendItemRequest(Item item, ActionListener<ItemResponse> listener) {
    asyncMethod(item, listener.map(response -> this.transform(response)));
    // ↑ 这个 lambda 捕获了 this，阻止 GC
}

// ✅ 正确：使用方法引用或避免捕获
@Override
protected void sendItemRequest(Item item, ActionListener<ItemResponse> listener) {
    asyncMethod(item, listener.map(this::transform));  // 方法引用在 listener 完成后释放
}
```

### 最佳实践

1. **测试内存释放**：使用 `ReachabilityChecker` 测试取消后 `this` 是否可达
2. **避免捕获引用**：在实现抽象方法时，小心 lambda 和方法引用捕获 `this`
3. **快速失败**：`onItemResponse` 和 `onItemFailure` 不应抛出异常
4. **线程安全**：`onItemResponse` 和 `onItemFailure` 可能并发调用，需要线程安全
5. **取消感知**：如果子任务本身支持取消，应该在取消时主动取消它们

### 适用场景

`CancellableFanOut` 适用于以下场景：

1. **分布式聚合**：向多个节点收集数据并聚合（如集群统计、节点信息）
2. **并行验证**：并行验证多个对象（如分片、索引）
3. **批量操作**：批量执行操作并收集结果（如批量索引、批量删除）
4. **内存敏感**：结果可能很大，需要在取消时及时释放内存

### 对比：CancellableFanOut vs RefCountingListener

| 特性 | CancellableFanOut | RefCountingListener |
|------|-------------------|---------------------|
| 用途 | 扇出操作 + 结果聚合 | 简单的引用计数 |
| 取消支持 | ✅ 支持，及时释放内存 | ❌ 不支持 |
| 结果聚合 | ✅ 通过抽象方法自定义 | ❌ 只能收集到 List |
| 失败处理 | ✅ 可自定义每个子任务的失败处理 | ❌ 任一失败则整体失败 |
| 复杂度 | 较高（需要实现4个抽象方法） | 简单（直接使用） |
| 内存管理 | ✅ 取消时释放聚合结果 | ❌ 无特殊处理 |

### 总结

`CancellableFanOut` 是一个精心设计的扇出操作框架，它：

1. **使用 SubscribableListener 的多订阅者特性**管理子任务生命周期
2. **通过引用计数**确保所有子任务完成后才完成最终 listener
3. **在取消时及时释放内存**，避免大对象长时间占用堆空间
4. **提供清晰的抽象方法**，让子类专注于业务逻辑

它是 `SubscribableListener` 在复杂场景下的优秀应用示例，展示了如何在分布式系统中优雅地处理并发、取消和内存管理。
