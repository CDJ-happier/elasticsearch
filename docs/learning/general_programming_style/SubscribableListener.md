# SubscribableListener

## 一句话总结

**`SubscribableListener` 是一个可以被多个 `ActionListener` 订阅的 `ActionListener`，用于让多个组件共享同一个异步操作的结果。**

## 什么是 SubscribableListener？

`SubscribableListener<T>` 本质上是一个**可以被多个 ActionListener 订阅的 ActionListener**。

### 核心概念解释

**订阅什么？**
- 订阅的是一个**异步操作的结果**（成功或失败）

**谁是订阅者？**
- 订阅者是普通的 `ActionListener`，它们想要获得某个异步操作的结果

**如何订阅？**
- 通过调用 `subscribableListener.addListener(myListener)` 来订阅

**订阅后会发生什么？**
- 当 `SubscribableListener` 被完成时（调用 `onResponse` 或 `onFailure`），它会自动通知所有订阅者
- 如果订阅时 `SubscribableListener` 已经完成，订阅者会立即收到结果

### 一个简单的例子理解订阅

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

## 核心特性

1. **多订阅者支持**：多个 `ActionListener` 可以订阅同一个异步操作的结果
2. **延迟订阅**：可以在操作完成前或完成后订阅，订阅者都能收到结果
3. **结果缓存**：一旦完成（成功或失败），结果会被缓存，后续订阅者立即获得该结果
4. **链式调用**：支持类似 `CompletionStage` 的链式异步操作，但不捕获 `Throwable`
5. **线程安全**：使用 CAS 操作保证多线程环境下的安全性

## 内部实现原理

### 状态管理
`SubscribableListener` 使用一个 `volatile Object state` 字段来管理状态：

- **未完成状态**：
  - `EMPTY`：没有订阅者
  - 单个 `ActionListener`：只有一个订阅者
  - `Cell` 链表头：多个订阅者（逆序链表）

- **完成状态**：
  - `SuccessResult<T>`：成功完成，包含结果
  - `FailureResult`：失败完成，包含异常

### 订阅机制
```java
public final void addListener(ActionListener<T> listener, Executor executor, @Nullable ThreadContext threadContext)
```

- 如果已完成：立即在当前线程完成订阅者
- 如果未完成：将订阅者加入链表，等待完成时通知
- 使用 `VarHandle` 进行 CAS 操作，保证线程安全

### 完成机制
```java
private void setResult(Object result)
```

- 使用 CAS 操作设置结果，保证只完成一次
- 将订阅者链表反转（恢复订阅顺序）
- 按订阅顺序依次完成所有订阅者

## 典型应用场景

### 场景 1：避免重复执行昂贵的异步操作

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

### 场景 2：构建清晰的异步操作链

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

### 场景 3：扇出操作 - 一个结果通知多个处理器

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

### 场景 4：超时控制

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

## 使用方式详解

### 步骤 1：创建 SubscribableListener

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

### 步骤 2：将 SubscribableListener 传给异步方法

```java
// 创建 SubscribableListener
var listener = new SubscribableListener<String>();

// 将它作为 ActionListener 传给异步方法
// 当异步操作完成时，会调用 listener.onResponse() 或 listener.onFailure()
someAsyncMethod(args, listener);

// 此时 listener 代表"someAsyncMethod 的执行结果"
```

### 步骤 3：添加订阅者（等待结果）

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

### 步骤 4：完成 SubscribableListener（通知所有订阅者）

```java
// 异步操作成功完成时
listener.onResponse(result);
// 此时所有订阅者的 onResponse 方法会被调用

// 或者异步操作失败时
listener.onFailure(exception);
// 此时所有订阅者的 onFailure 方法会被调用
```

### 高级用法：链式操作

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

## 完整使用实例

### 实例 1：多步异步操作链
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

### 实例 2：带超时的资源加载
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

### 实例 3：线程上下文传播
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

## 线程模型和执行器

### 订阅者的执行线程
订阅者的执行线程取决于两个因素：

1. **订阅时机**：
   - 如果 `SubscribableListener` 已完成：订阅者在**调用 `addListener` 的线程**上立即执行
   - 如果 `SubscribableListener` 未完成：订阅者在**完成 `SubscribableListener` 的线程**上执行

2. **指定的执行器**：
   - 如果传入 `EsExecutors.DIRECT_EXECUTOR_SERVICE`：直接在上述线程执行
   - 如果传入其他执行器：尝试在该执行器上执行（如果被拒绝，则在原线程上以异常完成）

### 确保特定执行器执行的方法
要确保订阅者在特定执行器上执行，必须同时满足：
1. 调用 `addListener` 时传入该执行器
2. 确保 `SubscribableListener` 总是在该执行器上完成

## 与 CompletionStage 的对比

| 特性 | SubscribableListener | CompletionStage |
|------|---------------------|-----------------|
| 异常处理 | 只处理 `Exception` | 捕获所有 `Throwable` |
| 多订阅者 | 原生支持 | 需要额外处理 |
| 线程控制 | 精确控制（Executor + ThreadContext） | 仅 Executor |
| 订阅顺序 | 保证按订阅顺序完成 | 无保证 |
| 使用场景 | Elasticsearch 内部异步操作 | 通用 Java 异步编程 |

## 最佳实践

1. **异常安全**：`andThen`、`andThenApply`、`andThenAccept` 都是异常安全的，抛出的异常会自动传递给 listener
2. **避免阻塞**：订阅者不应执行阻塞操作，否则会阻塞完成线程
3. **线程上下文**：需要传播线程上下文时，显式传入 `ThreadContext` 参数
4. **超时处理**：长时间运行的操作应添加超时，并在超时时清理资源
5. **订阅顺序**：依赖订阅顺序时，确保在完成前添加所有订阅者
