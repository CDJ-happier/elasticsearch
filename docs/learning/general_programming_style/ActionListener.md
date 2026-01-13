# ActionListener

## 一句话总结

**ActionListener 是 Elasticsearch 异步编程的核心组件，基于装饰器模式实现了 runBefore、runAfter、delegateFailure 等实用方法，用于在原有行为基础上增强功能。**

## 核心概念

Elasticsearch 中的 runBefore、runAfter、delegateFailure 等方法是基于**装饰器模式（Decorator Pattern）**的函数式编程实践。它们的本质是：

**包装一个 ActionListener，返回一个新的 ActionListener，在原有行为基础上增加额外的逻辑。**

```
原始 Listener → 包装 → 新 Listener（增强功能）
```

## 主要方法详解

### runBefore(listener, runnable) - 前置执行

**作用**：在 listener 的 onResponse 或 onFailure 被调用之前，先执行 runnable。

#### 实现原理（来自 RunBeforeActionListener）

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

#### 典型使用场景

**无论成功与否，调用 onResponse 或 onFailure 之前都先执行某个任务。**

**示例 1：释放资源**
```java
public void loadData(ActionListener<Data> listener) {
    var lock = acquireLock();

    // 无论成功或失败，都先释放锁
    var wrappedListener = ActionListener.runBefore(listener, lock::release);

    asyncLoadData(wrappedListener);
}
```

**示例 2：记录日志**
```java
public void processRequest(String requestId, ActionListener<Response> listener) {
    var wrappedListener = ActionListener.runBefore(
        listener,
        () -> logger.info("Processing completed for request: {}", requestId)
    );

    asyncProcess(requestId, wrappedListener);
}
```

**示例 3：清理临时状态**
```java
public void executeWithTempState(ActionListener<Result> listener) {
    var tempState = createTempState();

    // 完成后清理临时状态
    var wrappedListener = ActionListener.runBefore(
        listener,
        () -> cleanupTempState(tempState)
    );

    asyncExecute(tempState, wrappedListener);
}
```

### runAfter(listener, runnable) - 后置执行

**作用**：在 listener 的 onResponse 或 onFailure 被调用之后，再执行 runnable。

#### 实现原理

```java
@Override
public void onResponse(T response) {
    try {
        delegate.onResponse(response);  // ← 先调用原始 listener
    } finally {
        runAfter.run();  // ← 然后执行后置逻辑（无论是否异常）
    }
}

@Override
public void onFailure(Exception e) {
    try {
        delegate.onFailure(e);  // ← 先调用原始 listener
    } finally {
        runAfter.run();  // ← 然后执行后置逻辑（无论是否异常）
    }
}
```

#### 典型使用场景

**在 listener 完成后执行清理或通知操作。**

**示例 1：完成计数**
```java
public void processItems(List<Item> items, ActionListener<Void> finalListener) {
    var countDown = new CountDown(items.size());

    for (var item : items) {
        var itemListener = ActionListener.runAfter(
            ActionListener.wrap(
                result -> logger.info("Item processed: {}", item),
                error -> logger.error("Item failed: {}", item, error)
            ),
            () -> {
                if (countDown.countDown()) {
                    finalListener.onResponse(null);
                }
            }
        );

        asyncProcessItem(item, itemListener);
    }
}
```

**示例 2：关闭资源**
```java
public void readFile(String path, ActionListener<String> listener) {
    var reader = new FileReader(path);

    // 读取完成后关闭文件
    var wrappedListener = ActionListener.runAfter(listener, reader::close);

    asyncReadFile(reader, wrappedListener);
}
```

**示例 3：发送通知**
```java
public void executeTask(String taskId, ActionListener<Result> listener) {
    var wrappedListener = ActionListener.runAfter(
        listener,
        () -> notificationService.send("Task " + taskId + " completed")
    );

    asyncExecuteTask(taskId, wrappedListener);
}
```

### delegateFailure(listener, biConsumer) - 委托失败

**作用**：创建一个新 listener，它的 onResponse 由你自定义，但 onFailure 会委托给原始 listener。

#### 关键点

- **成功时**：执行你的自定义逻辑 `(listener, response) -> { ... }`
- **失败时**：直接转发给原始 listener，不做任何处理
- **异常处理**：如果你的自定义逻辑抛出异常，不会被捕获（需要你自己处理）

#### 实现原理

```java
public static <T, R> ActionListener<T> delegateFailure(
    ActionListener<R> delegate,
    BiConsumer<ActionListener<R>, T> onResponse
) {
    return new ActionListener<T>() {
        @Override
        public void onResponse(T response) {
            // 执行自定义逻辑
            onResponse.accept(delegate, response);
        }

        @Override
        public void onFailure(Exception e) {
            // 直接委托给原始 listener
            delegate.onFailure(e);
        }
    };
}
```

#### 典型使用场景

**在异步操作链中，只处理成功情况，失败自动传递给最终 listener。**

**示例 1：多步异步操作**
```java
public void processOrder(String orderId, ActionListener<Receipt> finalListener) {
    // 步骤1：加载订单
    loadOrder(orderId, ActionListener.delegateFailure(finalListener, (l, order) -> {
        // 成功加载订单后，步骤2：验证订单
        validateOrder(order, ActionListener.delegateFailure(l, (l2, validationResult) -> {
            // 验证通过后，步骤3：创建收据
            if (validationResult.isValid()) {
                createReceipt(order, l2);
            } else {
                l2.onFailure(new IllegalStateException("Order validation failed"));
            }
        }));
    }));
}
```

**示例 2：转换结果类型**
```java
public void getUserName(String userId, ActionListener<String> listener) {
    // 加载用户对象，然后提取用户名
    loadUser(userId, ActionListener.delegateFailure(listener, (l, user) -> {
        l.onResponse(user.getName());
    }));
}
```

**示例 3：条件处理**
```java
public void processIfValid(String data, ActionListener<Result> listener) {
    validateData(data, ActionListener.delegateFailure(listener, (l, validationResult) -> {
        if (validationResult.isValid()) {
            asyncProcess(data, l);
        } else {
            l.onFailure(new IllegalArgumentException("Invalid data: " + validationResult.getReason()));
        }
    }));
}
```

### delegateFailureAndWrap(listener, biConsumer) - 委托失败并包装异常

**作用**：与 delegateFailure 类似，但会捕获你的自定义逻辑中的异常，并转发给 onFailure。

#### 关键区别

| 特性 | delegateFailure | delegateFailureAndWrap |
|------|----------------|------------------------|
| 成功处理 | 自定义逻辑 | 自定义逻辑 |
| 失败处理 | 委托给原始 listener | 委托给原始 listener |
| 自定义逻辑的异常 | **不捕获**，会传播 | **捕获**，转为 onFailure |

#### 实现原理

```java
public static <T, R> ActionListener<T> delegateFailureAndWrap(
    ActionListener<R> delegate,
    BiConsumer<ActionListener<R>, T> onResponse
) {
    return new ActionListener<T>() {
        @Override
        public void onResponse(T response) {
            try {
                // 执行自定义逻辑，捕获异常
                onResponse.accept(delegate, response);
            } catch (Exception e) {
                // 将异常转发给 onFailure
                delegate.onFailure(e);
            }
        }

        @Override
        public void onFailure(Exception e) {
            // 直接委托给原始 listener
            delegate.onFailure(e);
        }
    };
}
```

#### 典型使用场景

**当自定义逻辑可能抛出异常时，使用 delegateFailureAndWrap 更安全。**

**示例 1：可能抛出异常的转换**
```java
public void parseAndProcess(String json, ActionListener<Result> listener) {
    loadJson(json, ActionListener.delegateFailureAndWrap(listener, (l, jsonString) -> {
        // 解析可能抛出 JsonParseException
        var data = JsonParser.parse(jsonString);  // 异常会被捕获并转为 onFailure
        asyncProcess(data, l);
    }));
}
```

**示例 2：业务逻辑验证**
```java
public void validateAndSave(User user, ActionListener<Void> listener) {
    loadExistingUser(user.getId(), ActionListener.delegateFailureAndWrap(listener, (l, existingUser) -> {
        // 业务逻辑可能抛出异常
        if (existingUser != null && !existingUser.canBeUpdated()) {
            throw new IllegalStateException("User cannot be updated");  // 异常会被捕获
        }
        saveUser(user, l);
    }));
}
```

**示例 3：复杂的数据转换**
```java
public void transformAndStore(String rawData, ActionListener<Void> listener) {
    loadRawData(rawData, ActionListener.delegateFailureAndWrap(listener, (l, data) -> {
        // 转换逻辑可能抛出多种异常
        var transformed = complexTransformation(data);  // 任何异常都会被捕获
        storeData(transformed, l);
    }));
}
```

## 其他实用方法

### wrap(onResponse, onFailure) - 创建 Listener

**快速创建一个 ActionListener，无需实现完整的类。**

```java
ActionListener<String> listener = ActionListener.wrap(
    result -> System.out.println("Success: " + result),
    error -> System.err.println("Error: " + error)
);
```

### map(function) - 转换结果

**将 ActionListener<T> 转换为 ActionListener<R>。**

```java
public void getUser(String userId, ActionListener<User> listener) {
    // 调用者想要 User 对象
    loadUserData(userId, listener.map(userData -> new User(userData)));
}
```

### releaseAfter(listener, releasable) - 释放资源

**在 listener 完成后自动释放资源。**

```java
public void readWithLock(ActionListener<Data> listener) {
    var lock = acquireLock();

    // 无论成功或失败，都会释放锁
    var wrappedListener = ActionListener.releaseAfter(listener, lock);

    asyncRead(wrappedListener);
}
```

### notifyOnce(listener) - 防止重复通知

**确保 listener 只被通知一次，防止重复调用。**

```java
public void unreliableAsync(ActionListener<Result> listener) {
    // 防止异步操作错误地多次调用 listener
    var safeListener = ActionListener.notifyOnce(listener);

    unreliableAsyncOperation(safeListener);
}
```

## 组合使用示例

### 示例 1：完整的资源管理链

```java
public void processWithResources(String data, ActionListener<Result> listener) {
    var lock = acquireLock();
    var connection = openConnection();

    var wrappedListener = ActionListener.runBefore(
        ActionListener.releaseAfter(
            ActionListener.releaseAfter(listener, connection),
            lock
        ),
        () -> logger.info("Processing completed")
    );

    asyncProcess(data, wrappedListener);
}
```

### 示例 2：多步异步操作链

```java
public void complexWorkflow(String input, ActionListener<FinalResult> listener) {
    // 步骤1：解析输入
    parseInput(input, ActionListener.delegateFailureAndWrap(listener, (l1, parsedData) -> {

        // 步骤2：验证数据
        validateData(parsedData, ActionListener.delegateFailureAndWrap(l1, (l2, validationResult) -> {

            if (!validationResult.isValid()) {
                l2.onFailure(new IllegalArgumentException("Invalid data"));
                return;
            }

            // 步骤3：处理数据
            processData(parsedData, ActionListener.delegateFailureAndWrap(l2, (l3, processedData) -> {

                // 步骤4：保存结果
                saveResult(processedData, l3);
            }));
        }));
    }));
}
```

### 示例 3：带计数和清理的批量操作

```java
public void processBatch(List<Item> items, ActionListener<Void> finalListener) {
    var countDown = new CountDown(items.size());
    var tempResources = new ArrayList<Resource>();

    for (var item : items) {
        var resource = allocateResource();
        tempResources.add(resource);

        var itemListener = ActionListener.runAfter(
            ActionListener.releaseAfter(
                ActionListener.wrap(
                    result -> logger.info("Item {} processed", item),
                    error -> logger.error("Item {} failed", item, error)
                ),
                resource
            ),
            () -> {
                if (countDown.countDown()) {
                    // 所有 item 完成后，清理所有资源
                    tempResources.forEach(Resource::cleanup);
                    finalListener.onResponse(null);
                }
            }
        );

        asyncProcessItem(item, itemListener);
    }
}
```

## 设计模式解析

### 装饰器模式

ActionListener 的各种方法都是装饰器模式的实现：

```
┌─────────────────────────────────────────┐
│          装饰器链示例                    │
├─────────────────────────────────────────┤
│                                         │
│  原始 Listener                          │
│       ↓                                 │
│  runBefore (添加前置逻辑)                │
│       ↓                                 │
│  delegateFailure (自定义成功处理)        │
│       ↓                                 │
│  runAfter (添加后置逻辑)                 │
│       ↓                                 │
│  最终增强的 Listener                     │
│                                         │
└─────────────────────────────────────────┘
```

**优点**：
- 功能组合灵活
- 不修改原有代码
- 可以动态添加功能
- 符合开闭原则

### 函数式编程

使用 BiConsumer 等函数式接口，使代码更简洁：

```java
// 传统方式：需要创建匿名类
ActionListener<Data> listener1 = new ActionListener<>() {
    @Override
    public void onResponse(Data data) {
        // 处理逻辑
    }

    @Override
    public void onFailure(Exception e) {
        // 错误处理
    }
};

// 函数式方式：使用 lambda
ActionListener<Data> listener2 = ActionListener.wrap(
    data -> { /* 处理逻辑 */ },
    error -> { /* 错误处理 */ }
);

// 使用 delegateFailure 进一步简化
ActionListener<Data> listener3 = ActionListener.delegateFailure(finalListener, (l, data) -> {
    // 只需要处理成功情况
});
```

## 最佳实践

### 1. 选择合适的方法

- **需要前置清理**：使用 `runBefore`
- **需要后置清理**：使用 `runAfter`
- **多步异步操作**：使用 `delegateFailure` 或 `delegateFailureAndWrap`
- **可能抛出异常**：使用 `delegateFailureAndWrap` 而不是 `delegateFailure`
- **自动释放资源**：使用 `releaseAfter`

### 2. 异常处理

```java
// ❌ 错误：delegateFailure 不捕获异常
ActionListener.delegateFailure(listener, (l, data) -> {
    riskyOperation(data);  // 如果抛出异常，会传播到调用栈
    l.onResponse(result);
});

// ✅ 正确：使用 delegateFailureAndWrap
ActionListener.delegateFailureAndWrap(listener, (l, data) -> {
    riskyOperation(data);  // 异常会被捕获并转为 onFailure
    l.onResponse(result);
});
```

### 3. 避免过度嵌套

```java
// ❌ 不好：深度嵌套
loadData(ActionListener.delegateFailure(listener, (l1, data) -> {
    validate(data, ActionListener.delegateFailure(l1, (l2, validation) -> {
        process(validation, ActionListener.delegateFailure(l2, (l3, result) -> {
            save(result, ActionListener.delegateFailure(l3, (l4, saved) -> {
                // 太深了...
            }));
        }));
    }));
}));

// ✅ 更好：使用 SubscribableListener 的链式调用
SubscribableListener
    .<Data>newForked(l -> loadData(l))
    .andThen((l, data) -> validate(data, l))
    .andThen((l, validation) -> process(validation, l))
    .andThen((l, result) -> save(result, l))
    .addListener(listener);
```

### 4. 资源管理

```java
// ✅ 使用 releaseAfter 确保资源释放
var resource = acquireResource();
asyncOperation(ActionListener.releaseAfter(listener, resource));

// ✅ 组合多个资源
var resource1 = acquireResource1();
var resource2 = acquireResource2();
asyncOperation(
    ActionListener.releaseAfter(
        ActionListener.releaseAfter(listener, resource2),
        resource1
    )
);
```

### 5. 防止重复通知

```java
// ✅ 对不可信的异步操作使用 notifyOnce
var safeListener = ActionListener.notifyOnce(listener);
unreliableAsyncOperation(safeListener);
```

## 常见陷阱

### 陷阱 1：忘记调用 listener

```java
// ❌ 错误：忘记调用 listener
ActionListener.delegateFailure(listener, (l, data) -> {
    if (data.isValid()) {
        processData(data, l);
    }
    // 如果 data 无效，listener 永远不会被调用！
});

// ✅ 正确：确保所有路径都调用 listener
ActionListener.delegateFailure(listener, (l, data) -> {
    if (data.isValid()) {
        processData(data, l);
    } else {
        l.onFailure(new IllegalArgumentException("Invalid data"));
    }
});
```

### 陷阱 2：错误的异常处理

```java
// ❌ 错误：delegateFailure 不捕获异常
ActionListener.delegateFailure(listener, (l, data) -> {
    var result = parseData(data);  // 可能抛出异常！
    l.onResponse(result);
});

// ✅ 正确：使用 delegateFailureAndWrap 或手动捕获
ActionListener.delegateFailureAndWrap(listener, (l, data) -> {
    var result = parseData(data);  // 异常会被捕获
    l.onResponse(result);
});
```

### 陷阱 3：资源泄漏

```java
// ❌ 错误：异常时资源未释放
var resource = acquireResource();
asyncOperation(ActionListener.wrap(
    result -> {
        resource.close();
        listener.onResponse(result);
    },
    error -> {
        // 忘记释放资源！
        listener.onFailure(error);
    }
));

// ✅ 正确：使用 releaseAfter
var resource = acquireResource();
asyncOperation(ActionListener.releaseAfter(listener, resource));
```

## 与其他组件的关系

- **SubscribableListener**：扩展了 ActionListener，支持多订阅者
- **CancellableFanOut**：内部大量使用 ActionListener 的装饰方法
- **RefCountingRunnable**：提供 `acquireListener()` 返回 ActionListener

## 总结

ActionListener 的装饰器方法是 Elasticsearch 异步编程的基础工具，通过简单的组合就能实现复杂的异步流程控制：

- **runBefore/runAfter**：添加前置/后置逻辑
- **delegateFailure/delegateFailureAndWrap**：简化异步操作链
- **releaseAfter**：自动资源管理
- **notifyOnce**：防止重复通知

掌握这些方法，能够写出更简洁、更安全的异步代码。
