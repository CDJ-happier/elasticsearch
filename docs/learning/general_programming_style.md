# 通用编程模式/思想

本目录记录了 Elasticsearch 中常用的通用编程组件和模式。为了方便维护和查阅，每个组件都有独立的文档。

## 组件列表

### [ActionListener](general_programming_style/ActionListener.md)
Elasticsearch 异步编程的核心组件，基于装饰器模式实现了 runBefore、runAfter、delegateFailure 等实用方法。
- runBefore/runAfter：前置/后置执行逻辑
- delegateFailure：委托失败处理
- delegateFailureAndWrap：捕获异常并委托失败处理

### [SubscribableListener](general_programming_style/SubscribableListener.md)
可以被多个 ActionListener 订阅的 ActionListener，用于让多个组件共享同一个异步操作的结果。
- 支持多订阅者
- 延迟订阅
- 结果缓存
- 链式调用（andThen、andThenApply、andThenAccept）
- 超时控制

### [CancellableFanOut](general_programming_style/CancellableFanOut.md)
支持取消的扇出操作框架，用于并发执行多个子任务并聚合结果，在取消时能够及时释放内存。
- 扇出模式（向多个节点发送请求）
- 结果聚合
- 取消支持
- 内存管理优化

### [RefCountingRunnable](general_programming_style/RefCountingRunnable.md)
基于引用计数的异步任务协调器，允许动态地获取引用，并在所有引用释放后自动执行委托的 Runnable。
- 动态引用获取（无需预先声明数量）
- 自动完成检测
- 异常安全
- 线程安全

## 组件关系

这些组件之间有着清晰的层次关系和协作关系：

1. **ActionListener** 是基础，所有异步操作都基于它
2. **SubscribableListener** 扩展了 ActionListener，支持多订阅者和链式调用
3. **CancellableFanOut** 使用 SubscribableListener 实现扇出操作和取消机制
4. **RefCountingRunnable** 用于协调多个异步操作的完成，常与上述组件配合使用

## 使用建议

- 简单的异步操作：使用 **ActionListener** 的装饰器方法
- 需要多个组件共享结果：使用 **SubscribableListener**
- 多步异步操作链：使用 **SubscribableListener** 的链式调用
- 扇出聚合操作：使用 **CancellableFanOut**
- 等待多个异步操作完成：使用 **RefCountingRunnable**
