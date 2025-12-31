
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
