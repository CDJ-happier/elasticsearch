/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;

import java.util.Iterator;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Allows an action to fan-out to several sub-actions and accumulate their results, but which reacts to a cancellation by releasing all
 * references to itself, and hence the partially-accumulated results, allowing them to be garbage-collected. This is a useful protection for
 * cases where the results may consume a lot of heap (e.g. stats) but the final response may be delayed by a single slow node for long
 * enough that the client gives up.
 * <p>
 * Note that it's easy to accidentally capture another reference to this class when implementing it, and this will prevent the early release
 * of any accumulated results. Beware of lambdas and method references. You must test your implementation carefully (using e.g.
 * {@code ReachabilityChecker}) to make sure it doesn't do this.
 *
 * <h2>主要职责</h2>
 * <p>
 * 该类是一个抽象的fan-out（扇出）任务执行框架，负责将一个主任务并发地分发到多个子任务（Item）执行，聚合所有子任务的结果，
 * 并提供优雅的取消机制。当主任务被取消时，能够快速释放所有已积累的结果和引用，避免长时间占用堆内存。
 * </p>
 *
 * <h2>工作机制</h2>
 * <p>
 * 1. <strong>任务分发阶段</strong>：在调用线程上迭代所有items，为每个item创建独立的ActionListener，并通过sendItemRequest()发送请求
 * <br>
 * 2. <strong>结果聚合阶段</strong>：通过RefCountingRunnable跟踪所有未完成的子任务，每个子任务完成时通过refs.acquire()获取引用，
 * 任务完成后自动调用Releasable释放引用
 * <br>
 * 3. <strong>取消处理机制</strong>：使用AtomicReference包装resultListenerCompleter，当任务取消时，通过信号量semaphore实现同步，
 * 确保取消逻辑和完成逻辑的正确执行顺序。如果任务被取消，立即调用itemCancellationListener通知所有未完成的item监听器
 * <br>
 * 4. <strong>最终完成处理</strong>：当所有子任务都完成时（refs引用计数归零），SubtasksCompletionHandler.run()被执行，
 * 完成resultListener并最终完成外部listener
 * </p>
 *
 * <h2>在架构中的位置</h2>
 * <p>
 * 该类属于 {@code org.elasticsearch.action.support} 模块，是Elasticsearch中处理集群级别操作（如节点状态查询、统计信息收集等）的核心工具类。
 * <br>
 * <strong>创建者</strong>：通常由TransportActions（如TransportListTasksAction、TransportNodesInfoAction等）创建，用于向集群中的多个节点发送并发请求
 * <br>
 * <strong>使用者</strong>：作为ActionListener的包装器，与TransportService协作，通过sendItemRequest()抽象方法让子类实现具体的请求发送逻辑
 * <br>
 * 典型应用场景包括：查询所有节点的任务列表、收集集群统计信息、批量索引操作等需要协调多个节点响应的操作
 * </p>
 *
 * <h2>关键设计点</h2>
 * <p>
 * <strong>1. 引用计数与资源释放</strong>：使用RefCountingRunnable和AtomicReference实现精确的引用管理，确保在任务取消时能够快速释放所有累积的结果
 * <br>
 * <strong>2. 取消通知机制</strong>：通过SubscribableListener实现发布-订阅模式的取消通知，itemCancellationListener可以向所有订阅的item监听器广播取消事件
 * <br>
 * <strong>3. 线程安全设计</strong>：使用Semaphore确保取消逻辑和完成逻辑的同步，避免在transport线程上阻塞过长时间
 * <br>
 * <strong>4. 防止内存泄漏</strong>：通过notifyOnce包装器确保每个item的监听器只被调用一次，并警告开发者注意lambdas和方法引用可能导致的隐式引用捕获
 * <br>
 * <strong>5. 延迟完成策略</strong>：在任务取消时，不立即完成外部listener，而是等待所有子任务完成后再通知，避免向外部暴露不一致的中间状态
 * </p>
 */
public abstract class CancellableFanOut<Item, ItemResponse, FinalResponse> {

    private static final Logger logger = LogManager.getLogger(CancellableFanOut.class);

    /**
     * Run the fan-out action.
     * <p>
     *   我理解该函数的作用是：TODO
     *   task用于取消任务。itemsIterator是获取需要执行的items，一般是节点列表。listener是外部调用fanout时提供的监听器，用于所有items都完成时调用
     *   其对应的回调（成功或失败）。
     *   fanout内部进行了封装处理，涉及resultListenerCompleter, resultListener, listener
     *   resultListenerCompleter用于完成resultListener，TODO在什么地方调用？（客户端取消任务 -> cancellableTask.addListener添加的逻辑 -> resultListenerCompleter）
     * </p>
     *
     * @param task          The task to watch for cancellations. If {@code null} or not a {@link CancellableTask} then the fan-out still
     *                      works, just without any cancellation handling. task可以为空，或者不是CancellableTask，fan-out仍然可以工作，只是没有任何取消处理
     * @param itemsIterator The items over which to fan out. Iterated on the calling thread. fanout的items，在调用线程上迭代
     * @param listener      A listener for the final response, which is completed after all the fanned-out actions have completed. It is not
     *                      completed promptly on cancellation. Completed on the thread that handles the final per-item response (or
     *                      the calling thread if there are no items). 最终的响应监听器，在所有fanned-out动作完成后完成。不及时完成取消。
     */
    public final void run(@Nullable Task task, Iterator<Item> itemsIterator, ActionListener<FinalResponse> listener) {

        final var cancellableTask = task instanceof CancellableTask ct ? ct : null;

        // Captures the final result as soon as it's known (either on completion or on cancellation) without necessarily completing the
        // outer listener, because we do not want to complete the outer listener until all sub-tasks are complete
        // 不管是成功还是失败，都尽快捕获最终结果，但是不会完成外部listener，除非所有子任务都完成。
        final var resultListener = new SubscribableListener<FinalResponse>();

        // Completes resultListener (either on completion or on cancellation). Captures a reference to 'this', but within an
        // 'AtomicReference' which is cleared, releasing the reference promptly, when executed.
        // 这个resultListenerCompleter是用来在完成或者取消时完成resultListener的，它捕获了this的引用，但是在一个AtomicReference中被清除，
        final var resultListenerCompleter = new AtomicReference<Runnable>(() -> {
            if (cancellableTask != null && cancellableTask.notifyIfCancelled(resultListener)) {
                return; // 如果任务已经取消，则通知resultListener并返回
            }
            // It's important that we complete resultListener before returning, because otherwise there's a risk that a cancellation arrives
            // later which might unexpectedly complete the final listener on a transport thread.
            ActionListener.completeWith(resultListener, this::onCompletion); // 将onCompletion的结果传递给resultListener.onResponse()
        });

        // Collects the per-item listeners up so they can all be completed exceptionally on cancellation. Never completed successfully.
        // 如果任务是可取消的，则添加一个监听器，当任务被取消时，会执行这个监听器，即这里会通过信号量来实现，最后通知itemCancellationListener，进而通知
        // 所有订阅了itemCancellationListener的监听器，也就是后续实际执行每个item的itemResponseListener。
        final var itemCancellationListener = new SubscribableListener<ItemResponse>();
        if (cancellableTask != null) {
            cancellableTask.addListener(() -> { // 这里是给cancellableTask添加一个监听器，当任务完成时，会执行这个监听器
                assert cancellableTask.isCancelled();
                // probably on a transport thread and we don't know if any of the callbacks are slow so we must avoid running them by
                // blocking the thread which might add a subscriber to resultListener until after we've completed it
                final var semaphore = new Semaphore(0);
                // resultListenerCompleter is currently either a no-op, or else it immediately completes resultListener with a cancellation
                // while it has no subscribers, so either way this semaphore is not held for long
                resultListenerCompleter.getAndSet(semaphore::acquireUninterruptibly).run(); // 有什么用？含义？
                semaphore.release(); // semaphore增加1一个许可
                // finally, release refs to all the per-item listeners (without calling onItemFailure, so this is also fast)
                cancellableTask.notifyIfCancelled(itemCancellationListener);
                // 此处如果cancellableTask任务取消了，则调用itemCancellationListener.onFailure() -> 调用订阅了itemCancellationListener的所有监听器
            });
        }

        // SubtasksCompletionHandler三个参数： resultListenerCompleter, resultListener, listener
        // 这里发起多个item任务后，是如何等待所有item任务完成的？这里SubtasksCompletionHandler是如何参与的？refs为什么只有acquire()
        try (var refs = new RefCountingRunnable(new SubtasksCompletionHandler<>(resultListenerCompleter, resultListener, listener))) {
            while (itemsIterator.hasNext()) {
                final var item = itemsIterator.next();

                // Captures a reference to 'this', but within a 'notifyOnce' so it is released promptly when completed. 啥意思？什么机制？
                // 这里定义每个item的ResponseListener
                final ActionListener<ItemResponse> itemResponseListener = ActionListener.notifyOnce(new ActionListener<>() {
                    @Override
                    public void onResponse(ItemResponse itemResponse) {
                        try {
                            onItemResponse(item, itemResponse);
                        } catch (Exception e) {
                            logger.error(
                                () -> Strings.format(
                                    "unexpected exception handling [%s] for item [%s] in [%s]",
                                    itemResponse,
                                    item,
                                    CancellableFanOut.this
                                ),
                                e
                            );
                            assert false : e;
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        if (cancellableTask != null && cancellableTask.isCancelled()) {
                            // Completed on cancellation so it is released promptly, but there's no need to handle the exception.
                            return;
                        }
                        onItemFailure(item, e); // must not throw, enforced by the ActionListener#notifyOnce wrapper
                    }

                    @Override
                    public String toString() {
                        return "[" + CancellableFanOut.this + "][" + listener + "][" + item + "]";
                    }
                });

                if (cancellableTask != null) {
                    if (cancellableTask.isCancelled()) {
                        return;
                    }

                    // Register this item's listener for prompt cancellation notification.
                    // 订阅itemCancellationListener，当itemCancellationListener完成时，会调用itemResponseListener响应的方法
                    itemCancellationListener.addListener(itemResponseListener);
                }

                // Process the item, capturing a ref to make sure the outer listener is completed after this item is processed.
                // refs.acquire()返回的是一个Releasable，会在合适
                ActionListener.run(ActionListener.releaseAfter(itemResponseListener, refs.acquire()), l -> sendItemRequest(item, l));
            }
        } catch (Exception e) {
            // NB the listener may have been completed already (by exiting this try block) so this exception may not be sent to the caller,
            // but we cannot do anything else with it; an exception here is a bug anyway.
            logger.error("unexpected failure in [" + this + "][" + listener + "]", e);
            assert false : e;
            throw e;
        }
    }

    /**
     * Run the action (typically by sending a transport request) for an individual item. Called in sequence on the thread that invoked
     * {@link #run}. May not be called for every item if the task is cancelled during the iteration.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void sendItemRequest(Item item, ActionListener<ItemResponse> listener);

    /**
     * Handle a successful response for an item. May be called concurrently for multiple items. Not called if the task is cancelled. Must
     * not throw any exceptions.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void onItemResponse(Item item, ItemResponse itemResponse);

    /**
     * Handle a failure for an item. May be called concurrently for multiple items. Not called if the task is cancelled. Must not throw any
     * exceptions.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract void onItemFailure(Item item, Exception e);

    /**
     * Called when responses for all items have been processed, on the thread that processed the last per-item response or possibly the
     * thread which called {@link #run} if all items were processed before {@link #run} returns. Not called if the task is cancelled.
     * <p>
     * Note that it's easy to accidentally capture another reference to this class when implementing this method, and that will prevent the
     * early release of any accumulated results. Beware of lambdas, and test carefully.
     */
    protected abstract FinalResponse onCompletion() throws Exception;

    private static class SubtasksCompletionHandler<FinalResponse> implements Runnable {
        private final AtomicReference<Runnable> resultListenerCompleter;
        private final SubscribableListener<FinalResponse> resultListener;
        private final ActionListener<FinalResponse> listener;

        private SubtasksCompletionHandler(
            AtomicReference<Runnable> resultListenerCompleter,
            SubscribableListener<FinalResponse> resultListener,
            ActionListener<FinalResponse> listener
        ) {
            this.resultListenerCompleter = resultListenerCompleter;
            this.resultListener = resultListener;
            this.listener = listener;
        }

        @Override
        public void run() {
            // When all sub-tasks are complete, pass the result from resultListener to the outer listener.
            // 这里get到的Runnable可能是原本初始化时的完成resultListener的Runnable，也可能是被设置的semaphore.acquireUninterruptibly()
            // 如果是前者，说明任务没有发起取消，走到这里是所有fanout的items子任务都完成了，调用该Runnable完成resultListener
            // 如果是后者，说明在所有fanout的items子任务完成之前发生了任务取消，此时执行的是semaphore.acquireUninterruptibly()
            //   这里会获取一个许可，显然，在任务取消是通过release增加了一个许可，这里是可以正常走后续流程的。但这里的前提是需要在取消任务线程执行完成之后
            //   才会执行release。
            resultListenerCompleter.getAndSet(() -> {}).run();
            // May block (very briefly) if there's a concurrent cancellation, so that we are sure the resultListener is now complete and
            // therefore the outer listener is completed on this thread.
            assert resultListener.isDone();
            resultListener.addListener(listener);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + listener.toString() + "]";
        }
    }
}
