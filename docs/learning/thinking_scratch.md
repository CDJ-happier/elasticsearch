# 手稿

## 如何了解一个类/模块
1. 先了解其核心功能是什么，其理论原理/机制
2. 其对外提供哪些方法？（有些类虽然方法很多，但实际提供给外部使用的也就几个，即入口）
3. 实现机制/方法包括子部分？（按照1、2的方式了解子部分）
4. 子部分如何运作形成整体？


## 基础思想

Listener：监听者，设置一些方法，比如onResponse、onFailure等，作为参数传入后由执行者在适当时机调用对应方法；

Supplier：提供者，函数式接口，通过get提供对象

Consumer：消费者，函数式接口，通过accept消费对象

Applier：应用者，

## 选主
Node.start() -> coordinator.startInitialJoin() -> becomeCandidate() -> peerFinder.activate(nodes) -> handleWakeup() ->

TODO：onFoundPeersUpdated(), onActiveMasterFound这两个函数在发现足够多的peer或者在发现过程中就遇到了master时会调用


## 搜索请求的历程
Netty -> ES Controller -> ES RestHandler.handleRequest -> prepareRequest(各个RestXXXAction实现)

prepareRequest返回一个consumer(channel), 这里channel就是客户的连接, 即这个consumer里面执行请求, 并在成功或失败后回调
channel.

在consumer中, 一般是通过NodeClient执行TransportXXXAction, 将channel传入listener.
NodeClient.execute -> doExecute -> executeLocally(action, request, listener) -> taskManager.registerAndExecute

NodeClient交给taskManager.

taskManager.registerAndExecute(type, action, request, connection, listener)
这里是从transport进入到task的入口，其中listener持有对channel的异步回调，然后listener在该函数里面会有成为别人的回调，
也就是实际action.execute()执行后会回调listener -> 回调channel -> 结果返回给请求者

action.execute -> TransportAction.handleExecution -> doExecute/doExecuteForking(后者是对前者的封装) -> TransportXXXAction.doExecute
这里又回到TransportXXXAction。
我理解不直接执行action是为了经过taskManager，方便记录、管理，在taskManager内部其实还是调用action.execute()，回到了action。

TransportSearchAction.doExecute(task, request, listener), 执行后会回调listener。这个doExecute是搜索的入口，在里面应该会分解任务，
比如可能会CCS，搜索会跨多个节点等，非常复杂。


## TaskManager

TODO：
1. 搞清楚register任务时，其内部startTrace是如何跟踪任务的（APMTracer）。在unregister中会stopTrace（此时会记录一些指标，比如运行时间？）


### Ack机制 TODO

ExecutionResult -> TaskAckListener -> ContextPreservingAckListener -> ClusterStateAckListener
1. ContextPreservingAckListener最内层包装，在ack回调时负责恢复线程上下文；
2. TaskAckListener负责ack的计数和超时管理，使用CountDown机制，支持超时调度；
3. ExecutionResult最外层，负责整个任务执行的生命周期。实现了TaskContext接口，支持多种回调机制；有一个getContextPreservingAckListener()方法；

任务提交阶段：将更新任务封装为ExecutionResult，提供多种异步回调。如果任务需要ack确认，则创建TaskAckListener和ContextPreservingAckListener的包装链

任务执行阶段：提交任务，进入ack等待阶段，TaskAckListener开始计数，等待所有必需节点的确

ACK确认阶段：其他节点收到集群状态更新后发送ack，askAckListener接收ack并递减计数器并满足条件后触发回调

回调阶段：ContextPreservingAckListener确保回调在正确的线程上下文中执行，最终通知原始的任务监听器

BatchingTaskQueue.submitTask() -> 将任务封装为Entry -> 有一个参数是Supplier<StoredContext> -> 就是给ContextPreservingAckListener使用的

在Processor.run()中会取出task并封装成ExecutionResult

## MasterService

Map<Priority, PerPriorityQueue> queuesByPriority：按优先级分类队列

createTaskQueue(this::executeAndPublishBatch, ...) -> BatchingTaskQueue.submitTask() ->
1. 向btq.queue添加任务
2. 执行该任务所在的perPriorityQueue.execute(processor), 每个btq会初始化一个processor, 是Batch的实现

ppq中将任务processor(Batch实现)添加到ppq.queue -> forkQueueProcessor() -> 其实就是fork了一个新线程执行一个全局的queuesProcessor.doRun()
1. 调用takeNextBatch()获取全局的一个任务batch（优先级最高）
2. 并batch.run(), 在onResponse会调用调用onCompletion -> forkQueueProcessor()，重复获取任务并执行
3. batch.run()就是btq.processor.run(), 此时batch.queue就是btq.queue(也就是submitTask添加的任务)
4. 最后调用batchConsumer.runBatch(executor, task, ...). 而batchConsumer就是createTaskQueue的第一个参数
5. 到executeAndPublishBatch这里才算开始真正的发布任务（集群状态发布任务）

总结：
createTaskQueue(name, priority, executor).submitTask(source, task, timeout)
1. 生成一个BatchingTaskQueue btq，Batching表示批量存储的任务队列（比如可以通过submitTask添加多个任务）
2. 提交任务时，task是一个接口，timeout应该是延迟执行？（我看代码里面是延迟，但字面意思感觉是超时时间）
3. submitTask这里会将task封装为Entry，放入到btq.queue，并调用perPriorityQueue.execute(processor)执行任务，这里processor持有btq.queue中的任务（即封装的Entry）
4. processor本身作为一个任务添加到perPriorityQueue.queue中，然后执行forkQueueProcessor() -> 本质是启动一个线程执行全局的queuesProcessor.doRun()
5. queuesProcessor.doRun() -> takeNextBatch() -> 获取全局队列queuesByPriority中的一个batch(processor类型)
6. 然后调用processor.run() -> 执行btq.queue中的任务Entry
7. 通过batchConsumer.runBatch(executor, tasks, ...)执行任务 -> batchConsumer就是this::executeAndPublishBatch

TODO：理清createTaskQueue(name, priority, executor)的参数及最终this::executeAndPublishBatch(executor, executionResults, summary, listener)参数的关系
1. executor就是最开始的executor
2. executionResults对应submitTask的次数，即存储任务Entry。TODO：executionResult必然也有方法会被异步回调，这个回调就是submitTask时任务task自己的回调逻辑。
3. summary：TODO
4. listener：这个是内部机制，用于执行完这个任务后重新触发forkQueueProcessor().
   具体是queuesProcessor.doRun -> ActionListener.run(l1, batch(l1)) -> batch(l1).run() -> ActionListener.run(l11, this::executeAndPublishBatch)
   而最开始的l1.onCompletion -> forkQueueProcessor -> queuesProcessor.doRun，实现只要全局队列不为空，就持续执行


### 超时取消机制

在BatchTaskQueue.submitTask(source, task, timeout)中，如果timeout不为null，则通过threadPool.schedule()生成一个timeout后执行的任务
new TaskTimeoutHandler<>(timeout, source, taskHolder)，会在timeout后将taskHolder置为null，即取消任务。同时！！！返回一个timeoutCancellable，
在processor.run()中entry.acquireForExecution()中判断任务是否设置了超时（即timeoutCancelable不为null）并且还未被取消（taskHolder不为null），
如果是，则调用timeoutCancellable.cancel()取消TaskTimeoutHandler的执行，即表明这个任务接下来会被处理，避免被取消（置为null）

### createTaskQueue被调用的模块

集群协调相关

1. JoinHelper：处理节点加入集群
2. Coordinator：集群协调器，处理节点离开
3. NodeJoinExecutorTests：测试节点加入

分片和分配相关

1. ShardStateAction：处理分片状态变更
2. AllocationService：处理分片分配
3. DesiredBalanceShardsAllocator：期望平衡分片分配器

元数据管理相关

1. MetadataIndexStateService：索引状态管理（打开/关闭索引等）
2. MetadataMappingService：映射管理
3. MetadataIndexAliasesService：索引别名管理
4. MetadataIndexTemplateService：索引模板管理

快照和恢复
1. SnapshotsService：快照服务
2. RepositoriesIT：仓库集成测试

其他核心服务
1. IngestService：数据摄取管道
2. ReservedClusterStateService：保留状态服务
3. HealthMetadataService：健康元数据服务
4. DataStreamLifecycleService：数据流生命周期管理


## 一些基础类

ThreadContext：就是一个map，用于string headers以及keyed objects；
