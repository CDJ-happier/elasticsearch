# Elasticsearch 写入与复制机制深度分析

## 概述

本文档深入分析 Elasticsearch 的完整写入流程，包括：
1. 主分片的写入操作（从 TransportShardBulkAction 到 InternalEngine）
2. 动态 Mapping 更新与集群状态发布机制
3. 副本分片的复制机制
4. 一致性保证与等待策略

## 核心问题

1. **主分片如何执行写入操作？**
2. **动态 Mapping 更新如何触发集群状态变更？**
3. **主分片成功后是否需要等待副本分片？**
4. **是否有多数派阈值（Quorum）机制？**
5. **何时响应客户端？**

## 源码分析

### 第一部分：主分片写入流程

#### 1. 写入请求的入口（TransportShardBulkAction）

写入请求从 `TransportShardBulkAction` 开始处理：

```java
@Override
protected void dispatchedShardOperationOnPrimary(
    BulkShardRequest request,
    IndexShard primary,
    ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener
) {
    ClusterStateObserver observer = new ClusterStateObserver(
        clusterService, request.timeout(), logger, threadPool.getThreadContext()
    );

    // 注意第5个参数 mappingUpdater，用于动态更新 mapping
    performOnPrimary(
        request,
        primary,
        updateHelper,
        threadPool::absoluteTimeInMillis,
        // Mapping 更新回调
        (update, shardId, mappingListener) -> {
            assert update != null;
            assert shardId != null;
            // 更新 mapping 到 master 节点
            mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
        },
        // 等待 mapping 更新完成的回调
        (mappingUpdateListener, initialMappingVersion) -> observer.waitForNextChange(
            new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    mappingUpdateListener.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    mappingUpdateListener.onFailure(
                        new MapperException("timed out while waiting for a dynamic mapping update")
                    );
                }
            },
            clusterState -> {
                var indexMetadata = clusterState.metadata().index(primary.shardId().getIndex());
                return indexMetadata == null
                    || (indexMetadata.mapping() != null
                        && indexMetadata.getMappingVersion() != initialMappingVersion);
            }
        ),
        listener,
        executor(primary),
        postWriteRefresh,
        postWriteAction,
        documentParsingProvider
    );
}
```

**关键点：**
- `mappingUpdater` 回调用于将 mapping 更新发送到 master 节点
- `waitForMappingUpdate` 回调用于等待集群状态更新完成
- 使用 `ClusterStateObserver` 监听集群状态变化

#### 2. 主分片批量操作执行（performOnPrimary）

```java
public static void performOnPrimary(
    BulkShardRequest request,
    IndexShard primary,
    UpdateHelper updateHelper,
    LongSupplier nowInMillisSupplier,
    MappingUpdatePerformer mappingUpdater,
    ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
    ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
    Executor executor,
    @Nullable PostWriteRefresh postWriteRefresh,
    @Nullable Consumer<Runnable> postWriteAction,
    DocumentParsingProvider documentParsingProvider
) {
    new ActionRunnable<>(listener) {
        private final BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);
        final long startBulkTime = System.nanoTime();
        private final ActionListener<Void> onMappingUpdateDone = ActionListener.wrap(
            v -> executor.execute(this),
            this::onRejection
        );

        @Override
        protected void doRun() throws Exception {
            // 循环处理每个 bulk 操作项
            while (context.hasMoreOperationsToExecute()) {
                if (executeBulkItemRequest(
                    context,
                    updateHelper,
                    nowInMillisSupplier,
                    mappingUpdater,
                    waitForMappingUpdate,
                    onMappingUpdateDone,
                    documentParsingProvider
                ) == false) {
                    // 需要等待 mapping 更新，暂停处理
                    // mapping 更新完成后会重新调用此方法
                    return;
                }
                assert context.isInitial();
            }

            primary.getBulkOperationListener().afterBulk(
                request.totalSizeInBytes(),
                System.nanoTime() - startBulkTime
            );

            // 所有操作完成，构造响应
            finishRequest();
        }

        private void finishRequest() {
            ActionListener.completeWith(
                listener,
                () -> new WritePrimaryResult<>(
                    context.getBulkShardRequest(),
                    context.buildShardResponse(),
                    context.getLocationToSync(),
                    context.getPrimary(),
                    logger,
                    postWriteRefresh,
                    postWriteAction
                )
            );
        }
    }.run();
}
```

**关键流程：**
1. 创建 `BulkPrimaryExecutionContext` 上下文对象
2. 循环处理每个 bulk 操作项
3. 如果需要 mapping 更新，暂停处理并等待
4. 所有操作完成后构造 `WritePrimaryResult`

#### 3. 单个操作项的执行（executeBulkItemRequest）

```java
static boolean executeBulkItemRequest(
    BulkPrimaryExecutionContext context,
    UpdateHelper updateHelper,
    LongSupplier nowInMillisSupplier,
    MappingUpdatePerformer mappingUpdater,
    ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
    ActionListener<Void> itemDoneListener,
    DocumentParsingProvider documentParsingProvider
) throws Exception {
    final DocWriteRequest.OpType opType = context.getCurrent().opType();

    // 1. 处理 UPDATE 请求，转换为 INDEX 或 DELETE
    final UpdateHelper.Result updateResult;
    if (opType == DocWriteRequest.OpType.UPDATE) {
        final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
        try {
            updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
        } catch (Exception failure) {
            final Engine.Result result = new Engine.IndexResult(
                failure, updateRequest.version(), updateRequest.id()
            );
            context.setRequestToExecute(updateRequest);
            context.markOperationAsExecuted(result);
            context.markAsCompleted(context.getExecutionResult());
            return true;
        }

        // NOOP 操作，直接返回
        if (updateResult.getResponseResult() == DocWriteResponse.Result.NOOP) {
            context.markOperationAsNoOp(updateResult.action());
            context.markAsCompleted(context.getExecutionResult());
            context.getPrimary().noopUpdate();
            return true;
        }
        context.setRequestToExecute(updateResult.action());
    } else {
        context.setRequestToExecute(context.getCurrent());
        updateResult = null;
    }

    // 2. 执行实际的 INDEX 或 DELETE 操作
    final IndexShard primary = context.getPrimary();
    final long version = context.getRequestToExecute().version();
    final boolean isDelete = context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE;
    final Engine.Result result;

    if (isDelete) {
        // DELETE 操作
        final DeleteRequest request = context.getRequestToExecute();
        result = primary.applyDeleteOperationOnPrimary(
            version,
            request.id(),
            request.versionType(),
            request.ifSeqNo(),
            request.ifPrimaryTerm()
        );
    } else {
        // INDEX 操作
        final IndexRequest request = context.getRequestToExecute();
        XContentMeteringParserDecorator meteringParserDecorator =
            documentParsingProvider.newMeteringParserDecorator(request);
        final SourceToParse sourceToParse = new SourceToParse(
            request.id(),
            request.source(),
            request.getContentType(),
            request.routing(),
            request.getDynamicTemplates(),
            meteringParserDecorator
        );

        // 调用 IndexShard.applyIndexOperationOnPrimary
        result = primary.applyIndexOperationOnPrimary(
            version,
            request.versionType(),
            sourceToParse,
            request.ifSeqNo(),
            request.ifPrimaryTerm(),
            request.getAutoGeneratedTimestamp(),
            request.isRetry()
        );

        // 3. 检查是否需要更新 mapping
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            return handleMappingUpdateRequired(
                context,
                mappingUpdater,
                waitForMappingUpdate,
                itemDoneListener,
                primary,
                result,
                version,
                updateResult
            );
        }
    }

    onComplete(result, context, updateResult);
    return true;
}
```

**关键点：**
- UPDATE 请求会被转换为 INDEX 或 DELETE 请求
- 调用 `IndexShard.applyIndexOperationOnPrimary` 执行实际的索引操作
- 如果返回 `MAPPING_UPDATE_REQUIRED`，则触发 mapping 更新流程

#### 4. Mapping 更新处理（handleMappingUpdateRequired）

```java
private static boolean handleMappingUpdateRequired(
    BulkPrimaryExecutionContext context,
    MappingUpdatePerformer mappingUpdater,
    ObjLongConsumer<ActionListener<Void>> waitForMappingUpdate,
    ActionListener<Void> itemDoneListener,
    IndexShard primary,
    Engine.Result result,
    long version,
    UpdateHelper.Result updateResult
) {
    final var mapperService = primary.mapperService();
    final long initialMappingVersion = mapperService.mappingVersion();

    try {
        // 1. 预检查：尝试合并 mapping
        CompressedXContent mergedSource = mapperService.merge(
            MapperService.SINGLE_MAPPING_NAME,
            new CompressedXContent(result.getRequiredMappingUpdate()),
            MapperService.MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT
        ).mappingSource();

        // 2. 检查是否真的需要更新（可能已经被其他请求更新了）
        final DocumentMapper existingDocumentMapper = mapperService.documentMapper();
        if (existingDocumentMapper != null
            && mergedSource.equals(existingDocumentMapper.mappingSource())) {
            // Mapping 已经是最新的，直接重试
            context.resetForNoopMappingUpdateRetry(mapperService.mappingVersion());
            return true;
        }
    } catch (Exception e) {
        logger.info(() -> format("%s mapping update rejected by primary", primary.shardId()), e);
        assert result.getId() != null;
        onComplete(exceptionToResult(e, primary, false, version, result.getId()), context, updateResult);
        return true;
    }

    // 3. 发送 mapping 更新请求到 master 节点
    mappingUpdater.updateMappings(
        result.getRequiredMappingUpdate(),
        primary.shardId(),
        new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                // 标记需要等待 mapping 更新
                context.markAsRequiringMappingUpdate();

                // 等待集群状态更新
                waitForMappingUpdate.accept(
                    ActionListener.runAfter(
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Void v) {
                                assert context.requiresWaitingForMappingUpdate();
                                // Mapping 更新完成，重置上下文以重试操作
                                context.resetForMappingUpdateRetry();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                context.failOnMappingUpdate(e);
                            }
                        },
                        () -> itemDoneListener.onResponse(null)
                    ),
                    initialMappingVersion
                );
            }

            @Override
            public void onFailure(Exception e) {
                // Mapping 更新失败
                onComplete(
                    exceptionToResult(e, primary, false, version, result.getId()),
                    context,
                    updateResult
                );
                assert context.isInitial();
                itemDoneListener.onResponse(null);
            }
        }
    );

    // 返回 false 表示需要等待 mapping 更新
    return false;
}
```

**Mapping 更新流程：**
1. **预检查**：尝试在本地合并 mapping，检查是否真的需要更新
2. **发送更新**：调用 `MappingUpdatedAction.updateMappingOnMaster` 发送到 master 节点
3. **等待集群状态**：使用 `ClusterStateObserver` 等待集群状态更新
4. **重试操作**：mapping 更新完成后，重置上下文并重试索引操作

#### 5. IndexShard 层的索引操作

```java
// IndexShard.java
public Engine.IndexResult applyIndexOperationOnPrimary(
    long version,
    VersionType versionType,
    SourceToParse sourceToParse,
    long ifSeqNo,
    long ifPrimaryTerm,
    long autoGeneratedTimestamp,
    boolean isRetry
) throws IOException {
    assert versionType.validateVersionForWrites(version);
    return applyIndexOperation(
        getEngine(),
        UNASSIGNED_SEQ_NO,
        getOperationPrimaryTerm(),
        version,
        versionType,
        ifSeqNo,
        ifPrimaryTerm,
        autoGeneratedTimestamp,
        isRetry,
        Engine.Operation.Origin.PRIMARY,
        sourceToParse
    );
}

private Engine.IndexResult applyIndexOperation(
    Engine engine,
    long seqNo,
    long opPrimaryTerm,
    long version,
    @Nullable VersionType versionType,
    long ifSeqNo,
    long ifPrimaryTerm,
    long autoGeneratedTimeStamp,
    boolean isRetry,
    Engine.Operation.Origin origin,
    SourceToParse sourceToParse
) throws IOException {
    assert opPrimaryTerm <= getOperationPrimaryTerm();
    ensureWriteAllowed(origin);

    Engine.Index operation;
    try {
        // 1. 准备索引操作（解析文档）
        operation = prepareIndex(
            mapperService,
            sourceToParse,
            seqNo,
            opPrimaryTerm,
            version,
            versionType,
            origin,
            autoGeneratedTimeStamp,
            isRetry,
            ifSeqNo,
            ifPrimaryTerm,
            getRelativeTimeInNanos()
        );

        // 2. 检查是否有动态 mapping 更新
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            // 需要更新 mapping，返回特殊结果
            // 注意：这里不执行实际的索引操作，而是返回 MAPPING_UPDATE_REQUIRED
            return new Engine.IndexResult(update, operation.parsedDoc().id());
        }
    } catch (Exception e) {
        verifyNotClosed(e);
        return new Engine.IndexResult(e, version, opPrimaryTerm, seqNo, sourceToParse.id());
    }

    // 3. 执行实际的索引操作
    return index(engine, operation);
}

private Engine.IndexResult index(Engine engine, Engine.Index index) throws IOException {
    active.set(true);
    // 调用 InternalEngine.index
    final Engine.IndexResult result = engine.index(index);
    if (result.getResultType() == Engine.Result.Type.SUCCESS) {
        // 索引成功
    }
    return result;
}
```

**关键点：**
- `prepareIndex` 方法会解析文档并检测动态 mapping 变化
- 如果检测到动态 mapping，返回 `MAPPING_UPDATE_REQUIRED` 而不执行索引
- Mapping 更新完成后，会重新调用此方法执行实际的索引操作

#### 6. InternalEngine 层的索引操作

```java
// InternalEngine.java
@Override
public IndexResult index(Index index) throws IOException {
    final boolean doThrottle = index.origin().isRecovery() == false;
    try (var ignored1 = acquireEnsureOpenRef()) {
        assert assertIncomingSequenceNumber(index.origin(), index.seqNo());
        int reservedDocs = 0;
        try (
            Releasable ignored = versionMap.acquireLock(index.uid());
            Releasable indexThrottle = doThrottle ? throttle.acquireThrottle() : () -> {}
        ) {
            lastWriteNanos = index.startTime();

            // 1. 确定索引策略（addDocument vs updateDocument）
            final IndexingStrategy plan = indexingStrategyForOperation(index);
            reservedDocs = plan.reservedDocs;

            final IndexResult indexResult;
            if (plan.earlyResultOnPreFlightError.isPresent()) {
                // 预检查失败
                indexResult = plan.earlyResultOnPreFlightError.get();
            } else {
                // 2. 生成或注册序列号
                if (index.origin() == Operation.Origin.PRIMARY) {
                    index = new Index(
                        index.uid(),
                        index.parsedDoc(),
                        generateSeqNoForOperationOnPrimary(index),  // 生成 seqNo
                        index.primaryTerm(),
                        index.version(),
                        index.versionType(),
                        index.origin(),
                        index.startTime(),
                        index.getAutoGeneratedIdTimestamp(),
                        index.isRetry(),
                        index.getIfSeqNo(),
                        index.getIfPrimaryTerm()
                    );

                    final boolean toAppend = plan.indexIntoLucene
                        && plan.useLuceneUpdateDocument == false;
                    if (toAppend == false) {
                        advanceMaxSeqNoOfUpdatesOnPrimary(index.seqNo());
                    }
                } else {
                    markSeqNoAsSeen(index.seqNo());
                }

                // 3. 写入 Lucene
                if (plan.indexIntoLucene || plan.addStaleOpToLucene) {
                    indexResult = indexIntoLucene(index, plan);
                } else {
                    indexResult = new IndexResult(
                        plan.versionForIndexing,
                        index.primaryTerm(),
                        index.seqNo(),
                        plan.currentNotFoundOrDeleted,
                        index.id()
                    );
                }
            }

            // 4. 写入 Translog
            if (index.origin().isFromTranslog() == false) {
                final Translog.Location location;
                if (indexResult.getResultType() == Result.Type.SUCCESS) {
                    location = translog.add(new Translog.Index(index, indexResult));
                } else if (indexResult.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    // 失败但有 seqNo，记录为 no-op
                    final NoOp noOp = new NoOp(
                        indexResult.getSeqNo(),
                        index.primaryTerm(),
                        index.origin(),
                        index.startTime(),
                        indexResult.getFailure().toString()
                    );
                    location = innerNoOp(noOp).getTranslogLocation();
                } else {
                    location = null;
                }
                indexResult.setTranslogLocation(location);
            }

            // 5. 更新 VersionMap
            if (plan.indexIntoLucene && indexResult.getResultType() == Result.Type.SUCCESS) {
                final Translog.Location translogLocation =
                    trackTranslogLocation.get() ? indexResult.getTranslogLocation() : null;
                versionMap.maybePutIndexUnderLock(
                    index.uid(),
                    new IndexVersionValue(
                        translogLocation,
                        plan.versionForIndexing,
                        index.seqNo(),
                        index.primaryTerm()
                    )
                );
            }

            // 6. 更新本地检查点
            localCheckpointTracker.markSeqNoAsProcessed(indexResult.getSeqNo());
            if (indexResult.getTranslogLocation() == null) {
                localCheckpointTracker.markSeqNoAsPersisted(indexResult.getSeqNo());
            }

            indexResult.setTook(relativeTimeInNanosSupplier.getAsLong() - index.startTime());
            indexResult.freeze();
            return indexResult;
        } finally {
            releaseInFlightDocs(reservedDocs);
        }
    } catch (RuntimeException | IOException e) {
        try {
            if (e instanceof AlreadyClosedException == false
                && treatDocumentFailureAsTragicError(index)) {
                failEngine("index id[" + index.id() + "]", e);
            } else {
                maybeFailEngine("index id[" + index.id() + "]", e);
            }
        } catch (Exception inner) {
            e.addSuppressed(inner);
        }
        throw e;
    }
}
```

**InternalEngine 索引流程：**
1. **获取锁**：对文档 UID 加锁，防止并发修改
2. **确定策略**：决定使用 `addDocument`（新文档）还是 `updateDocument`（更新）
3. **生成序列号**：主分片生成 seqNo，副本使用主分片的 seqNo
4. **写入 Lucene**：调用 `indexIntoLucene` 写入 Lucene 索引
5. **写入 Translog**：将操作记录到 Translog（预写日志）
6. **更新 VersionMap**：在内存中记录文档版本信息
7. **更新检查点**：更新本地检查点追踪器

#### 7. Mapping 更新的集群状态发布

```java
// MappingUpdatedAction.java
public void updateMappingOnMaster(Index index, Mapping mappingUpdate, ActionListener<Void> listener) {
    final RunOnce release = new RunOnce(semaphore::release);
    try {
        // 1. 获取信号量（限制并发更新数量）
        semaphore.acquire();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        listener.onFailure(e);
        return;
    }

    boolean successFullySent = false;
    try {
        sendUpdateMapping(index, mappingUpdate, ActionListener.runBefore(listener, release::run));
        successFullySent = true;
    } finally {
        if (successFullySent == false) {
            release.run();
        }
    }
}

protected void sendUpdateMapping(Index index, Mapping mappingUpdate, ActionListener<Void> listener) {
    // 2. 构造 PutMappingRequest
    PutMappingRequest putMappingRequest = new PutMappingRequest();
    putMappingRequest.setConcreteIndex(index);
    putMappingRequest.source(mappingUpdate.toString(), XContentType.JSON);
    putMappingRequest.masterNodeTimeout(dynamicMappingUpdateTimeout);
    putMappingRequest.ackTimeout(TimeValue.ZERO);  // 不等待所有节点确认

    // 3. 发送到 master 节点
    client.execute(
        TransportAutoPutMappingAction.TYPE,
        putMappingRequest,
        listener.delegateFailureAndWrap((l, r) -> l.onResponse(null))
    );
}
```

**Mapping 更新流程：**
1. **限流控制**：使用信号量限制并发 mapping 更新数量（默认 10）
2. **发送到 Master**：通过 `TransportAutoPutMappingAction` 发送到 master 节点
3. **集群状态更新**：Master 节点更新集群状态并发布到所有节点
4. **等待应用**：数据节点通过 `ClusterStateObserver` 等待新的集群状态
5. **重试索引**：收到新的集群状态后，重试之前失败的索引操作

**配置参数：**
- `indices.mapping.dynamic_timeout`：动态 mapping 更新超时时间（默认 30s）
- `indices.mapping.max_in_flight_updates`：最大并发更新数（默认 10）

### 第二部分：副本复制机制

#### 1. 完整写入流程概览

根据源码分析，Elasticsearch 的完整写入流程如下：

```
客户端写入请求
  -> TransportBulkAction (协调节点)
  -> executeBulkRequestsByShard() (按分片分组)
  -> executeBulkShardRequest()
  -> client.executeLocally(TransportShardBulkAction.TYPE, bulkShardRequest, listener)

// === 主分片写入阶段 ===
  -> TransportReplicationAction.doExecute()
  -> ReroutePhase.doRun()
  -> performLocalAction()
  -> RPC: transportPrimaryAction
  -> TransportReplicationAction.handlePrimaryRequest()
  -> AsyncPrimaryAction.doRun()
  -> acquirePrimaryOperationPermit(..., onAcquired)
  -> runWithPrimaryShardReference()
  -> ReplicationOperation.execute()
      -> checkActiveShardCount()  // 检查活跃分片数
      -> primary.perform(request, handlePrimaryResult)
          -> TransportReplicationAction.perform()
          -> TransportWriteAction.shardOperationOnPrimary()
          -> TransportShardBulkAction.dispatchedShardOperationOnPrimary()
          -> performOnPrimary()
              -> executeBulkItemRequest() (循环处理每个操作)
                  -> IndexShard.applyIndexOperationOnPrimary()
                  -> applyIndexOperation()
                      -> prepareIndex() // 解析文档，检测动态 mapping
                      -> 如果有动态 mapping 更新:
                          -> 返回 MAPPING_UPDATE_REQUIRED
                          -> handleMappingUpdateRequired()
                          -> MappingUpdatedAction.updateMappingOnMaster()
                          -> 发送 PutMappingRequest 到 master
                          -> Master 更新集群状态并发布
                          -> ClusterStateObserver.waitForNextChange()
                          -> 收到新集群状态后重试索引操作
                      -> index(engine, operation)
                          -> InternalEngine.index()
                              -> 获取文档锁
                              -> 确定索引策略
                              -> 生成序列号 (seqNo)
                              -> indexIntoLucene() // 写入 Lucene
                              -> translog.add() // 写入 Translog
                              -> versionMap.put() // 更新版本映射
                              -> localCheckpointTracker.markSeqNoAsProcessed()
                              -> 返回 IndexResult

      // === 副本复制阶段 ===
      -> handlePrimaryResult() // 主分片成功回调
          -> replicasProxy.onPrimaryOperationComplete()
          -> markUnavailableShardsAsStale() // 标记不可用分片
          -> performOnReplicas() // 向所有副本发送请求
              -> performOnReplica() (for each replica)
                  -> pendingActions.incrementAndGet()
                  -> replicasProxy.performOn()
                  -> RPC: transportReplicaAction
                  -> TransportReplicationAction.handleReplicaRequest()
                  -> AsyncReplicaAction.doRun()
                  -> acquireReplicaOperationPermit()
                  -> shardOperationOnReplica()
                  -> TransportShardBulkAction.dispatchedShardOperationOnReplica()
                  -> performOnReplica()
                      -> IndexShard.applyIndexOperationOnReplica()
                      -> InternalEngine.index() // 副本写入
                  -> onResponse/onFailure
                  -> decPendingAndFinishIfNeeded()
          -> primaryResult.runPostReplicationActions()
              -> AsyncAfterWriteAction.run()
                  -> indexShard.afterWriteOperation()
                  -> 可选: refresh (根据 RefreshPolicy)
                  -> 可选: fsync (根据 Durability)
              -> decPendingAndFinishIfNeeded()

      -> finish() // 所有操作完成，响应客户端
          -> resultListener.onResponse(primaryResult)
```

**关键阶段说明：**

1. **协调阶段**：协调节点接收请求，按分片分组
2. **路由阶段**：找到主分片所在节点，发送请求
3. **主分片写入**：
   - 解析文档，检测动态 mapping
   - 如需更新 mapping，发送到 master 并等待集群状态更新
   - 写入 Lucene 和 Translog
   - 生成序列号和版本号
4. **副本复制**：
   - 并行向所有 in-sync 副本发送请求
   - 副本使用主分片的序列号写入
   - 等待所有副本操作完成（成功或失败）
5. **后置操作**：
   - 根据配置执行 refresh
   - 根据配置执行 fsync
6. **响应客户端**：返回操作结果

### 2. 主分片执行逻辑

在 `ReplicationOperation.execute()` 方法中：

```java
public void execute() throws Exception {
    final String activeShardCountFailure = checkActiveShardCount();
    final ShardRouting primaryRouting = primary.routingEntry();
    final ShardId primaryId = primaryRouting.shardId();
    if (activeShardCountFailure != null) {
        finishAsFailed(new UnavailableShardsException(...));
        return;
    }

    totalShards.incrementAndGet();
    pendingActions.incrementAndGet(); // 增加待处理操作计数
    // 执行主分片操作，成功后回调 handlePrimaryResult
    primary.perform(request, ActionListener.wrap(this::handlePrimaryResult, this::finishAsFailed));
}
```

**关键点：**
- 在执行主分片操作前，会先检查 `activeShardCount`（活跃分片数量）
- 使用 `pendingActions` 计数器追踪所有待完成的操作

### 3. 活跃分片检查（waitForActiveShards）

在主分片执行前，会检查是否有足够的活跃分片：

```java
protected String checkActiveShardCount() {
    final ShardId shardId = primary.routingEntry().shardId();
    final ActiveShardCount waitForActiveShards = request.waitForActiveShards();
    if (waitForActiveShards == ActiveShardCount.NONE) {
        return null;  // 不等待任何分片
    }
    final IndexShardRoutingTable shardRoutingTable = primary.getReplicationGroup().getRoutingTable();
    ActiveShardCount.EnoughShards enoughShardsActive = waitForActiveShards.enoughShardsActive(shardRoutingTable);
    if (enoughShardsActive.enoughShards()) {
        return null;
    } else {
        // 活跃分片不足，返回错误信息
        return "Not enough active copies to meet shard count of [" + waitForActiveShards + "]...";
    }
}
```

**`waitForActiveShards` 配置选项：**
- `NONE` (0)：不等待任何分片，主分片写入即可
- `ONE` (1)：默认值，只需主分片活跃即可（**注意：这是写入前的检查，不是写入后的等待**）
- `ALL`：需要所有分片（主分片 + 所有副本分片）都活跃
- 具体数字（如 2, 3）：需要指定数量的分片活跃

**重要理解：**
`waitForActiveShards` 是在**写入前**的检查，确保有足够的分片可用，而不是写入后等待副本完成。

### 4. 主分片成功后的处理（handlePrimaryResult）

主分片写入成功后，会调用 `handlePrimaryResult` 方法：

```java
private void handlePrimaryResult(final PrimaryResultT primaryResult) {
    this.primaryResult = primaryResult;
    final ReplicaRequest replicaRequest = primaryResult.replicaRequest();
    if (replicaRequest != null) {
        final ReplicationGroup replicationGroup = primary.getReplicationGroup();

        pendingActions.incrementAndGet();
        // 处理 unpromotable 副本（如搜索副本）
        replicasProxy.onPrimaryOperationComplete(
            replicaRequest,
            replicationGroup.getRoutingTable(),
            ActionListener.wrap(ignored -> decPendingAndFinishIfNeeded(), exception -> {
                totalShards.incrementAndGet();
                shardReplicaFailures.add(...);
                decPendingAndFinishIfNeeded();
            })
        );

        // 获取全局检查点和最大序列号
        final long globalCheckpoint = primary.computedGlobalCheckpoint();
        final long maxSeqNoOfUpdatesOrDeletes = primary.maxSeqNoOfUpdatesOrDeletes();

        // 标记不可用的 in-sync 分片为过时
        markUnavailableShardsAsStale(replicaRequest, replicationGroup);

        // 向所有副本分片发送请求
        performOnReplicas(replicaRequest, globalCheckpoint, maxSeqNoOfUpdatesOrDeletes,
                         replicationGroup, pendingReplicationActions);
    }

    // 执行后置操作（如刷新检查点）
    primaryResult.runPostReplicationActions(new ActionListener<>() {
        @Override
        public void onResponse(Void aVoid) {
            successfulShards.incrementAndGet();
            updateCheckPoints(..., () -> decPendingAndFinishIfNeeded());
        }

        @Override
        public void onFailure(Exception e) {
            updateCheckPoints(..., () -> finishAsFailed(e));
        }
    });
}
```

**关键流程：**
1. 主分片成功后，立即开始向副本分片发送请求
2. 使用 `pendingActions` 计数器追踪所有待完成的操作（包括主分片和所有副本分片）
3. 标记不可用的 in-sync 分片为过时（stale）
4. 异步向所有副本分片发送请求

### 5. 副本分片执行（performOnReplicas）

```java
private void performOnReplicas(
    final ReplicaRequest replicaRequest,
    final long globalCheckpoint,
    final long maxSeqNoOfUpdatesOrDeletes,
    final ReplicationGroup replicationGroup,
    final PendingReplicationActions pendingReplicationActions
) {
    // 统计跳过的分片（未分配或正在初始化的分片）
    totalShards.addAndGet(replicationGroup.getSkippedShards().size());

    final ShardRouting primaryRouting = primary.routingEntry();

    // 遍历所有复制目标（副本分片）
    for (final ShardRouting shard : replicationGroup.getReplicationTargets()) {
        if (shard.isSameAllocation(primaryRouting) == false) {
            performOnReplica(shard, replicaRequest, globalCheckpoint,
                           maxSeqNoOfUpdatesOrDeletes, pendingReplicationActions);
        }
    }
}
```

### 6. 单个副本分片的处理（performOnReplica）

```java
private void performOnReplica(
    final ShardRouting shard,
    final ReplicaRequest replicaRequest,
    final long globalCheckpoint,
    final long maxSeqNoOfUpdatesOrDeletes,
    final PendingReplicationActions pendingReplicationActions
) {
    totalShards.incrementAndGet();
    pendingActions.incrementAndGet();  // 每个副本操作增加计数

    final ActionListener<ReplicaResponse> replicationListener = new ActionListener<>() {
        @Override
        public void onResponse(ReplicaResponse response) {
            successfulShards.incrementAndGet();
            updateCheckPoints(shard, response::localCheckpoint, response::globalCheckpoint,
                            () -> decPendingAndFinishIfNeeded());
        }

        @Override
        public void onFailure(Exception replicaException) {
            // 记录失败，但不阻止整体操作完成
            shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(...));
            replicasProxy.failShardIfNeeded(shard, primaryTerm, message, replicaException,
                ActionListener.wrap(r -> decPendingAndFinishIfNeeded(),
                                  ReplicationOperation.this::onNoLongerPrimary));
        }
    };

    // 创建可重试的副本操作
    final RetryableAction<ReplicaResponse> replicationAction = new RetryableAction<>(...) {
        @Override
        public void tryAction(ActionListener<ReplicaResponse> listener) {
            replicasProxy.performOn(shard, replicaRequest, primaryTerm,
                                   globalCheckpoint, maxSeqNoOfUpdatesOrDeletes, listener);
        }

        @Override
        public void onFinished() {
            super.onFinished();
            pendingReplicationActions.removeReplicationAction(allocationId, this);
        }

        @Override
        public boolean shouldRetry(Exception e) {
            final Throwable cause = ExceptionsHelper.unwrapCause(e);
            return cause instanceof CircuitBreakingException
                || cause instanceof EsRejectedExecutionException
                || cause instanceof ConnectTransportException;
        }
    };

    pendingReplicationActions.addPendingAction(allocationId, replicationAction);
    replicationAction.run();  // 异步执行
}
```

**关键点：**
- 每个副本操作都会增加 `pendingActions` 计数
- 副本操作是**异步**执行的
- 副本失败不会导致整体操作失败，只会记录失败信息
- 支持重试机制（针对特定异常）

### 7. 完成条件判断（decPendingAndFinishIfNeeded）

```java
private void decPendingAndFinishIfNeeded() {
    assert pendingActions.get() > 0 : "pending action count goes below 0 for request [" + request + "]";
    if (pendingActions.decrementAndGet() == 0) {
        finish();
    }
}

private void finish() {
    if (finished.compareAndSet(false, true)) {
        primaryResult.setShardInfo(
            ReplicationResponse.ShardInfo.of(
                totalShards.get(),
                successfulShards.get(),
                shardReplicaFailures.toArray(ReplicationResponse.NO_FAILURES)
            )
        );
        resultListener.onResponse(primaryResult);  // 响应客户端
    }
}
```

**关键逻辑：**
- 使用 `pendingActions` 原子计数器追踪所有待完成的操作
- 当 `pendingActions` 减到 0 时，表示所有操作（主分片 + 所有副本分片）都已完成
- 此时才会调用 `finish()` 响应客户端
- 响应中包含成功的分片数和失败的分片信息

### 8. In-Sync Allocation IDs 机制

Elasticsearch 使用 **In-Sync Allocation IDs** 来追踪哪些副本分片与主分片保持同步：

```java
public class ReplicationGroup {
    private final IndexShardRoutingTable routingTable;
    private final Set<String> inSyncAllocationIds;  // 同步的分片 ID 集合
    private final Set<String> trackedAllocationIds;  // 追踪的分片 ID 集合

    // 不可用的 in-sync 分片（在 in-sync 集合中但路由表中不存在）
    private final Set<String> unavailableInSyncShards;

    // 复制目标（需要接收写入请求的副本分片）
    private final List<ShardRouting> replicationTargets;

    // 跳过的分片（未分配或正在初始化）
    private final List<ShardRouting> skippedShards;
}
```

**In-Sync 机制说明：**
- `inSyncAllocationIds`：与主分片保持同步的副本分片集合
- 只有 in-sync 的副本分片才会接收写入请求
- 如果 in-sync 分片不可用，会被标记为 stale（过时）
- 这确保了数据的一致性，避免向过时的副本发送请求

### 9. 副本分片的实际执行

副本分片通过 RPC 调用执行：

```java
// ReplicasProxy.performOn()
public void performOn(
    final ShardRouting replica,
    final ReplicaRequest request,
    final long primaryTerm,
    final long globalCheckpoint,
    final long maxSeqNoOfUpdatesOrDeletes,
    final ActionListener<ReplicaResponse> listener
) {
    String nodeId = replica.currentNodeId();
    final DiscoveryNode node = clusterService.state().nodes().get(nodeId);
    if (node == null) {
        listener.onFailure(new NoNodeAvailableException("unknown node [" + nodeId + "]"));
        return;
    }
    final ConcreteReplicaRequest<ReplicaRequest> replicaRequest = new ConcreteReplicaRequest<>(
        request, replica.allocationId().getId(), primaryTerm,
        globalCheckpoint, maxSeqNoOfUpdatesOrDeletes
    );
    // 通过 Transport 层发送 RPC 请求
    transportService.sendRequest(node, transportReplicaAction, replicaRequest,
                                transportOptions, handler);
}
```

副本节点接收到请求后，通过 `handleReplicaRequest` 处理：

```java
protected void handleReplicaRequest(
    final ConcreteReplicaRequest<ReplicaRequest> replicaRequest,
    final TransportChannel channel,
    final Task task
) {
    Releasable releasable = checkReplicaLimits(replicaRequest.getRequest());
    ActionListener<ReplicaResponse> listener = ActionListener.runBefore(
        new ChannelActionListener<>(channel), releasable::close
    );

    try {
        new AsyncReplicaAction(replicaRequest, listener, (ReplicationTask) task).run();
    } catch (RuntimeException e) {
        listener.onFailure(e);
    }
}
```

## 核心问题解答

### 1. 主分片如何执行写入操作？

**答案：主分片写入经历多个层次，从 TransportShardBulkAction 到 InternalEngine。**

**详细流程：**

1. **TransportShardBulkAction 层**：
   - 接收批量写入请求
   - 循环处理每个操作项（index/delete/update）
   - UPDATE 请求转换为 INDEX 或 DELETE

2. **IndexShard 层**：
   - 调用 `applyIndexOperationOnPrimary`
   - 调用 `prepareIndex` 解析文档
   - 检测动态 mapping 变化
   - 如有 mapping 更新，返回 `MAPPING_UPDATE_REQUIRED`

3. **InternalEngine 层**：
   - 获取文档锁（基于文档 UID）
   - 确定索引策略（addDocument vs updateDocument）
   - 生成序列号（seqNo）
   - 写入 Lucene 索引
   - 写入 Translog（预写日志）
   - 更新 VersionMap（内存版本映射）
   - 更新本地检查点

**关键数据结构：**
- **SeqNo（序列号）**：全局递增，用于排序和恢复
- **PrimaryTerm**：主分片任期号，用于检测主分片切换
- **Version**：文档版本号，用于乐观并发控制
- **Translog**：预写日志，用于故障恢复
- **VersionMap**：内存中的文档版本映射，加速版本检查

### 2. 动态 Mapping 更新如何触发集群状态变更？

**答案：通过 MappingUpdatedAction 发送到 master 节点，master 更新集群状态并发布到所有节点。**

**详细流程：**

1. **检测动态 Mapping**：
   ```java
   // IndexShard.applyIndexOperation()
   operation = prepareIndex(mapperService, sourceToParse, ...);
   Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
   if (update != null) {
       // 需要更新 mapping
       return new Engine.IndexResult(update, operation.parsedDoc().id());
   }
   ```

2. **预检查 Mapping**：
   ```java
   // 尝试在本地合并 mapping
   CompressedXContent mergedSource = mapperService.merge(
       MapperService.SINGLE_MAPPING_NAME,
       new CompressedXContent(result.getRequiredMappingUpdate()),
       MapperService.MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT
   ).mappingSource();

   // 检查是否真的需要更新（可能已被其他请求更新）
   if (existingDocumentMapper != null
       && mergedSource.equals(existingDocumentMapper.mappingSource())) {
       // 已经是最新的，直接重试
       context.resetForNoopMappingUpdateRetry(mapperService.mappingVersion());
       return true;
   }
   ```

3. **发送到 Master 节点**：
   ```java
   // MappingUpdatedAction.updateMappingOnMaster()
   semaphore.acquire();  // 限流：默认最多 10 个并发更新

   PutMappingRequest putMappingRequest = new PutMappingRequest();
   putMappingRequest.setConcreteIndex(index);
   putMappingRequest.source(mappingUpdate.toString(), XContentType.JSON);
   putMappingRequest.masterNodeTimeout(dynamicMappingUpdateTimeout);  // 默认 30s

   client.execute(TransportAutoPutMappingAction.TYPE, putMappingRequest, listener);
   ```

4. **Master 节点处理**：
   - 接收 PutMappingRequest
   - 验证 mapping 变更
   - 更新集群元数据（IndexMetadata）
   - 增加 mapping 版本号
   - 发布新的集群状态到所有节点

5. **等待集群状态更新**：
   ```java
   // 使用 ClusterStateObserver 监听集群状态变化
   observer.waitForNextChange(new ClusterStateObserver.Listener() {
       @Override
       public void onNewClusterState(ClusterState state) {
           // 收到新的集群状态
           var indexMetadata = state.metadata().index(primary.shardId().getIndex());
           if (indexMetadata != null
               && indexMetadata.getMappingVersion() != initialMappingVersion) {
               // Mapping 版本已更新
               mappingUpdateListener.onResponse(null);
           }
       }

       @Override
       public void onTimeout(TimeValue timeout) {
           mappingUpdateListener.onFailure(
               new MapperException("timed out while waiting for a dynamic mapping update")
           );
       }
   });
   ```

6. **重试索引操作**：
   ```java
   // Mapping 更新完成后
   context.resetForMappingUpdateRetry();
   // 重新执行 executeBulkItemRequest，这次会使用新的 mapping
   ```

**关键配置：**
- `indices.mapping.dynamic_timeout`：动态 mapping 更新超时（默认 30s）
- `indices.mapping.max_in_flight_updates`：最大并发更新数（默认 10）

**优化机制：**
- **预检查**：在发送到 master 前，先在本地检查是否真的需要更新
- **限流**：使用信号量限制并发更新数量，防止 master 过载
- **批量合并**：多个字段的更新会合并为一次 mapping 更新
- **版本检查**：通过 mapping 版本号避免重复更新

### 3. 主分片成功后是否需要等待副本分片？

**答案：是的，需要等待所有副本分片操作完成（无论成功或失败）。**

**详细说明：**
- 主分片成功后，会向所有 in-sync 的副本分片发送写入请求
- 使用 `pendingActions` 计数器追踪所有操作（主分片 + 所有副本分片）
- 只有当 `pendingActions` 减到 0（所有操作完成）时，才会响应客户端
- 副本分片的失败不会导致整体操作失败，但会记录在响应中

**代码证据：**
```java
// 主分片操作
pendingActions.incrementAndGet(); // +1

// 每个副本操作
pendingActions.incrementAndGet(); // +1

// 每个操作完成时
decPendingAndFinishIfNeeded() {
    if (pendingActions.decrementAndGet() == 0) {
        finish(); // 响应客户端
    }
}
```

### 2. 是否有多数派阈值（Quorum）机制？

**答案：没有传统意义上的多数派（Quorum）机制，但有 `waitForActiveShards` 配置。**

**详细说明：**

Elasticsearch **不使用多数派（Quorum）机制**，而是采用以下策略：

1. **写入前检查（waitForActiveShards）：**
   - 在主分片执行前，检查是否有足够的活跃分片
   - 默认值是 `ONE`（只需主分片活跃）
   - 可以配置为 `ALL` 或具体数字

2. **写入后等待：**
   - 等待**所有** in-sync 副本分片完成（无论成功或失败）
   - 不是等待多数派，而是等待所有副本

3. **In-Sync Allocation IDs：**
   - 只向 in-sync 的副本分片发送请求
   - 不可用的 in-sync 分片会被标记为 stale
   - 这确保了一致性

**与传统 Quorum 的区别：**
- **传统 Quorum**：写入成功需要多数派（如 3 个副本中的 2 个）确认
- **Elasticsearch**：写入成功只需主分片成功，但会等待所有副本操作完成（记录失败信息）

### 3. 何时响应客户端？

**答案：当所有操作（主分片 + 所有副本分片）完成时响应客户端。**

**响应时机：**
1. 主分片写入成功
2. 所有 in-sync 副本分片的操作完成（成功或失败）
3. `pendingActions` 计数器减到 0
4. 调用 `finish()` 方法响应客户端

**响应内容：**
```java
primaryResult.setShardInfo(
    ReplicationResponse.ShardInfo.of(
        totalShards.get(),        // 总分片数
        successfulShards.get(),   // 成功的分片数
        shardReplicaFailures.toArray(...)  // 失败的分片信息
    )
);
resultListener.onResponse(primaryResult);
```

客户端会收到：
- 总分片数
- 成功的分片数
- 失败的分片详细信息

### 4. 副本失败的影响

**副本失败不会导致写入失败，但会：**
1. 记录在响应的 `failures` 字段中
2. 尝试将失败的副本标记为 stale
3. 触发分片恢复机制
4. 客户端可以根据 `successfulShards` 判断写入的可靠性

## 一致性保证

### 1. 写入一致性

Elasticsearch 的写入一致性保证：

1. **主分片成功是必要条件：**
   - 主分片写入失败，整个操作失败
   - 主分片写入成功，操作被认为是成功的（即使副本失败）

2. **In-Sync 机制：**
   - 只向 in-sync 的副本发送请求
   - 确保副本数据与主分片一致
   - 不可用的副本会被标记为 stale，需要重新同步

3. **全局检查点（Global Checkpoint）：**
   - 追踪所有 in-sync 副本都已确认的最大序列号
   - 用于数据恢复和一致性保证

### 2. 读取一致性

- **默认情况**：读取可能命中主分片或任意副本分片
- **强一致性读取**：可以通过 `preference=_primary` 强制从主分片读取
- **最终一致性**：副本分片最终会与主分片保持一致

### 3. 故障恢复

当副本分片失败或节点宕机时：
1. 副本被标记为 stale
2. 从 in-sync 集合中移除
3. 触发分片恢复流程
4. 恢复完成后重新加入 in-sync 集合

## 配置参数

### 1. waitForActiveShards

**作用：** 写入前检查活跃分片数量

**配置级别：**
- 索引级别：`index.write.wait_for_active_shards`
- 请求级别：`wait_for_active_shards` 参数

**可选值：**
- `0` 或 `NONE`：不等待，主分片可写即可
- `1` 或 `ONE`：默认值，只需主分片活跃
- `all` 或 `ALL`：需要所有分片（主 + 所有副本）活跃
- 具体数字（如 `2`, `3`）：需要指定数量的分片活跃

**示例：**
```json
PUT /my_index/_doc/1?wait_for_active_shards=2
{
  "field": "value"
}
```

### 2. number_of_replicas

**作用：** 设置副本分片数量

**配置：**
```json
PUT /my_index/_settings
{
  "index": {
    "number_of_replicas": 2
  }
}
```

## 性能考虑

### 1. 副本数量的影响

- **更多副本：**
  - 优点：更高的读取吞吐量、更好的容错性
  - 缺点：更高的写入延迟、更多的存储空间

- **更少副本：**
  - 优点：更低的写入延迟、更少的存储空间
  - 缺点：更低的读取吞吐量、更差的容错性

### 2. waitForActiveShards 的影响

- **`NONE` 或 `ONE`（默认）：**
  - 最快的写入速度
  - 可能在副本未就绪时写入

- **`ALL`：**
  - 最慢的写入速度
  - 确保所有副本都可用才写入
  - 适合对数据可靠性要求极高的场景

### 3. 异步复制的优势

- 主分片和副本分片的写入是**并行**的（异步）
- 不需要等待副本写入完成才开始下一个副本
- 充分利用网络和磁盘 I/O

## 总结

### 关键要点

1. **等待策略：**
   - Elasticsearch **会等待所有副本分片操作完成**（无论成功或失败）
   - 使用 `pendingActions` 计数器追踪所有操作
   - 所有操作完成后才响应客户端

2. **没有多数派机制：**
   - 不使用传统的 Quorum（多数派）机制
   - 主分片成功即认为写入成功
   - 副本失败会记录但不影响整体成功

3. **一致性保证：**
   - 通过 In-Sync Allocation IDs 机制保证一致性
   - 只向 in-sync 的副本发送请求
   - 全局检查点追踪已确认的操作

4. **配置灵活性：**
   - `waitForActiveShards` 控制写入前的检查
   - 可以根据业务需求在性能和可靠性之间权衡

### 与其他分布式系统的对比

| 特性 | Elasticsearch | Raft/Paxos | Kafka |
|------|---------------|------------|-------|
| 一致性模型 | 主副本模型 | 多数派（Quorum） | ISR（In-Sync Replicas） |
| 写入确认 | 主分片成功 | 多数派确认 | ISR 确认 |
| 副本失败影响 | 记录但不阻止 | 阻止写入（如果无法达到多数派） | 记录但不阻止（如果 ISR > min.insync.replicas） |
| 读取一致性 | 最终一致性（可配置） | 强一致性 | 最终一致性 |

### 最佳实践

1. **生产环境建议：**
   - 至少配置 1 个副本（`number_of_replicas: 1`）
   - 使用默认的 `waitForActiveShards: 1`
   - 监控副本健康状态

2. **高可用场景：**
   - 配置 2 个或更多副本
   - 考虑使用 `waitForActiveShards: 2` 或更高
   - 跨可用区部署

3. **性能优化场景：**
   - 减少副本数量
   - 使用 `waitForActiveShards: 1`（默认）
   - 批量写入

4. **监控指标：**
   - 监控 `successfulShards` 和 `failedShards`
   - 关注副本同步延迟
   - 监控 in-sync 副本数量

## 参考代码位置

- `ReplicationOperation.java`：核心复制逻辑
- `TransportReplicationAction.java`：传输层复制操作
- `ActiveShardCount.java`：活跃分片计数
- `ReplicationGroup.java`：复制组和 in-sync 管理
- `IndexShard.java`：分片级别操作

## 附录：完整调用链

### 主分片写入 + 副本复制完整流程

```
客户端写入请求
  -> TransportBulkAction (协调节点)
  -> executeBulkRequestsByShard() (按分片分组)
  -> executeBulkShardRequest()
  -> client.executeLocally(TransportShardBulkAction.TYPE, bulkShardRequest, listener)

// ========== 第一阶段：路由到主分片 ==========
  -> TransportReplicationAction.doExecute()
  -> ReroutePhase.doRun()
      -> 查找主分片所在节点
      -> performLocalAction() 或 performRemoteAction()
  -> transportService.sendRequest(transportPrimaryAction, ...)
  -> TransportReplicationAction.handlePrimaryRequest()

// ========== 第二阶段：主分片写入 ==========
  -> AsyncPrimaryAction.doRun()
  -> acquirePrimaryOperationPermit(..., onAcquired)
      -> 获取主分片操作许可（IndexShard.acquirePrimaryOperationPermit）
      -> 检查分片状态、全局检查点等
  -> runWithPrimaryShardReference()
  -> ReplicationOperation.execute()
      -> checkActiveShardCount()  // 检查活跃分片数（waitForActiveShards）
      -> pendingActions.incrementAndGet()  // 主分片操作计数 +1
      -> primary.perform(request, ActionListener.wrap(this::handlePrimaryResult, this::finishAsFailed))
          -> TransportReplicationAction.perform()
          -> TransportWriteAction.shardOperationOnPrimary()
              -> executor.execute() // 切换到 WRITE 线程池
          -> TransportShardBulkAction.dispatchedShardOperationOnPrimary()
              -> ClusterStateObserver observer = new ClusterStateObserver(...)
              -> performOnPrimary(
                  request,
                  primary,
                  updateHelper,
                  nowInMillisSupplier,
                  // Mapping 更新回调
                  (update, shardId, mappingListener) -> {
                      mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
                  },
                  // 等待 Mapping 更新回调
                  (mappingUpdateListener, initialMappingVersion) -> {
                      observer.waitForNextChange(new ClusterStateObserver.Listener() {
                          @Override
                          public void onNewClusterState(ClusterState state) {
                              // 检查 mapping 版本是否更新
                              if (indexMetadata.getMappingVersion() != initialMappingVersion) {
                                  mappingUpdateListener.onResponse(null);
                              }
                          }
                          @Override
                          public void onTimeout(TimeValue timeout) {
                              mappingUpdateListener.onFailure(new MapperException("..."));
                          }
                      });
                  },
                  listener,
                  executor,
                  postWriteRefresh,
                  postWriteAction
              )

              // ===== 批量操作循环 =====
              -> new ActionRunnable<>(listener) {
                  BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);

                  @Override
                  protected void doRun() throws Exception {
                      while (context.hasMoreOperationsToExecute()) {
                          if (executeBulkItemRequest(...) == false) {
                              // 需要等待 mapping 更新，暂停处理
                              return;
                          }
                      }
                      // 所有操作完成
                      finishRequest();
                  }
              }.run()

              // ===== 单个操作处理 =====
              -> executeBulkItemRequest(
                  context,
                  updateHelper,
                  nowInMillisSupplier,
                  mappingUpdater,
                  waitForMappingUpdate,
                  onMappingUpdateDone
              )
                  // 1. 处理 UPDATE 请求
                  -> if (opType == UPDATE) {
                      updateResult = updateHelper.prepare(updateRequest, primary, nowInMillisSupplier);
                      if (updateResult.getResponseResult() == NOOP) {
                          // NOOP 操作，直接返回
                          context.markOperationAsNoOp();
                          return true;
                      }
                      context.setRequestToExecute(updateResult.action());  // 转换为 INDEX 或 DELETE
                  }

                  // 2. 执行 INDEX 或 DELETE
                  -> if (isDelete) {
                      result = primary.applyDeleteOperationOnPrimary(...);
                  } else {
                      // INDEX 操作
                      SourceToParse sourceToParse = new SourceToParse(...);
                      result = primary.applyIndexOperationOnPrimary(
                          version,
                          versionType,
                          sourceToParse,
                          ifSeqNo,
                          ifPrimaryTerm,
                          autoGeneratedTimestamp,
                          isRetry
                      );

                      // ===== IndexShard 层 =====
                      -> IndexShard.applyIndexOperationOnPrimary()
                      -> applyIndexOperation(
                          engine,
                          UNASSIGNED_SEQ_NO,
                          primaryTerm,
                          version,
                          versionType,
                          ...,
                          Origin.PRIMARY,
                          sourceToParse
                      )
                          -> ensureWriteAllowed(origin);  // 检查分片是否可写

                          // 准备索引操作（解析文档）
                          -> operation = prepareIndex(
                              mapperService,
                              sourceToParse,
                              seqNo,
                              opPrimaryTerm,
                              version,
                              versionType,
                              origin,
                              autoGeneratedTimeStamp,
                              isRetry,
                              ifSeqNo,
                              ifPrimaryTerm
                          )
                              -> DocumentMapper documentMapper = mapperService.documentMapper();
                              -> ParsedDocument parsedDoc = documentMapper.parse(sourceToParse);
                              -> 检测动态 mapping 变化
                              -> return new Engine.Index(..., parsedDoc, ...);

                          // 检查是否有动态 mapping 更新
                          -> Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
                          -> if (update != null) {
                              // 需要更新 mapping
                              return new Engine.IndexResult(update, operation.parsedDoc().id());
                          }

                          // 执行实际的索引操作
                          -> return index(engine, operation);
                              -> active.set(true);
                              -> result = engine.index(operation);

                              // ===== InternalEngine 层 =====
                              -> InternalEngine.index(Index index)
                                  -> acquireEnsureOpenRef();  // 确保 engine 未关闭
                                  -> versionMap.acquireLock(index.uid());  // 获取文档锁
                                  -> throttle.acquireThrottle();  // 索引限流

                                  // 1. 确定索引策略
                                  -> IndexingStrategy plan = indexingStrategyForOperation(index);
                                      -> 检查是否可以使用 addDocument（新文档）
                                      -> 或者必须使用 updateDocument（更新文档）
                                      -> 处理版本冲突、自动生成 ID 等

                                  // 2. 生成序列号（主分片）
                                  -> if (index.origin() == PRIMARY) {
                                      seqNo = generateSeqNoForOperationOnPrimary(index);
                                      index = new Index(..., seqNo, ...);
                                      advanceMaxSeqNoOfUpdatesOnPrimary(seqNo);
                                  }

                                  // 3. 写入 Lucene
                                  -> if (plan.indexIntoLucene) {
                                      indexResult = indexIntoLucene(index, plan);
                                          -> if (plan.useLuceneUpdateDocument) {
                                              indexWriter.updateDocument(index.uid(), index.docs());
                                          } else {
                                              indexWriter.addDocuments(index.docs());
                                          }
                                  }

                                  // 4. 写入 Translog
                                  -> if (indexResult.getResultType() == SUCCESS) {
                                      location = translog.add(new Translog.Index(index, indexResult));
                                      indexResult.setTranslogLocation(location);
                                  }

                                  // 5. 更新 VersionMap
                                  -> versionMap.maybePutIndexUnderLock(
                                      index.uid(),
                                      new IndexVersionValue(location, version, seqNo, primaryTerm)
                                  );

                                  // 6. 更新本地检查点
                                  -> localCheckpointTracker.markSeqNoAsProcessed(seqNo);
                                  -> localCheckpointTracker.markSeqNoAsPersisted(seqNo);

                                  -> indexResult.freeze();
                                  -> return indexResult;

                      // 3. 检查是否需要更新 mapping
                      -> if (result.getResultType() == MAPPING_UPDATE_REQUIRED) {
                          return handleMappingUpdateRequired(
                              context,
                              mappingUpdater,
                              waitForMappingUpdate,
                              itemDoneListener,
                              primary,
                              result,
                              version,
                              updateResult
                          );

                          // ===== Mapping 更新流程 =====
                          -> handleMappingUpdateRequired()
                              // 1. 预检查：尝试在本地合并 mapping
                              -> mapperService = primary.mapperService();
                              -> initialMappingVersion = mapperService.mappingVersion();
                              -> mergedSource = mapperService.merge(
                                  SINGLE_MAPPING_NAME,
                                  new CompressedXContent(result.getRequiredMappingUpdate()),
                                  MergeReason.MAPPING_AUTO_UPDATE_PREFLIGHT
                              ).mappingSource();

                              // 2. 检查是否真的需要更新
                              -> existingDocumentMapper = mapperService.documentMapper();
                              -> if (existingDocumentMapper != null
                                  && mergedSource.equals(existingDocumentMapper.mappingSource())) {
                                  // 已经是最新的，直接重试
                                  context.resetForNoopMappingUpdateRetry(mappingVersion);
                                  return true;
                              }

                              // 3. 发送 mapping 更新到 master
                              -> mappingUpdater.updateMappings(
                                  result.getRequiredMappingUpdate(),
                                  primary.shardId(),
                                  new ActionListener<>() {
                                      @Override
                                      public void onResponse(Void v) {
                                          // 标记需要等待 mapping 更新
                                          context.markAsRequiringMappingUpdate();

                                          // 等待集群状态更新
                                          waitForMappingUpdate.accept(
                                              ActionListener.runAfter(
                                                  new ActionListener<>() {
                                                      @Override
                                                      public void onResponse(Void v) {
                                                          // Mapping 更新完成，重置上下文以重试
                                                          context.resetForMappingUpdateRetry();
                                                      }
                                                      @Override
                                                      public void onFailure(Exception e) {
                                                          context.failOnMappingUpdate(e);
                                                      }
                                                  },
                                                  () -> itemDoneListener.onResponse(null)
                                              ),
                                              initialMappingVersion
                                          );
                                      }
                                      @Override
                                      public void onFailure(Exception e) {
                                          // Mapping 更新失败
                                          onComplete(exceptionToResult(e, ...), context, updateResult);
                                          itemDoneListener.onResponse(null);
                                      }
                                  }
                              );

                              // MappingUpdatedAction.updateMappingOnMaster()
                              -> semaphore.acquire();  // 限流
                              -> sendUpdateMapping(index, mappingUpdate, listener);
                                  -> PutMappingRequest putMappingRequest = new PutMappingRequest();
                                  -> putMappingRequest.setConcreteIndex(index);
                                  -> putMappingRequest.source(mappingUpdate.toString(), XContentType.JSON);
                                  -> putMappingRequest.masterNodeTimeout(dynamicMappingUpdateTimeout);
                                  -> client.execute(TransportAutoPutMappingAction.TYPE, putMappingRequest, listener);

                                      // Master 节点处理
                                      -> TransportAutoPutMappingAction.masterOperation()
                                      -> MetadataMappingService.putMapping()
                                      -> 更新 IndexMetadata
                                      -> 增加 mapping 版本号
                                      -> 发布新的集群状态

                                      // 所有节点接收新的集群状态
                                      -> ClusterApplierService.onNewClusterState()
                                      -> IndicesClusterStateService.applyClusterState()
                                      -> 更新本地 MapperService

                                      // ClusterStateObserver 检测到变化
                                      -> observer.onNewClusterState(state)
                                      -> mappingUpdateListener.onResponse(null)
                                      -> context.resetForMappingUpdateRetry()
                                      -> executor.execute(this)  // 重新执行 doRun()

                                      // 重试索引操作（这次使用新的 mapping）
                                      -> executeBulkItemRequest(...)
                                      -> primary.applyIndexOperationOnPrimary(...)
                                      -> 这次 dynamicMappingsUpdate() 返回 null
                                      -> index(engine, operation)  // 成功写入

                              // 返回 false 表示需要等待 mapping 更新
                              -> return false;
                      }
                  }

                  // 4. 操作完成
                  -> onComplete(result, context, updateResult);
                  -> context.markOperationAsExecuted(result);
                  -> context.markAsCompleted(executionResult);
                  -> return true;

              // 所有操作完成，构造响应
              -> finishRequest()
              -> new WritePrimaryResult<>(
                  context.getBulkShardRequest(),
                  context.buildShardResponse(),
                  context.getLocationToSync(),
                  context.getPrimary(),
                  logger,
                  postWriteRefresh,
                  postWriteAction
              )

// ========== 第三阶段：副本复制 ==========
      -> handlePrimaryResult(primaryResult)
          -> this.primaryResult = primaryResult;
          -> replicationGroup = primary.getReplicationGroup();

          // 1. 处理 unpromotable 副本
          -> pendingActions.incrementAndGet();
          -> replicasProxy.onPrimaryOperationComplete(
              replicaRequest,
              replicationGroup.getRoutingTable(),
              ActionListener.wrap(
                  ignored -> decPendingAndFinishIfNeeded(),
                  exception -> {
                      totalShards.incrementAndGet();
                      shardReplicaFailures.add(...);
                      decPendingAndFinishIfNeeded();
                  }
              )
          );

          // 2. 标记不可用的 in-sync 分片为过时
          -> markUnavailableShardsAsStale(replicaRequest, replicationGroup);
              -> for (String allocationId : replicationGroup.getUnavailableInSyncShards()) {
                  pendingActions.incrementAndGet();
                  replicasProxy.markShardCopyAsStaleIfNeeded(
                      shardId,
                      allocationId,
                      primaryTerm,
                      ActionListener.wrap(
                          r -> decPendingAndFinishIfNeeded(),
                          e -> decPendingAndFinishIfNeeded()
                      )
                  );
              }

          // 3. 向所有副本分片发送请求
          -> performOnReplicas(
              replicaRequest,
              globalCheckpoint,
              maxSeqNoOfUpdatesOrDeletes,
              replicationGroup,
              pendingReplicationActions
          );
              -> totalShards.addAndGet(replicationGroup.getSkippedShards().size());

              // 遍历所有复制目标（副本分片）
              -> for (ShardRouting shard : replicationGroup.getReplicationTargets()) {
                  if (shard.isSameAllocation(primaryRouting) == false) {
                      performOnReplica(
                          shard,
                          replicaRequest,
                          globalCheckpoint,
                          maxSeqNoOfUpdatesOrDeletes,
                          pendingReplicationActions
                      );

                      // ===== 单个副本操作 =====
                      -> performOnReplica()
                          -> totalShards.incrementAndGet();
                          -> pendingActions.incrementAndGet();  // 每个副本操作 +1

                          // 副本操作监听器
                          -> replicationListener = new ActionListener<>() {
                              @Override
                              public void onResponse(ReplicaResponse response) {
                                  successfulShards.incrementAndGet();
                                  updateCheckPoints(
                                      shard,
                                      response::localCheckpoint,
                                      response::globalCheckpoint,
                                      () -> decPendingAndFinishIfNeeded()
                                  );
                              }

                              @Override
                              public void onFailure(Exception replicaException) {
                                  // 副本失败，记录但不阻止整体操作
                                  shardReplicaFailures.add(new ReplicationResponse.ShardInfo.Failure(...));
                                  replicasProxy.failShardIfNeeded(
                                      shard,
                                      primaryTerm,
                                      message,
                                      replicaException,
                                      ActionListener.wrap(
                                          r -> decPendingAndFinishIfNeeded(),
                                          ReplicationOperation.this::onNoLongerPrimary
                                      )
                                  );
                              }
                          };

                          // 创建可重试的副本操作
                          -> replicationAction = new RetryableAction<>(...) {
                              @Override
                              public void tryAction(ActionListener<ReplicaResponse> listener) {
                                  replicasProxy.performOn(
                                      shard,
                                      replicaRequest,
                                      primaryTerm,
                                      globalCheckpoint,
                                      maxSeqNoOfUpdatesOrDeletes,
                                      listener
                                  );

                                  // ReplicasProxy.performOn()
                                  -> nodeId = replica.currentNodeId();
                                  -> node = clusterService.state().nodes().get(nodeId);
                                  -> concreteReplicaRequest = new ConcreteReplicaRequest<>(
                                      request,
                                      replica.allocationId().getId(),
                                      primaryTerm,
                                      globalCheckpoint,
                                      maxSeqNoOfUpdatesOrDeletes
                                  );

                                  // 通过 Transport 层发送 RPC 请求
                                  -> transportService.sendRequest(
                                      node,
                                      transportReplicaAction,
                                      concreteReplicaRequest,
                                      transportOptions,
                                      handler
                                  );

                                  // ===== 副本节点处理 =====
                                  -> TransportReplicationAction.handleReplicaRequest()
                                  -> checkReplicaLimits(replicaRequest.getRequest());
                                  -> new AsyncReplicaAction(replicaRequest, listener, task).run();
                                      -> AsyncReplicaAction.doRun()
                                      -> acquireReplicaOperationPermit(
                                          indexShard,
                                          replicaRequest.getPrimaryTerm(),
                                          replicaRequest.getGlobalCheckpoint(),
                                          replicaRequest.getMaxSeqNoOfUpdatesOrDeletes(),
                                          this,
                                          executor
                                      );
                                          -> 检查副本分片状态
                                          -> 更新全局检查点
                                          -> 获取副本操作许可

                                      -> runWithReplicaShardReference()
                                      -> shardOperationOnReplica(replicaRequest, replica, listener);
                                          -> TransportWriteAction.shardOperationOnReplica()
                                          -> executor.execute() // 切换到 WRITE 线程池
                                          -> dispatchedShardOperationOnReplica(request, replica, listener);
                                              -> TransportShardBulkAction.dispatchedShardOperationOnReplica()
                                              -> performOnReplica(request, replica);
                                                  -> for (BulkItemRequest item : request.items()) {
                                                      response = item.getPrimaryResponse();

                                                      if (response.isFailed()) {
                                                          if (response.getFailure().getSeqNo() != UNASSIGNED_SEQ_NO) {
                                                              // 主分片失败但有 seqNo，标记为 no-op
                                                              operationResult = replica.markSeqNoAsNoop(
                                                                  response.getFailure().getSeqNo(),
                                                                  primaryTerm,
                                                                  response.getFailure().getMessage()
                                                              );
                                                          }
                                                      } else {
                                                          if (response.getResponse().getResult() != NOOP) {
                                                              // 执行副本操作
                                                              operationResult = performOpOnReplica(
                                                                  response.getResponse(),
                                                                  item.request(),
                                                                  replica
                                                              );

                                                              // performOpOnReplica()
                                                              -> switch (docWriteRequest.opType()) {
                                                                  case INDEX -> {
                                                                      IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                                                                      SourceToParse sourceToParse = new SourceToParse(...);
                                                                      result = replica.applyIndexOperationOnReplica(
                                                                          primaryResponse.getSeqNo(),  // 使用主分片的 seqNo
                                                                          primaryResponse.getPrimaryTerm(),
                                                                          primaryResponse.getVersion(),
                                                                          indexRequest.getAutoGeneratedTimestamp(),
                                                                          indexRequest.isRetry(),
                                                                          sourceToParse
                                                                      );

                                                                      // IndexShard.applyIndexOperationOnReplica()
                                                                      -> applyIndexOperation(
                                                                          engine,
                                                                          seqNo,  // 使用主分片的 seqNo
                                                                          opPrimaryTerm,
                                                                          version,
                                                                          null,
                                                                          UNASSIGNED_SEQ_NO,
                                                                          0,
                                                                          autoGeneratedTimeStamp,
                                                                          isRetry,
                                                                          Origin.REPLICA,
                                                                          sourceToParse
                                                                      );
                                                                          -> operation = prepareIndex(...);
                                                                          -> update = operation.parsedDoc().dynamicMappingsUpdate();
                                                                          -> if (update != null) {
                                                                              // 副本也可能遇到 mapping 更新
                                                                              // 抛出 RetryOnReplicaException，触发重试
                                                                              throw new RetryOnReplicaException(
                                                                                  replica.shardId(),
                                                                                  "Mappings are not available on the replica yet"
                                                                              );
                                                                          }
                                                                          -> return index(engine, operation);
                                                                              -> InternalEngine.index(operation)
                                                                                  // 副本写入流程与主分片类似
                                                                                  -> markSeqNoAsSeen(seqNo);  // 副本使用主分片的 seqNo
                                                                                  -> indexIntoLucene(index, plan);
                                                                                  -> translog.add(new Translog.Index(index, indexResult));
                                                                                  -> versionMap.maybePutIndexUnderLock(...);
                                                                                  -> localCheckpointTracker.markSeqNoAsProcessed(seqNo);
                                                                                  -> return indexResult;
                                                                  }
                                                                  case DELETE -> {
                                                                      result = replica.applyDeleteOperationOnReplica(...);
                                                                  }
                                                              }
                                                          }
                                                      }

                                                      // 同步操作结果
                                                      location = syncOperationResultOrThrow(operationResult, location);
                                                  }
                                                  -> return location;

                                              -> new WriteReplicaResult<>(
                                                  request,
                                                  location,
                                                  null,
                                                  replica,
                                                  logger,
                                                  postWriteAction
                                              );

                                          -> listener.onResponse(replicaResult);
                                              -> replicationListener.onResponse(response);
                                              -> successfulShards.incrementAndGet();
                                              -> updateCheckPoints(..., () -> decPendingAndFinishIfNeeded());
                              }

                              @Override
                              public void onFinished() {
                                  super.onFinished();
                                  pendingReplicationActions.removeReplicationAction(allocationId, this);
                              }

                              @Override
                              public boolean shouldRetry(Exception e) {
                                  // 判断是否应该重试
                                  final Throwable cause = ExceptionsHelper.unwrapCause(e);
                                  return cause instanceof CircuitBreakingException
                                      || cause instanceof EsRejectedExecutionException
                                      || cause instanceof ConnectTransportException;
                              }
                          };

                          -> pendingReplicationActions.addPendingAction(allocationId, replicationAction);
                          -> replicationAction.run();  // 异步执行
                  }
              }

          // 4. 执行后置操作（如刷新检查点）
          -> primaryResult.runPostReplicationActions(new ActionListener<>() {
              @Override
              public void onResponse(Void aVoid) {
                  successfulShards.incrementAndGet();
                  updateCheckPoints(..., () -> decPendingAndFinishIfNeeded());
              }

              @Override
              public void onFailure(Exception e) {
                  updateCheckPoints(..., () -> finishAsFailed(e));
              }
          });
              -> WritePrimaryResult.runPostReplicationActions()
              -> new AsyncAfterWriteAction(
                  primary,
                  replicaRequest(),
                  location,
                  new RespondingWriteResult() {
                      @Override
                      public void onSuccess(boolean forcedRefresh) {
                          replicationResponse.setForcedRefresh(forcedRefresh);
                          listener.onResponse(null);
                      }
                      @Override
                      public void onFailure(Exception ex) {
                          listener.onFailure(ex);
                      }
                  },
                  logger,
                  postWriteRefresh,
                  postWriteAction
              ).run();
                  -> indexShard.afterWriteOperation();
                  -> maybeFinish();  // decrement pendingOps

                  // 可选：执行 refresh
                  -> if (needsRefreshAction) {
                      pendingOps.incrementAndGet();
                      if (postWriteRefresh != null) {
                          postWriteRefresh.refreshShard(
                              request.getRefreshPolicy(),
                              indexShard,
                              location,
                              refreshListener,
                              postWriteRefreshTimeout
                          );
                      } else {
                          PostWriteRefresh.refreshReplicaShard(
                              request.getRefreshPolicy(),
                              indexShard,
                              location,
                              refreshListener
                          );
                      }
                      -> refreshListener.onResponse(forceRefresh);
                      -> refreshed.set(forceRefresh);
                      -> maybeFinish();
                  }

                  // 可选：执行 fsync
                  -> if (sync) {
                      pendingOps.incrementAndGet();
                      indexShard.syncAfterWrite(location, e -> {
                          syncFailure.set(e);
                          maybeFinish();
                      });
                  }

                  // 可选：执行后置动作
                  -> if (postWriteAction != null) {
                      postWriteAction.accept(this::maybeFinish);
                  }

// ========== 第四阶段：完成并响应客户端 ==========
      -> decPendingAndFinishIfNeeded()
          -> if (pendingActions.decrementAndGet() == 0) {
              finish();
          }

      -> finish()
          -> if (finished.compareAndSet(false, true)) {
              // 构造响应信息
              primaryResult.setShardInfo(
                  ReplicationResponse.ShardInfo.of(
                      totalShards.get(),        // 总分片数
                      successfulShards.get(),   // 成功的分片数
                      shardReplicaFailures.toArray(ReplicationResponse.NO_FAILURES)  // 失败信息
                  )
              );
              // 响应客户端
              resultListener.onResponse(primaryResult);
          }

// 客户端收到响应
-> BulkResponse
    -> BulkItemResponse[] items
        -> for each item:
            -> ShardInfo shardInfo
                -> int total  // 总分片数（1 主 + N 副本）
                -> int successful  // 成功的分片数
                -> Failure[] failures  // 失败的分片详情
```

---

**文档版本：** 2.0
**最后更新：** 2026-01-17
**基于 Elasticsearch 版本：** 8.x
