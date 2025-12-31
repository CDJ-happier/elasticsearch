# Elasticsearch 集群形成详细代码分析

本文档是对 `bootstrap_analysis.md` 的补充,重点从代码层面详细分析Elasticsearch的集群形成过程。

## 一、节点发现与选举流程的代码实现

### 1.1 节点发现的完整调用链

#### 启动入口
```
Node.start()
  └─> Coordinator.start()
      └─> Coordinator.startInitialJoin()
          ├─> becomeCandidate("startInitialJoin")
          │   └─> peerFinder.activate(lastAcceptedNodes)
          │       └─> handleWakeUp()  // 开始探测循环
          └─> clusterBootstrapService.scheduleUnconfiguredBootstrap()
```

#### PeerFinder的探测循环详解

**代码位置**: `PeerFinder.java#handleWakeUp()`

```java
private boolean handleWakeUp() {
    // 【步骤1】探测上次集群状态中的主节点
    // 如果节点重启,会优先尝试连接之前的主节点
    for (DiscoveryNode discoveryNode : lastAcceptedNodes.getMasterNodes().values()) {
        startProbe(discoveryNode.getAddress());
    }

    // 【步骤2】解析并探测配置的种子节点
    // 这是首次启动时发现其他节点的主要方式
    configuredHostsResolver.resolveConfiguredHosts(providedAddresses -> {
        synchronized (mutex) {
            lastResolvedAddresses = providedAddresses;
            providedAddresses.forEach(this::startProbe);
        }
    });

    // 【步骤3】定期重复探测(默认每1秒)
    // 持续发现新节点,直到找到Leader或成为Leader
    transportService.getThreadPool().scheduleUnlessShuttingDown(
        findPeersInterval,  // 默认1秒
        clusterCoordinationExecutor,
        new Runnable() {
            public void run() {
                synchronized (mutex) {
                    if (handleWakeUp() == false) return;
                }
                onFoundPeersUpdated();  // 通知发现的节点已更新
            }
        }
    );
}
```

#### 探测单个节点的流程

**代码位置**: `PeerFinder.Peer#establishConnection()`

```java
private void establishConnection() {
    // 【步骤1】建立传输层连接
    transportAddressConnector.connectToRemoteMasterNode(transportAddress, new ActionListener<>() {
        @Override
        public void onResponse(DiscoveryNode remoteNode) {
            // 连接成功,记录节点信息
            discoveryNode = remoteNode;

            // 【步骤2】请求对方已知的节点列表
            requestPeers();
        }

        @Override
        public void onFailure(Exception e) {
            // 连接失败,标记为失败并稍后重试
            connectionState = ConnectionState.DISCONNECTED;
        }
    });
}
```

#### 请求并处理对等节点信息

**代码位置**: `PeerFinder.Peer#requestPeers()`

```java
private void requestPeers() {
    // 【步骤1】构造PeersRequest,包含本地节点和已知节点
    final PeersRequest peersRequest = new PeersRequest(
        getLocalNode(),
        getFoundPeersUnderLock()  // 本地已发现的所有节点
    );

    // 【步骤2】发送请求
    transportService.sendRequest(
        discoveryNode,
        REQUEST_PEERS_ACTION_NAME,
        peersRequest,
        TransportRequestOptions.timeout(requestPeersTimeout),
        new TransportResponseHandler<PeersResponse>() {
            @Override
            public void handleResponse(PeersResponse response) {
                synchronized (mutex) {
                    // 【步骤3】记录对方知道的主节点
                    lastKnownMasterNode = response.getMasterNode();

                    // 【步骤4】探测响应中的主节点
                    response.getMasterNode().ifPresent(node ->
                        startProbe(node.getAddress())
                    );

                    // 【步骤5】探测响应中的其他节点(传播式发现)
                    for (DiscoveryNode node : response.getKnownPeers()) {
                        startProbe(node.getAddress());
                    }
                }

                // 【步骤6】如果对方认为自己是主节点,尝试加入
                if (response.getMasterNode().equals(Optional.of(discoveryNode))) {
                    onActiveMasterFound(discoveryNode, response.getTerm());
                }
            }
        }
    );
}
```

### 1.2 集群引导的触发条件

**代码位置**: `ClusterBootstrapService.java#onFoundPeersUpdated()`

```java
public void onFoundPeersUpdated() {
    final Set<DiscoveryNode> nodes = getDiscoveredNodes();

    // 【前置条件检查】
    if (bootstrappingPermitted.get()  // 允许引导
        && transportService.getLocalNode().isMasterNode()  // 本地节点是主节点候选者
        && bootstrapRequirements.isEmpty() == false  // 配置了initial_master_nodes
        && isBootstrappedSupplier.getAsBoolean() == false) {  // 集群尚未引导

        // 【步骤1】检查发现的节点是否匹配initial_master_nodes
        final Tuple<Set<DiscoveryNode>, List<String>> requirementMatchingResult
            = checkRequirements(nodes);

        final Set<DiscoveryNode> nodesMatchingRequirements = requirementMatchingResult.v1();
        final List<String> unsatisfiedRequirements = requirementMatchingResult.v2();

        // 【步骤2】验证本地节点在要求列表中
        if (nodesMatchingRequirements.contains(transportService.getLocalNode()) == false) {
            bootstrappingPermitted.set(false);
            return;
        }

        // 【步骤3】检查是否满足多数派条件
        // 关键公式: nodesMatchingRequirements.size() * 2 > bootstrapRequirements.size()
        // 例如: 配置3个节点,至少发现2个才能引导 (2 * 2 > 3)
        if (nodesMatchingRequirements.size() * 2 > bootstrapRequirements.size()) {
            startBootstrap(nodesMatchingRequirements, unsatisfiedRequirements);
        }
    }
}
```

#### 匹配节点的逻辑

```java
private Tuple<Set<DiscoveryNode>, List<String>> checkRequirements(Set<DiscoveryNode> nodes) {
    final Set<DiscoveryNode> selectedNodes = new HashSet<>();
    final List<String> unmatchedRequirements = new ArrayList<>();

    for (final String bootstrapRequirement : bootstrapRequirements) {
        // 【匹配规则】通过节点名称、地址或IP匹配
        final Set<DiscoveryNode> matchingNodes = nodes.stream()
            .filter(n -> matchesRequirement(n, bootstrapRequirement))
            .collect(Collectors.toSet());

        if (matchingNodes.size() == 0) {
            // 未匹配到节点,记录为未满足的要求
            unmatchedRequirements.add(bootstrapRequirement);
        }

        if (matchingNodes.size() > 1) {
            // 匹配到多个节点,配置有歧义
            throw new IllegalStateException(
                "requirement [" + bootstrapRequirement + "] matches multiple nodes"
            );
        }

        selectedNodes.addAll(matchingNodes);
    }

    return Tuple.tuple(selectedNodes, unmatchedRequirements);
}
```

### 1.3 选举流程的详细实现

#### 选举调度器的工作原理

**代码位置**: `ElectionSchedulerFactory.java#startElectionScheduler()`

```java
public Releasable startElectionScheduler(TimeValue gracePeriod, Runnable scheduledRunnable) {
    // 【关键设计】使用闭包和递归调度实现持续的选举尝试
    final AtomicBoolean isClosed = new AtomicBoolean();

    // 【递归调度函数】
    final Runnable[] scheduleNextElection = new Runnable[1];
    scheduleNextElection[0] = new Runnable() {
        @Override
        public void run() {
            if (isClosed.get()) {
                return;  // 已关闭,停止调度
            }

            // 【计算随机延迟】避免多个节点同时发起选举
            final long thisGracePeriod = gracePeriod.millis();
            final long maxAdditionalDelay = maxElectionDelay.millis();
            final long randomAdditionalDelay = random.nextLong(maxAdditionalDelay + 1);
            final long totalDelay = thisGracePeriod + randomAdditionalDelay;

            // 【调度下一次选举】
            threadPool.scheduleUnlessShuttingDown(
                TimeValue.timeValueMillis(totalDelay),
                Names.CLUSTER_COORDINATION,
                new Runnable() {
                    @Override
                    public void run() {
                        if (isClosed.get()) {
                            return;
                        }

                        // 【先调度下一次】确保持续尝试
                        scheduleNextElection[0].run();

                        // 【执行选举逻辑】
                        scheduledRunnable.run();
                    }
                }
            );
        }
    };

    // 【首次调度】立即开始
    scheduleNextElection[0].run();

    // 【返回关闭句柄】
    return () -> isClosed.set(true);
}
```

**设计亮点**:
1. **递归调度**: 每次执行前先调度下一次,确保持续尝试
2. **随机退避**: 使用随机延迟避免冲突
3. **优雅期**: 首次调度可以设置优雅期(gracePeriod),后续调度会增加延迟
4. **原子关闭**: 使用AtomicBoolean确保线程安全的关闭

#### 预投票流程

**代码位置**: `Coordinator.java#startPreVotingRound()`

```java
private void startPreVotingRound() {
    synchronized (mutex) {
        if (mode != Mode.CANDIDATE) {
            return;  // 只有候选者才能发起预投票
        }

        // 【步骤1】关闭之前的预投票轮次
        closePrevotingRound();

        // 【步骤2】启动新的预投票轮次
        prevotingRound = preVoteCollector.start(
            getLastAcceptedState(),
            getDiscoveredNodes()
        );
    }
}
```

**PreVoteCollector的实现**:

```java
public Releasable start(ClusterState clusterState, Iterable<DiscoveryNode> broadcastNodes) {
    // 【步骤1】向所有发现的节点发送PreVoteRequest
    for (DiscoveryNode node : broadcastNodes) {
        transportService.sendRequest(
            node,
            PRE_VOTE_ACTION_NAME,
            new PreVoteRequest(getLocalNode(), getCurrentTerm()),
            new TransportResponseHandler<PreVoteResponse>() {
                @Override
                public void handleResponse(PreVoteResponse response) {
                    // 【步骤2】收集预投票响应
                    synchronized (mutex) {
                        if (response.willVote()) {
                            preVotes.add(node);
                        }

                        // 【步骤3】检查是否获得多数预投票
                        if (isQuorum(preVotes)) {
                            // 获得多数预投票,发起正式选举
                            startElection.run();
                        }
                    }
                }
            }
        );
    }

    return () -> {
        // 关闭预投票轮次
    };
}
```

#### 正式选举流程

**代码位置**: `Coordinator.java#startElection()`

```java
private void startElection() {
    synchronized (mutex) {
        // 【前置检查】确保仍处于候选者模式
        if (mode != Mode.CANDIDATE) {
            return;
        }

        // 【步骤1】检查本地节点是否有资格赢得选举
        final var nodeEligibility = localNodeMayWinElection(getLastAcceptedState(), electionStrategy);
        if (nodeEligibility.mayWin() == false) {
            logger.trace("skip election as local node may not win it ({})", nodeEligibility.reason());
            return;
        }

        // 【步骤2】获取新的任期号(term + 1)
        final var electionTerm = getTermForNewElection();

        logger.debug("starting election for {} in term {}", getLocalNode(), electionTerm);

        // 【步骤3】向所有发现的节点广播StartJoinRequest
        broadcastStartJoinRequest(getLocalNode(), electionTerm, getDiscoveredNodes());
    }
}
```

**广播StartJoinRequest**:

```java
private void broadcastStartJoinRequest(DiscoveryNode candidateMasterNode, long term, List<DiscoveryNode> discoveredNodes) {
    // 【步骤1】通过选举策略创建StartJoinRequest
    electionStrategy.onNewElection(candidateMasterNode, term, new ActionListener<>() {
        @Override
        public void onResponse(StartJoinRequest startJoinRequest) {
            // 【步骤2】向每个发现的节点发送StartJoinRequest
            discoveredNodes.forEach(node ->
                joinHelper.sendStartJoinRequest(startJoinRequest, node)
            );
        }

        @Override
        public void onFailure(Exception e) {
            logger.log(
                e instanceof CoordinationStateRejectedException ? Level.DEBUG : Level.WARN,
                "election attempt failed",
                e
            );
        }
    });
}
```

### 1.4 投票处理流程

#### 接收StartJoinRequest并返回Join

**代码位置**: `JoinHelper.java` 的 START_JOIN_ACTION handler

```java
transportService.registerRequestHandler(
    START_JOIN_ACTION_NAME,
    transportService.getThreadPool().executor(Names.CLUSTER_COORDINATION),
    false,
    false,
    StartJoinRequest::new,
    (request, channel, task) -> {
        // 【步骤1】获取候选节点
        final DiscoveryNode destination = request.getMasterCandidateNode();

        // 【步骤2】调用joinLeaderInTerm处理请求
        // 这会更新本地term,转换为CANDIDATE,生成Join对象
        final Join join = joinLeaderInTerm.apply(request);

        // 【步骤3】向候选节点发送Join请求(投票)
        sendJoinRequest(destination, currentTermSupplier.getAsLong(), Optional.of(join));

        // 【步骤4】立即返回空响应
        // StartJoin是单向通知,不需要等待Join发送完成
        channel.sendResponse(Empty.INSTANCE);
    }
);
```

#### joinLeaderInTerm的实现

**代码位置**: `Coordinator.java#joinLeaderInTerm()`

```java
private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
    synchronized (mutex) {
        logger.debug("joinLeaderInTerm: for [{}] with term {}",
            startJoinRequest.getMasterCandidateNode(),
            startJoinRequest.getTerm());

        // 【步骤1】处理StartJoin请求,更新term,生成Join对象
        final Join join = coordinationState.get().handleStartJoin(startJoinRequest);

        // 【步骤2】记录最后一次投票
        lastJoin = Optional.of(join);

        // 【步骤3】通知PeerFinder更新term
        peerFinder.setCurrentTerm(getCurrentTerm());

        // 【步骤4】转换节点状态为CANDIDATE
        if (mode != Mode.CANDIDATE) {
            becomeCandidate("joinLeaderInTerm");
        } else {
            // 如果已经是CANDIDATE,更新状态
            followersChecker.updateFastResponseState(getCurrentTerm(), mode);
            preVoteCollector.update(getPreVoteResponse(), null);
        }

        return join;
    }
}
```

#### CoordinationState处理StartJoin

**代码位置**: `CoordinationState.java#handleStartJoin()`

```java
public Join handleStartJoin(StartJoinRequest startJoinRequest) {
    // 【验证1】term必须大于当前term
    if (startJoinRequest.getTerm() <= getCurrentTerm()) {
        throw new CoordinationStateRejectedException(
            "incoming term " + startJoinRequest.getTerm() +
            " not greater than current term " + getCurrentTerm()
        );
    }

    logger.debug("handleStartJoin: leaving term [{}] due to {}", getCurrentTerm(), startJoinRequest);

    // 【步骤1】更新当前term
    persistedState.setCurrentTerm(startJoinRequest.getTerm());

    // 【步骤2】重置状态
    lastPublishedVersion = 0;
    lastPublishedConfiguration = getLastAcceptedConfiguration();
    startedJoinSinceLastReboot = true;
    electionWon = false;
    joinVotes = new VoteCollection();  // 重置投票集合
    publishVotes = new VoteCollection();

    // 【步骤3】生成Join对象
    return new Join(
        localNode,  // 投票节点(本节点)
        startJoinRequest.getMasterCandidateNode(),  // 候选节点
        getCurrentTerm(),  // 当前term
        getLastAcceptedTerm(),  // 最后接受的term
        getLastAcceptedVersion()  // 最后接受的version
    );
}
```

#### 候选节点处理Join请求

**代码位置**: `Coordinator.java#handleJoinRequest()`

```java
private void handleJoinRequest(JoinRequest joinRequest, ActionListener<Void> joinListener) {
    // 【步骤1】建立与投票节点的反向连接
    transportService.connectToNode(joinRequest.getSourceNode(), new ActionListener<>() {
        @Override
        public void onResponse(Releasable response) {
            // 【步骤2】验证Join请求
            validateJoinRequest(joinRequest, ActionListener.runBefore(joinListener,
                () -> Releasables.close(response))
                .delegateFailure((l, ignored) ->
                    // 【步骤3】处理Join请求
                    processJoinRequest(joinRequest, l)
                )
            );
        }
    });
}
```

**处理Join请求**:

```java
private void processJoinRequest(JoinRequest joinRequest, ActionListener<Void> joinListener) {
    synchronized (mutex) {
        // 【步骤1】更新maxTermSeen
        updateMaxTermSeen(joinRequest.getTerm());

        // 【步骤2】记录选举前的状态
        final boolean prevElectionWon = coordinationState.get().electionWon();

        // 【步骤3】处理Join对象
        joinRequest.getOptionalJoin().ifPresent(this::handleJoin);

        // 【步骤4】将Join请求交给JoinAccumulator
        joinAccumulator.handleJoinRequest(
            joinRequest.getSourceNode(),
            joinRequest.getCompatibilityVersions(),
            joinRequest.getFeatures(),
            joinListener
        );

        // 【步骤5】检查是否赢得选举
        if (prevElectionWon == false && coordinationState.get().electionWon()) {
            becomeLeader();  // 成为Leader
        }
    }
}
```

**CoordinationState处理Join**:

```java
public boolean handleJoin(Join join) {
    // 【验证1】term必须匹配
    if (join.term() != getCurrentTerm()) {
        throw new CoordinationStateRejectedException(
            "incoming term " + join.term() +
            " does not match current term " + getCurrentTerm()
        );
    }

    // 【验证2】投票节点的lastAcceptedTerm不能大于本地
    if (join.lastAcceptedTerm() > getLastAcceptedTerm()) {
        throw new CoordinationStateRejectedException(
            "incoming last accepted term " + join.lastAcceptedTerm() +
            " higher than current last accepted term " + getLastAcceptedTerm()
        );
    }

    // 【验证3】同一term内,投票节点的lastAcceptedVersion不能大于本地
    if (join.lastAcceptedTerm() == getLastAcceptedTerm()
        && join.lastAcceptedVersion() > getLastAcceptedVersion()) {
        throw new CoordinationStateRejectedException(
            "incoming last accepted version " + join.lastAcceptedVersion() +
            " higher than current last accepted version " + getLastAcceptedVersion()
        );
    }

    // 【步骤1】添加投票
    boolean added = joinVotes.addJoinVote(join);

    // 【步骤2】检查是否赢得选举
    boolean prevElectionWon = electionWon;
    electionWon = isElectionQuorum(joinVotes);

    logger.debug("handleJoin: added join {} from [{}] for election, electionWon={}",
        join, join.votingNode(), electionWon);

    // 【步骤3】如果刚刚赢得选举,更新lastPublishedVersion
    if (electionWon && prevElectionWon == false) {
        logger.debug("handleJoin: election won in term [{}] with {}", getCurrentTerm(), joinVotes);
        lastPublishedVersion = getLastAcceptedVersion();
    }

    return added;
}
```

## 二、CoordinationState类的核心职责

### 2.1 类的设计定位

**CoordinationState**是Elasticsearch集群协调算法的核心实现类,直接对应Raft协议的状态机。

**设计特点**:
1. **状态封装**: 封装了所有协调相关的状态(term、投票、集群状态等)
2. **协议实现**: 实现了Raft协议的核心逻辑(投票、日志复制、提交等)
3. **持久化抽象**: 通过PersistedState接口抽象持久化操作
4. **线程安全**: 所有方法都假设在外部同步(Coordinator的mutex)

### 2.2 核心状态变量

```java
public class CoordinationState {
    // 【本地节点信息】
    private final DiscoveryNode localNode;

    // 【选举策略】决定如何判断选举成功
    private final ElectionStrategy electionStrategy;

    // 【持久化状态】
    private final PersistedState persistedState;

    // 【瞬态状态 - 投票相关】
    private VoteCollection joinVotes;  // 收集的Join投票
    private boolean startedJoinSinceLastReboot;  // 重启后是否开始过Join
    private boolean electionWon;  // 是否赢得选举

    // 【瞬态状态 - 发布相关】
    private long lastPublishedVersion;  // 最后发布的版本号
    private VotingConfiguration lastPublishedConfiguration;  // 最后发布的投票配置
    private VoteCollection publishVotes;  // 收集的发布投票
}
```

### 2.3 核心方法详解

#### handleStartJoin - 处理选举邀请

```java
/**
 * 处理StartJoinRequest,决定是否投票给候选节点
 *
 * @param startJoinRequest 选举邀请请求
 * @return Join对象,包含投票信息
 * @throws CoordinationStateRejectedException 如果拒绝投票
 */
public Join handleStartJoin(StartJoinRequest startJoinRequest) {
    // 【关键决策】只要请求的term大于当前term,就同意投票
    // 这意味着ES允许一个节点在同一term内多次投票(与标准Raft不同)

    if (startJoinRequest.getTerm() <= getCurrentTerm()) {
        throw new CoordinationStateRejectedException(
            "incoming term not greater than current term"
        );
    }

    // 更新term并生成Join对象
    persistedState.setCurrentTerm(startJoinRequest.getTerm());
    // ... 重置状态 ...

    return new Join(localNode, startJoinRequest.getMasterCandidateNode(),
        getCurrentTerm(), getLastAcceptedTerm(), getLastAcceptedVersion());
}
```

**设计要点**:
- **宽松投票**: 只要term更高就投票,不限制每个term只投一次
- **状态重置**: 更新term时重置所有瞬态状态
- **持久化**: term的更新会立即持久化

#### handleJoin - 处理投票

```java
/**
 * 处理收到的Join投票,累积投票并判断是否赢得选举
 *
 * @param join 投票信息
 * @return true表示这是一个新的投票
 * @throws CoordinationStateRejectedException 如果投票不合法
 */
public boolean handleJoin(Join join) {
    // 【验证1】term必须匹配
    if (join.term() != getCurrentTerm()) {
        throw new CoordinationStateRejectedException("term mismatch");
    }

    // 【验证2】投票节点的状态不能比本地更新
    // 这确保了只有拥有最新状态的节点才能成为Leader
    if (join.lastAcceptedTerm() > getLastAcceptedTerm()) {
        throw new CoordinationStateRejectedException("joiner has better term");
    }

    if (join.lastAcceptedTerm() == getLastAcceptedTerm()
        && join.lastAcceptedVersion() > getLastAcceptedVersion()) {
        throw new CoordinationStateRejectedException("joiner has better version");
    }

    // 【累积投票】
    boolean added = joinVotes.addJoinVote(join);

    // 【判断是否赢得选举】
    boolean prevElectionWon = electionWon;
    electionWon = isElectionQuorum(joinVotes);

    // 【更新发布版本】
    if (electionWon && prevElectionWon == false) {
        lastPublishedVersion = getLastAcceptedVersion();
    }

    return added;
}
```

**设计要点**:
- **状态比较**: 确保Leader拥有最新的集群状态
- **Quorum判断**: 通过ElectionStrategy判断是否获得多数投票
- **幂等性**: 重复的投票会被忽略

#### handleClientValue - 准备发布集群状态

```java
/**
 * 准备发布新的集群状态
 *
 * @param clusterState 要发布的集群状态
 * @return PublishRequest 发布请求
 * @throws CoordinationStateRejectedException 如果不能发布
 */
public PublishRequest handleClientValue(ClusterState clusterState) {
    // 【前置条件】必须已经赢得选举
    if (electionWon == false) {
        throw new CoordinationStateRejectedException("election not won");
    }

    // 【前置条件】上一个状态必须已经被接受
    if (lastPublishedVersion != getLastAcceptedVersion()) {
        throw new CoordinationStateRejectedException(
            "cannot start publishing next value before accepting previous one"
        );
    }

    // 【验证】term和version必须递增
    if (clusterState.term() != getCurrentTerm()) {
        throw new CoordinationStateRejectedException("term mismatch");
    }

    if (clusterState.version() <= lastPublishedVersion) {
        throw new CoordinationStateRejectedException("version not increasing");
    }

    // 【验证】投票配置变更的限制
    if (electionStrategy.isInvalidReconfiguration(clusterState,
        getLastAcceptedConfiguration(), getLastCommittedConfiguration())) {
        throw new CoordinationStateRejectedException(
            "only allow reconfiguration while not already reconfiguring"
        );
    }

    // 【验证】新配置必须有quorum的Join投票
    if (joinVotesHaveQuorumFor(clusterState.getLastAcceptedConfiguration()) == false) {
        throw new CoordinationStateRejectedException(
            "only allow reconfiguration if joinVotes have quorum for new config"
        );
    }

    // 【更新状态】
    lastPublishedVersion = clusterState.version();
    lastPublishedConfiguration = clusterState.getLastAcceptedConfiguration();
    publishVotes = new VoteCollection();  // 重置发布投票

    return new PublishRequest(clusterState);
}
```

**设计要点**:
- **严格验证**: 确保发布的状态满足所有约束
- **配置变更**: 限制投票配置的变更时机
- **两阶段提交**: 通过publishVotes实现两阶段提交

#### handlePublishRequest - 接受集群状态

```java
/**
 * 处理收到的集群状态发布请求
 *
 * @param publishRequest 发布请求
 * @return PublishResponse 发布响应
 * @throws CoordinationStateRejectedException 如果拒绝接受
 */
public PublishResponse handlePublishRequest(PublishRequest publishRequest) {
    final ClusterState clusterState = publishRequest.getAcceptedState();

    // 【验证】term必须匹配
    if (clusterState.term() != getCurrentTerm()) {
        throw new CoordinationStateRejectedException("term mismatch");
    }

    // 【验证】version必须递增
    if (clusterState.term() == getLastAcceptedTerm()
        && clusterState.version() <= getLastAcceptedVersion()) {
        throw new CoordinationStateRejectedException("version not increasing");
    }

    // 【接受状态】持久化到磁盘
    persistedState.setLastAcceptedState(clusterState);

    return new PublishResponse(clusterState.term(), clusterState.version());
}
```

**设计要点**:
- **立即持久化**: 接受状态后立即持久化
- **版本检查**: 确保状态单调递增

#### handlePublishResponse - 收集发布响应

```java
/**
 * 处理节点对发布请求的响应
 *
 * @param sourceNode 响应节点
 * @param publishResponse 发布响应
 * @return Optional<ApplyCommitRequest> 如果达到quorum,返回提交请求
 * @throws CoordinationStateRejectedException 如果响应不合法
 */
public Optional<ApplyCommitRequest> handlePublishResponse(
    DiscoveryNode sourceNode,
    PublishResponse publishResponse) {

    // 【验证】必须是Leader
    if (electionWon == false) {
        throw new CoordinationStateRejectedException("election not won");
    }

    // 【验证】term和version必须匹配
    if (publishResponse.getTerm() != getCurrentTerm()) {
        throw new CoordinationStateRejectedException("term mismatch");
    }

    if (publishResponse.getVersion() != lastPublishedVersion) {
        throw new CoordinationStateRejectedException("version mismatch");
    }

    // 【累积响应】
    publishVotes.addVote(sourceNode);

    // 【检查是否达到quorum】
    if (isPublishQuorum(publishVotes)) {
        // 达到quorum,可以提交
        return Optional.of(new ApplyCommitRequest(
            localNode,
            publishResponse.getTerm(),
            publishResponse.getVersion()
        ));
    }

    return Optional.empty();
}
```

**设计要点**:
- **两阶段提交**: 只有达到quorum才能提交
- **原子性**: 确保集群状态的原子性更新

#### handleCommit - 提交集群状态

```java
/**
 * 处理提交请求,将接受的状态标记为已提交
 *
 * @param applyCommit 提交请求
 * @throws CoordinationStateRejectedException 如果不能提交
 */
public void handleCommit(ApplyCommitRequest applyCommit) {
    // 【验证】term和version必须完全匹配
    if (applyCommit.getTerm() != getCurrentTerm()) {
        throw new CoordinationStateRejectedException("term mismatch");
    }

    if (applyCommit.getTerm() != getLastAcceptedTerm()) {
        throw new CoordinationStateRejectedException("term mismatch with last accepted");
    }

    if (applyCommit.getVersion() != getLastAcceptedVersion()) {
        throw new CoordinationStateRejectedException("version mismatch");
    }

    // 【提交】标记为已提交
    persistedState.markLastAcceptedStateAsCommitted();
}
```

**设计要点**:
- **严格匹配**: 只能提交当前接受的状态
- **持久化**: 提交信息会持久化

### 2.4 VoteCollection - 投票集合

```java
public static class VoteCollection {
    // 【存储投票节点】按节点ID索引
    private final Map<String, DiscoveryNode> nodes;

    // 【存储Join对象】用于候选者模式
    private final Set<Join> joins;

    /**
     * 添加投票
     *
     * @param sourceNode 投票节点
     * @return true表示这是一个新的投票
     */
    public boolean addVote(DiscoveryNode sourceNode) {
        // 【限制】只有主节点候选者才能投票
        return sourceNode.isMasterNode()
            && nodes.put(sourceNode.getId(), sourceNode) == null;
    }

    /**
     * 添加Join投票
     *
     * @param join Join对象
     * @return true表示这是一个新的投票
     */
    public boolean addJoinVote(Join join) {
        final boolean added = addVote(join.votingNode());
        if (added) {
            joins.add(join);
        }
        return added;
    }

    /**
     * 检查是否达到quorum
     *
     * @param configuration 投票配置
     * @return true表示达到quorum
     */
    public boolean isQuorum(VotingConfiguration configuration) {
        return configuration.hasQuorum(nodes.keySet());
    }
}
```

**设计要点**:
- **去重**: 同一节点只能投一次票
- **主节点限制**: 只有主节点候选者的投票才有效
- **Quorum判断**: 委托给VotingConfiguration

## 三、核心类的协作关系

### 3.1 类图关系

```
┌─────────────────┐
│   Coordinator   │ ◄─── 集群协调的中枢
└────────┬────────┘
         │ owns
         ├──────────────────────────────────────┐
         │                                      │
         ▼                                      ▼
┌──────────────────┐                  ┌──────────────────┐
│ CoordinationState│                  │   PeerFinder     │
│                  │                  │                  │
│ - currentTerm    │                  │ - peersByAddress │
│ - joinVotes      │                  │ - leader         │
│ - publishVotes   │                  │                  │
└────────┬─────────┘                  └────────┬─────────┘
         │ uses                                │ uses
         ▼                                     ▼
┌──────────────────┐                  ┌──────────────────┐
│  PersistedState  │                  │ TransportService │
│                  │                  │                  │
│ - getCurrentTerm │                  │ - sendRequest    │
│ - setCurrentTerm │                  │ - connectToNode  │
└──────────────────┘                  └──────────────────┘

         │ owns
         ├──────────────────────────────────────┐
         │                                      │
         ▼                                      ▼
┌──────────────────┐                  ┌──────────────────┐
│   JoinHelper     │                  │ PreVoteCollector │
│                  │                  │                  │
│ - joinAccumulator│                  │ - preVoteResponse│
│ - pendingJoins   │                  │                  │
└────────┬─────────┘                  └──────────────────┘
         │ has
         ├──────────────────────────────────────┐
         │                                      │
         ▼                                      ▼
┌──────────────────┐                  ┌──────────────────┐
│CandidateJoin     │                  │  LeaderJoin      │
│Accumulator       │                  │  Accumulator     │
│                  │                  │                  │
│ - joinRequests   │                  │ - submitTask     │
└──────────────────┘                  └──────────────────┘
```

### 3.2 选举流程中的类交互时序

```
时间 │ Coordinator │ PeerFinder │ CoordinationState │ JoinHelper │ PreVoteCollector
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T1   │ becomeCandidate()
     │     │
     │     └──> activate()
     │              │
     │              └──> handleWakeUp()
     │                   └──> startProbe()
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T2   │              │ requestPeers()
     │              │     │
     │              │     └──> PeersResponse
     │              │          └──> onFoundPeersUpdated()
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T3   │ startPreVotingRound()
     │     │
     │     └────────────────────────────────────────────────────> start()
     │                                                                 │
     │                                                                 └──> sendPreVoteRequest()
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T4   │                                                                 │
     │ <───────────────────────────────────────────────────────────── PreVoteResponse
     │                                                                 │
     │                                                                 └──> isQuorum() ?
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T5   │ startElection()
     │     │
     │     └──> broadcastStartJoinRequest()
     │              │
     │              └────────────────────────────> sendStartJoinRequest()
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T6   │                                                  │
     │ <─────────────────────────────────────────────── START_JOIN handler
     │                                                  │
     │                                                  └──> joinLeaderInTerm()
     │                                                       │
     │                                                       └──> handleStartJoin()
     │                                                            └──> return Join
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T7   │                                                  │
     │                                                  └──> sendJoinRequest()
     │                                                       │
     │ <─────────────────────────────────────────────────── JOIN_REQUEST
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T8   │ handleJoinRequest()
     │     │
     │     └──> processJoinRequest()
     │              │
     │              └──> handleJoin()
     │                   └──> isElectionQuorum() ?
─────┼─────────────┼────────────┼───────────────────┼────────────┼──────────────────
T9   │ becomeLeader()
     │     │
     │     ├──> deactivate()
     │     │
     │     └──> leaderHeartbeatService.start()
```

### 3.3 集群状态发布流程中的类交互

```
时间 │ Coordinator │ CoordinationState │ PublicationHandler │ ClusterApplier
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T1   │ publish()
     │     │
     │     └──> handleClientValue()
     │          └──> return PublishRequest
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T2   │     │
     │     └────────────────────────> sendPublishRequest()
     │                                     │
     │                                     └──> PUBLISH_REQUEST
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T3   │                                     │
     │ <─────────────────────────────────── handlePublishRequest()
     │                                     │
     │                                     └──> handlePublishRequest()
     │                                          └──> return PublishResponse
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T4   │ handlePublishResponse()
     │     │
     │     └──> handlePublishResponse()
     │          └──> isPublishQuorum() ?
     │               └──> return ApplyCommitRequest
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T5   │     │
     │     └────────────────────────> sendCommitRequest()
     │                                     │
     │                                     └──> COMMIT_REQUEST
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T6   │                                     │
     │ <─────────────────────────────────── handleApplyCommit()
     │                                     │
     │                                     └──> handleCommit()
     │                                          └──> markAsCommitted()
─────┼─────────────┼───────────────────┼────────────────────┼────────────────
T7   │ applyClusterState()
     │     │
     │     └──────────────────────────────────────────────> onNewClusterState()
     │                                                            │
     │                                                            └──> apply()
```

## 四、集群形成后的服务就绪流程

### 4.1 集群状态应用流程

当集群形成并发布第一个包含所有节点的集群状态后,每个节点需要应用这个状态:

**代码位置**: `Coordinator.java#handleApplyCommit()`

```java
private void handleApplyCommit(ApplyCommitRequest applyCommitRequest, ActionListener<Void> applyListener) {
    synchronized (mutex) {
        logger.trace("handleApplyCommit: applying commit {}", applyCommitRequest);

        // 【步骤1】标记状态为已提交
        coordinationState.get().handleCommit(applyCommitRequest);

        // 【步骤2】获取已提交的状态
        final ClusterState committedState = hideStateIfNotRecovered(
            coordinationState.get().getLastAcceptedState()
        );

        // 【步骤3】根据模式决定是否添加NO_MASTER_BLOCK
        applierState = mode == Mode.CANDIDATE
            ? clusterStateWithNoMasterBlock(committedState)
            : committedState;

        // 【步骤4】更新单节点集群检查器
        updateSingleNodeClusterChecker();

        // 【步骤5】应用集群状态
        if (applyCommitRequest.getSourceNode().equals(getLocalNode())) {
            // Leader节点在发布结束时应用,不在这里
            applyListener.onResponse(null);
        } else {
            // Follower节点在这里应用
            clusterApplier.onNewClusterState(
                applyCommitRequest.toString(),
                () -> applierState,
                applyListener.map(r -> {
                    onClusterStateApplied();
                    return r;
                })
            );
        }
    }
}
```

### 4.2 ClusterApplier应用集群状态

**代码位置**: `ClusterApplierService.java#onNewClusterState()`

```java
public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier,
                              ActionListener<Void> listener) {
    // 【步骤1】提交任务到applier线程
    submitUnbatchedStateUpdateTask(source, new ClusterApplyListener() {
        @Override
        public void onNewClusterState(ClusterState newState) {
            // 【步骤2】在applier线程中应用状态
            applyChanges(newState);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    });
}
```

**应用变更**:

```java
private void applyChanges(ClusterState newClusterState) {
    final ClusterState previousClusterState = state.get();

    // 【步骤1】更新本地状态
    state.set(newClusterState);

    // 【步骤2】通知所有监听器
    for (ClusterStateListener listener : clusterStateListeners) {
        try {
            listener.clusterChanged(new ClusterChangedEvent(
                source,
                newClusterState,
                previousClusterState
            ));
        } catch (Exception e) {
            logger.warn("failed to notify listener", e);
        }
    }

    // 【步骤3】通知超时监听器
    for (TimeoutClusterStateListener listener : timeoutClusterStateListeners) {
        try {
            listener.clusterChanged(new ClusterChangedEvent(
                source,
                newClusterState,
                previousClusterState
            ));
        } catch (Exception e) {
            logger.warn("failed to notify timeout listener", e);
        }
    }

    // 【步骤4】通知节点连接服务
    nodeConnectionsService.connectToNodes(
        newClusterState.nodes(),
        () -> logger.trace("connected to nodes")
    );

    // 【步骤5】更新集群设置
    clusterSettings.applySettings(newClusterState.metadata().settings());
}
```

### 4.3 关键服务的初始化

#### IndicesService - 索引服务

**监听器**: `IndicesClusterStateService`

```java
public void clusterChanged(ClusterChangedEvent event) {
    if (event.localNodeMaster() == false) {
        // 【Follower节点】只处理分配给自己的分片
        applyClusterState(event);
    } else {
        // 【Leader节点】处理所有索引的元数据变更
        applyClusterState(event);
    }
}

private void applyClusterState(ClusterChangedEvent event) {
    final ClusterState state = event.state();

    // 【步骤1】删除不再存在的索引
    deleteIndices(event);

    // 【步骤2】创建新的索引
    createIndices(state);

    // 【步骤3】更新索引映射
    updateIndices(event);

    // 【步骤4】处理分片分配
    applyShardAllocation(event);
}
```

#### ShardStateAction - 分片状态服务

```java
public void clusterChanged(ClusterChangedEvent event) {
    // 【步骤1】处理分片启动
    for (ShardRouting shardRouting : event.state().getRoutingNodes().shardsWithState(INITIALIZING)) {
        if (shardRouting.currentNodeId().equals(event.state().nodes().getLocalNodeId())) {
            // 启动分配给本节点的分片
            startShard(shardRouting);
        }
    }

    // 【步骤2】处理分片停止
    for (ShardRouting shardRouting : event.previousState().getRoutingNodes().shardsWithState(STARTED)) {
        if (event.state().getRoutingNodes().node(shardRouting.currentNodeId()) == null) {
            // 停止不再分配给本节点的分片
            stopShard(shardRouting);
        }
    }
}
```

### 4.4 集群服务就绪的判断标准

集群被认为"就绪"需要满足以下条件:

#### 1. 集群状态已恢复

```java
// GatewayService.java
public void clusterChanged(ClusterChangedEvent event) {
    if (event.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
        // 【未就绪】集群状态尚未从磁盘恢复
        return;
    }

    // 【已就绪】可以开始处理请求
}
```

**STATE_NOT_RECOVERED_BLOCK**会在以下情况被移除:
- Leader节点: 成功发布第一个集群状态后
- Follower节点: 成功应用第一个集群状态后

#### 2. 主节点已选出

```java
// NoMasterBlockService.java
public ClusterBlock getNoMasterBlock() {
    if (clusterState.nodes().getMasterNodeId() != null) {
        // 【已就绪】有主节点
        return null;
    } else {
        // 【未就绪】没有主节点
        return NO_MASTER_BLOCK;
    }
}
```

#### 3. 本地节点已加入集群

```java
// DiscoveryNodes.java
public boolean isLocalNodeElectedMaster() {
    return localNode.equals(masterNode);
}

public boolean nodeExists(DiscoveryNode node) {
    return nodes.containsKey(node.getId());
}
```

### 4.5 开始处理客户端请求

当集群就绪后,各个服务开始接受客户端请求:

#### 索引请求处理

```java
// TransportBulkAction.java
protected void doExecute(Task task, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
    // 【检查1】集群状态是否已恢复
    if (clusterService.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK)) {
        listener.onFailure(new ClusterBlockException(STATE_NOT_RECOVERED_BLOCK));
        return;
    }

    // 【检查2】是否有主节点
    if (clusterService.state().blocks().hasGlobalBlock(NO_MASTER_BLOCK_ID)) {
        listener.onFailure(new ClusterBlockException(NO_MASTER_BLOCK));
        return;
    }

    // 【处理请求】
    executeBulk(task, bulkRequest, listener);
}
```

#### 搜索请求处理

```java
// TransportSearchAction.java
protected void doExecute(Task task, SearchRequest searchRequest, ActionListener<SearchResponse> listener) {
    // 【检查】集群状态
    ClusterState clusterState = clusterService.state();
    if (clusterState.blocks().hasGlobalBlockWithLevel(ClusterBlockLevel.READ)) {
        listener.onFailure(new ClusterBlockException(clusterState.blocks().global()));
        return;
    }

    // 【处理请求】
    executeSearch(task, searchRequest, listener);
}
```

### 4.6 集群就绪的完整时序

```
时间 │ Coordinator │ ClusterApplier │ IndicesService │ TransportService
─────┼─────────────┼────────────────┼────────────────┼──────────────────
T1   │ becomeLeader()
     │     │
     │     └──> publish(clusterState)
─────┼─────────────┼────────────────┼────────────────┼──────────────────
T2   │ handleApplyCommit()
     │     │
     │     └──> onNewClusterState()
     │              │
     │              └──> applyChanges()
─────┼─────────────┼────────────────┼────────────────┼──────────────────
T3   │              │ clusterChanged()
     │              │     │
     │              │     ├──> deleteIndices()
     │              │     ├──> createIndices()
     │              │     ├──> updateIndices()
     │              │     └──> applyShardAllocation()
─────┼─────────────┼────────────────┼────────────────┼──────────────────
T4   │              │                │ startShard()
     │              │                │     │
     │              │                │     ├──> createShard()
     │              │                │     ├──> recoverShard()
     │              │                │     └──> markShardAsStarted()
─────┼─────────────┼────────────────┼────────────────┼──────────────────
T5   │              │                │                │ acceptInboundMessage()
     │              │                │                │     │
     │              │                │                │     └──> 开始接受请求
─────┼─────────────┼────────────────┼────────────────┼──────────────────
T6   │ 集群完全就绪 ✓
     │ - 主节点已选出
     │ - 集群状态已恢复
     │ - 分片已分配
     │ - 服务已启动
```

## 五、总结

### 5.1 关键设计模式

1. **状态机模式**: Coordinator的Mode(CANDIDATE/LEADER/FOLLOWER)
2. **策略模式**: ElectionStrategy定义选举策略
3. **观察者模式**: ClusterStateListener监听集群状态变化
4. **模板方法模式**: JoinAccumulator的不同实现
5. **两阶段提交**: 集群状态发布的Publish和Commit阶段

### 5.2 核心机制

1. **节点发现**: 通过PeerFinder实现传播式发现
2. **集群引导**: 通过ClusterBootstrapService实现首次启动
3. **预投票**: 通过PreVoteCollector防止不必要的选举
4. **选举**: 基于Raft的选举算法
5. **状态发布**: 两阶段提交确保一致性
6. **心跳检测**: Leader和Follower互相监控

### 5.3 容错机制

1. **多数派原则**: 选举和提交都需要多数节点同意
2. **任期机制**: 防止旧Leader干扰
3. **版本检查**: 确保状态单调递增
4. **故障检测**: 及时发现节点故障
5. **自动重试**: 选举失败后自动重试

### 5.4 性能优化

1. **批量处理**: JoinAccumulator批量处理加入请求
2. **异步处理**: 大量使用ActionListener实现异步
3. **连接复用**: 节点间连接复用
4. **状态缓存**: 避免重复计算
5. **并发控制**: 使用mutex保护关键状态

这份详细的代码分析应该能帮助你深入理解Elasticsearch集群形成的完整过程。
