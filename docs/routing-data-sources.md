# LND 寻路数据来源深度分析文档

> 生成时间: 2026年5月6日
> 项目: Lightning Terminal (LiT) / LND v0.20.1

---

## 目录

1. [概述](#1-概述)
2. [通道图数据](#2-通道图数据)
3. [网络拓扑信息收集](#3-网络拓扑信息收集)
4. [本地通道状态管理](#4-本地通道状态管理)
5. [带宽管理器](#5-带宽管理器)
6. [数据持久化与迁移](#6-数据持久化与迁移)
7. [寻路数据流总览](#7-寻路数据流总览)
8. [相关文件索引](#8-相关文件索引)

---

## 1. 概述

寻路算法依赖于多种数据来源，这些数据共同构建了 Lightning Network 的拓扑视图。LND 使用分层架构来组织和管理这些数据：

```
┌──────────────────────────────────────────────────────────────────┐
│                        数据来源层次结构                            │
└──────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│                      实时数据 (运行时)                            │
├──────────────────────────────────────────────────────────────────┤
│  HTLC Switch    │  本地通道状态  │  MissionControl 历史记录  │
│  (带宽/流动性)   │  (开/闭状态)    │  (成功/失败统计)        │
└──────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌──────────────────────────────────────────────────────────────────┐
│                      拓扑数据 (来自网络)                          │
├──────────────────────────────────────────────────────────────────┤
│  Gossip 协议  │  Channel Update  │  Node Announcement        │
│  (通道公告)    │  (策略更新)       │  (节点公告)               │
└──────────────────────────────────────────────────────────────────┘
                              ▲
                              │
┌──────────────────────────────────────────────────────────────────┐
│                      持久化数据 (数据库)                          │
├──────────────────────────────────────────────────────────────────┤
│  ChannelEdgeInfo │  ChannelEdgePolicy  │  Node                │
│  (通道信息)       │  (有向策略)         │  (节点信息)           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. 通道图数据

### 2.1 存储架构

LND 使用两层存储架构来管理通道图数据：

#### KV 存储层 (`graph/db/kv_store.go`)

```go
// KVStore 是图数据的底层持久化存储
type KVStore struct {
    db           kvdb.Backend           // 键值数据库后端
    rejectCache  *rejectCache          // 拒绝缓存
    chanCache    *channelCache         // 通道缓存
    chanScheduler batch.Scheduler[kvdb.RwTx]  // 通道批处理调度器
    nodeScheduler batch.Scheduler[kvdb.RwTx]  // 节点批处理调度器
}
```

#### 缓存层 (`graph/db/graph_cache.go`)

```go
// GraphCache 是内存中的图数据缓存，加速遍历操作
type GraphCache struct {
    // 节点到其通道的映射
    nodeChannels map[route.Vertex]map[uint64]*DirectedChannel

    // 节点特性向量缓存
    nodeFeatures map[route.Vertex]*lnwire.FeatureVector

    mtx          sync.RWMutex
}
```

### 2.2 数据库桶结构

| 桶名 | 用途 | 键结构 |
|------|------|--------|
| `nodeBucket` | 节点公钥到节点信息的映射 | `pubKey -> Node` |
| `edgeBucket` | 通道策略存储 | `pubKey \|\| chanID -> ChannelEdgePolicy` |
| `edgeIndexBucket` | 通道ID到完整通道信息的索引 | `chanID -> ChannelEdgeInfo` |
| `zombieBucket` | 追踪"僵尸"通道 | `chanID -> timestamp` |
| `pruneLogBucket` | 记录已修剪区块的日志 | `blockHeight -> blockHash` |
| `closedScidBucket` | 已关闭通道的SCID索引 | `chanID -> metadata` |

### 2.3 节点数据 (`graph/db/models/node.go`)

```go
// Node 代表图中的一个节点(闪电节点)
type Node struct {
    // 节点公钥 (33字节压缩格式)
    PubKeyBytes [33]byte

    // 是否收到过节点公告
    HaveNodeAnnouncement bool

    // 最后更新时间
    LastUpdate time.Time

    // TCP 地址列表
    Addresses []net.Addr

    // 节点颜色 (用于可视化)
    Color color.RGBA

    // 节点别名
    Alias string

    // 节点公告签名
    AuthSigBytes []byte

    // 协议特性向量
    Features *lnwire.FeatureVector

    // 扩展不透明数据
    ExtraOpaqueData []byte
}
```

### 2.4 通道信息 (`graph/db/models/channel_edge_info.go`)

```go
// ChannelEdgeInfo 代表通道的静态信息(不随策略变化)
type ChannelEdgeInfo struct {
    // 唯一通道标识符
    // 格式: 区块高度(3字节) || 交易索引(3字节) || 输出索引(2字节)
    ChannelID uint64

    // 所属链的哈希
    ChainHash chainhash.Hash

    // 两个节点的压缩公钥
    NodeKey1Bytes [33]byte
    NodeKey2Bytes [33]byte

    // 两个节点的 BTC 多签密钥
    BitcoinKey1Bytes [33]byte
    BitcoinKey2Bytes [33]byte

    // 通道特性
    Features *lnwire.FeatureVector

    // 认证证明 (两个节点 + 两个 BTC 密钥的签名)
    AuthProof *ChannelAuthProof

    // 通道创建交易的输出点
    ChannelPoint wire.OutPoint

    // 通道总容量 (satoshi)
    Capacity btcutil.Amount
}
```

### 2.5 通道策略 (`graph/db/models/channel_edge_policy.go`)

```go
// ChannelEdgePolicy 代表通道的转发策略(有向边)
type ChannelEdgePolicy struct {
    // 签名
    SigBytes []byte

    // 通道ID
    ChannelID uint64

    // 最后更新时间
    LastUpdate time.Time

    // 消息标志
    MessageFlags lnwire.ChanUpdateMsgFlags

    // 通道方向标志
    // 0 = 从低公钥节点到高公钥节点
    // 1 = 从高公钥节点到低公钥节点
    ChannelFlags lnwire.ChanUpdateChanFlags

    // 时间锁增量
    // 转发支付时需要满足的最小 CLTV 时间差
    TimeLockDelta uint16

    // 最小 HTLC 值
    MinHTLC lnwire.MilliSatoshi

    // 最大 HTLC 值
    MaxHTLC lnwire.MilliSatoshi

    // 基础费率 (每笔转发收取的固定费用)
    FeeBaseMSat lnwire.MilliSatoshi

    // 比例费率 (每百万 sat 收取的费用)
    // 实际费率 = FeeProportionalMillionths / 1,000,000
    FeeProportionalMillionths lnwire.MilliSatoshi

    // 目标节点公钥
    ToNode [33]byte

    // 入站费用 (可选)
    InboundFee fn.Option[lnwire.Fee]
}
```

### 2.6 有向通道结构

```go
// DirectedChannel 组合了通道信息和两个方向的策略
type DirectedChannel struct {
    // 通道静态信息
    EdgeInfo *ChannelEdgeInfo

    // 两个方向的策略 (可能是 nil 表示未设置)
    // 0 方向策略 (低 -> 高)
    Policy1 *ChannelEdgePolicy

    // 1 方向策略 (高 -> 低)
    Policy2 *ChannelEdgePolicy
}
```

**关键设计点**: 每个物理通道存储为**两条有向边**，分别代表两个方向的转发策略。这使得路径查找可以从任意方向遍历图。

---

## 3. 网络拓扑信息收集

### 3.1 Gossip 协议概述

LND 通过 Gossip 协议从网络中收集拓扑信息。

**位置**: `lib/lndv0.20.1/discovery/gossiper.go`

```go
// Config 是 Gossip 配置
type Config struct {
    Graph          graph.ChannelGraphSource  // 通道图源
    ChainIO        lnwallet.BlockChainIO    // 区块链查询接口
    ChanSeries    ChannelGraphTimeSeries    // 通道时间序列
    Notifier      chainntnfs.ChainNotifier // 区块通知器
    Broadcast     func(...) error          // 广播函数
    AnnSigner     lnwallet.MessageSigner   // 公告签名器
}
```

### 3.2 公告类型处理

#### 3.2.1 Channel Announcement (通道公告)

通道公告由两个节点共同签名，声明一条新通道的存在。

**处理流程**:

```
┌──────────────────────────────────────────────────────────────────┐
│              Channel Announcement 处理流程                          │
└──────────────────────────────────────────────────────────────────┘

1. 接收公告
        │
        ▼
2. 签名验证
   - 验证两个节点签名
   - 验证两个 BTC 密钥签名
        │
        ▼
3. 区块链确认检查
   - 验证 funding tx 在区块链中
   - 检查确认数量
        │
        ▼
4. 添加到图数据库
   - 存储 ChannelEdgeInfo
   - 更新索引
        │
        ▼
5. 监控关闭
   - 添加到链视图过滤器
```

**代码实现** (`graph/builder.go`):

```go
func (b *Builder) addEdge(ctx context.Context,
    edge *models.ChannelEdgeInfo) error {

    // 1. 检查通道是否已存在
    _, _, exists, isZombie, err := b.cfg.Graph.HasChannelEdge(edge.ChannelID)
    if err != nil {
        return fmt.Errorf("failed checking for edge: %w", err)
    }

    // 2. 添加到图数据库
    if err := b.cfg.Graph.AddChannelEdge(ctx, edge); err != nil {
        return fmt.Errorf("unable to add edge: %w", err)
    }

    // 3. 如果不是预确认通道，添加到链视图监控
    if !b.cfg.AssumeChannelValid {
        // 获取 funding pk script 用于监控
        fundingPkScript, err := input.GenFundingPkScript(
            edge.BitcoinKey1Bytes[:], edge.BitcoinKey2Bytes[:],
            int64(edge.Capacity),
        )

        filterUpdate := []graphdb.EdgePoint{
            {
                FundingPkScript: fundingPkScript,
                OutPoint:        edge.ChannelPoint,
            },
        }

        // 更新链视图过滤器
        err = b.cfg.ChainView.UpdateFilter(
            filterUpdate, b.bestHeight.Load(),
        )
        if err != nil {
            return fmt.Errorf("failed to update filter: %w", err)
        }
    }

    return nil
}
```

#### 3.2.2 Channel Update (通道更新)

通道更新由单个节点发布，用于更新转发策略。

**处理的更新类型** (`discovery/netann.go`):

```go
// ApplyChannelUpdate 应用通道更新到图
func (b *Builder) ApplyChannelUpdate(msg *lnwire.ChannelUpdate1) bool {
    // 1. 获取通道信息
    ch, _, _, err := b.GetChannelByID(msg.ShortChannelID)
    if err != nil {
        log.Tracef("Unable to find channel %v: %v",
            msg.ShortChannelID, err)
        return false
    }

    // 2. 确定是哪个方向的策略
    isFirst := ch.NodeKey1Bytes == msg.NewScidFlags.NodeID ||
               bytes.Equal(ch.NodeKey1Bytes[:], msg.RawSigBytes[:33])

    // 3. 验证签名
    err = netann.ValidateChannelUpdateAnn(pubKey, ch.Capacity, msg)
    if err != nil {
        log.Tracef("Invalid signature for channel update: %v", err)
        return false
    }

    // 4. 构建策略
    update := &models.ChannelEdgePolicy{
        ChannelID:                   msg.ShortChannelID.ToUint64(),
        LastUpdate:                  time.Unix(int64(msg.Timestamp), 0),
        TimeLockDelta:              msg.TimeLockDelta,
        MinHTLC:                    msg.HtlcMinimumMsat,
        MaxHTLC:                    msg.HtlcMaximumMsat,
        FeeBaseMSat:                lnwire.MilliSatoshi(msg.BaseFee),
        FeeProportionalMillionths:   lnwire.MilliSatoshi(msg.FeeRate),
        MessageFlags:                msg.MessageFlags,
        ChannelFlags:               msg.ChannelFlags,
        ToNode:                     toNode,
        // ...
    }

    // 5. 更新策略
    err = b.UpdateEdgePolicy(update, isFirst)
    return err == nil
}
```

#### 3.2.3 Node Announcement (节点公告)

节点公告由节点自己签名，包含其网络地址和特性。

```go
func (b *Builder) addNode(ctx context.Context,
    node *models.Node) error {

    // 1. 验证新鲜度 (防止重放攻击)
    err := b.assertNodeAnnFreshness(ctx, node.PubKeyBytes, node.LastUpdate)
    if err != nil {
        return err
    }

    // 2. 添加到数据库
    if err := b.cfg.Graph.AddNode(ctx, node); err != nil {
        return fmt.Errorf("unable to add node %x to the graph: %w",
            node.PubKeyBytes, err)
    }

    return nil
}
```

### 3.3 僵尸通道检测

```go
// 默认通道过期时间
const DefaultChannelPruneExpiry = 14 * 24 * time.Hour  // 14天

func (b *Builder) isZombieChannel(e1, e2 *models.ChannelEdgePolicy) (
    bool, bool, bool) {

    chanExpiry := b.cfg.ChannelPruneExpiry

    // 如果通道超过指定时间未更新，标记为僵尸
    e1Zombie := e1 == nil || time.Since(e1.LastUpdate) >= chanExpiry
    e2Zombie := e2 == nil || time.Since(e2.LastUpdate) >= chanExpiry

    return e1Zombie, e2Zombie, b.IsZombieChannel(e1Time, e2Time)
}
```

### 3.4 Gossip 同步流程

```
┌──────────────────────────────────────────────────────────────────┐
│                    Gossip 同步流程                                │
└──────────────────────────────────────────────────────────────────┘

                    ┌─────────────────┐
                    │   启动同步       │
                    └────────┬────────┘
                             │
                             ▼
               ┌───────────────────────────────┐
               │ 1. 发送 GossipQuery           │
               │    (询问对方知道的通道)         │
               └───────────────┬───────────────┘
                               │
               ┌───────────────▼───────────────┐
               │ 2. 接收 GossipBatch           │
               │    (批量通道公告/更新)         │
               └───────────────┬───────────────┘
                               │
                               ▼
               ┌───────────────────────────────┐
               │ 3. 验证签名                    │
               │    - ChannelAuthProof          │
               │    - Node Announcement Sig     │
               └───────────────┬───────────────┘
                               │
                               ▼
               ┌───────────────────────────────┐
               │ 4. 区块链确认检查              │
               │    (对于通道公告)             │
               └───────────────┬───────────────┘
                               │
                               ▼
               ┌───────────────────────────────┐
               │ 5. 添加/更新图数据             │
               │    - 新通道: AddChannelEdge   │
               │    - 新策略: UpdateEdgePolicy │
               │    - 新节点: AddNode          │
               └───────────────┬───────────────┘
                               │
                               ▼
                    ┌──────────────────┐
                    │   同步完成        │
                    └──────────────────┘
```

---

## 4. 本地通道状态管理

### 4.1 Manager 架构 (`routing/localchans/manager.go`)

```go
// Manager 管理本地通道的状态和策略
type Manager struct {
    // 本节点公钥
    SelfPub *btcec.PublicKey

    // 默认路由策略
    DefaultRoutingPolicy models.ForwardingPolicy

    // 更新链接策略回调
    UpdateForwardingPolicies func(...) error

    // 广播策略更新回调
    PropagateChanPolicyUpdate func(...) error

    // 遍历所有出站通道回调
    ForAllOutgoingChannels func(...) error

    // 查询通道回调
    FetchChannel func(...) (*lnwallet.channel, error)

    // 添加边回调
    AddEdge func(...) error

    // 锁
    policyUpdateLock sync.Mutex
}
```

### 4.2 策略更新流程

```go
func (r *Manager) UpdatePolicy(ctx context.Context,
    newSchema routing.ChannelPolicy,
    createMissingEdge bool,
    chanPoints ...wire.OutPoint) ([]*lnrpc.FailedUpdate, error) {

    // 1. 锁定防止并发更新
    r.policyUpdateLock.Lock()
    defer r.policyUpdateLock.Unlock()

    // 2. 构建要更新的策略映射
    policiesToUpdate := make(map[wire.OutPoint]*models.ChannelEdgePolicy)
    edgesToUpdate := make(map[uint64]*models.ChannelEdgePolicy)

    // 3. 遍历所有出站通道
    err := r.ForAllOutgoingChannels(ctx, func(link *htlcswitch.ChannelLink,
        edge *models.ChannelEdgePolicy) error {

        // 如果指定了特定通道，跳过其他
        if len(chanPoints) > 0 && !contains(chanPoints, link.ChannelPoint()) {
            return nil
        }

        // 创建新策略
        newPolicy := &models.ChannelEdgePolicy{
            ChannelID: edge.ChannelID,
            TimeLockDelta: newSchema.TimeLockDelta,
            MinHTLC: newSchema.MinHtlc,
            MaxHTLC: newSchema.MaxHtlc,
            FeeBaseMSat: newSchema.BaseFee,
            FeeProportionalMillionths: newSchema.FeeRate,
            // ...
        }

        policiesToUpdate[link.ChannelPoint()] = newPolicy
        edgesToUpdate[newPolicy.ChannelID] = newPolicy

        return nil
    }, createMissingEdge)

    if err != nil {
        return nil, err
    }

    // 4. 持久化策略
    err = r.PropagateChanPolicyUpdate(edgesToUpdate)
    if err != nil {
        return nil, err
    }

    // 5. 更新活跃链接
    r.UpdateForwardingPolicies(policiesToUpdate)

    return nil, nil
}
```

### 4.3 通道关闭处理 (`graph/builder.go`)

```go
func (b *Builder) updateGraphWithClosedChannels(
    chainUpdate *chainview.FilteredBlock) error {

    // 1. 收集所有被花费的输出
    var spentOutputs []*wire.OutPoint
    for _, tx := range chainUpdate.Transactions {
        for _, txIn := range tx.TxIn {
            spentOutputs = append(spentOutputs,
                &txIn.PreviousOutPoint)
        }
    }

    // 2. 如果没有输出花费，跳过
    if len(spentOutputs) == 0 {
        return nil
    }

    // 3. 从图中修剪已关闭通道
    chansClosed, err := b.cfg.Graph.PruneGraph(
        spentOutputs, &chainUpdate.Hash, chainUpdate.Height)
    if err != nil {
        return fmt.Errorf("unable to prune closed channels: %w", err)
    }

    // 4. 记录已关闭的 SCID
    for _, chanID := range chansClosed {
        err := b.cfg.Graph.AddClosedChannelID(chanID)
        if err != nil {
            log.Errorf("Unable to record closed channel %v: %v",
                chanID, err)
        }
    }

    log.Debugf("Pruned %v channels from graph", len(chansClosed))
    return nil
}
```

### 4.4 通道状态转换

```
┌──────────────────────────────────────────────────────────────────┐
│                    通道状态转换图                                  │
└──────────────────────────────────────────────────────────────────┘

                    ┌─────────────────┐
                    │   PendingOpen    │
                    │   (等待开放)      │
                    └────────┬────────┘
                             │ funding tx 确认
                             ▼
                    ┌─────────────────┐
                    │     Open        │◄──────────────────┐
                    │    (开放)       │                   │
                    └────────┬────────┘                   │
                             │                             │
               ┌─────────────┼─────────────┐               │
               │             │             │               │
               ▼             ▼             ▼               │
      ┌───────────┐  ┌───────────┐  ┌───────────┐         │
      │  Active   │  │ Inactive  │  │  Borked   │         │
      │ (活跃)    │  │ (非活跃)  │  │ (已损坏)  │         │
      └─────┬─────┘  └─────┬─────┘  └───────────┘         │
            │              │                              │
            │              └──────────┐                   │
            │                         │                   │
            └────────────┬───────────┴───────────────────┘
                         │
                         │ 任意一方关闭通道
                         ▼
                ┌─────────────────┐
                │    Closing      │
                │    (关闭中)     │
                └────────┬────────┘
                         │ 关闭完成
                         ▼
                ┌─────────────────┐
                │    Closed       │
                │    (已关闭)     │
                └─────────────────┘
```

---

## 5. 带宽管理器

### 5.1 核心实现 (`routing/bandwidth.go`)

```go
// bandwidthManager 跟踪本地通道的可用带宽
type bandwidthManager struct {
    // 查询 HTLC Switch 的函数
    getLink getLinkQuery

    // 本地出站通道的 ShortChannelID 集合
    localChans map[lnwire.ShortChannelID]struct{}

    // 首跳自定义数据 (用于自定义通道)
    firstHopBlob fn.Option[tlv.Blob]

    // 流量整形器 (可选)
    trafficShaper fn.Option[htlcswitch.AuxTrafficShaper]
}

func newBandwidthManager(graph Graph, sourceNode route.Vertex,
    linkQuery getLinkQuery,
    firstHopBlob fn.Option[tlv.Blob],
    trafficShaper fn.Option[htlcswitch.AuxTrafficShaper]) (
    *bandwidthManager, error) {

    manager := &bandwidthManager{
        getLink:        linkQuery,
        localChans:    make(map[lnwire.ShortChannelID]struct{}),
        firstHopBlob:  firstHopBlob,
        trafficShaper: trafficShaper,
    }

    // 从图中收集所有本地出站通道
    err := graph.ForEachNodeDirectedChannel(
        sourceNode,
        // 回调: 收集每个通道
        func(channel *graphdb.DirectedChannel) error {
            shortID := lnwire.NewShortChanIDFromInt(channel.ChannelID)
            manager.localChans[shortID] = struct{}{}
            return nil
        },
        // 重置回调
        func() { clear(manager.localChans) })

    return manager, err
}
```

### 5.2 带宽查询

```go
// getBandwidth 返回指定通道的可用带宽
func (b *bandwidthManager) getBandwidth(cid lnwire.ShortChannelID,
    amount lnwire.MilliSatoshi) (lnwire.MilliSatoshi, error) {

    // 1. 查询 HTLC Switch 获取链路
    link, err := b.getLink(cid)
    if err != nil {
        return 0, err
    }

    // 2. 检查链路是否可用
    if !link.EligibleToForward() {
        return 0, fmt.Errorf("link not eligible to forward")
    }

    // 3. 查询外部流量整形器 (如果有)
    if b.trafficShaper.IsSome() {
        shaper := b.trafficShaper.Must()

        // 获取 AuxBandwidth
        auxBandwidth, ok := link.AuxBandwidth(amount, cid,
            shaper.AuxChanID(), shaper.Policy())

        // 如果返回有效值，使用它
        if ok && auxBandwidth > 0 {
            return auxBandwidth, nil
        }
    }

    // 4. 获取链路带宽
    linkBandwidth := link.Bandwidth()

    // 5. 检查是否可以添加新 HTLC
    if err := link.MayAddOutgoingHtlc(linkBandwidth - amount); err != nil {
        return 0, err
    }

    return linkBandwidth, nil
}
```

### 5.3 带宽管理器接口

```go
// bandwidthHints 是带宽管理器的接口
type bandwidthHints interface {
    // availableChanBandwidth 返回通道的可用带宽
    availableChanBandwidth(cid lnwire.ShortChannelID,
        amount lnwire.MilliSatoshi) (lnwire.MilliSatoshi, bool)

    // isKnownChannel 检查是否是本地通道
    isKnownChannel(cid lnwire.ShortChannelID) bool
}

// availableChanBandwidth 实现
func (b *bandwidthManager) availableChanBandwidth(
    cid lnwire.ShortChannelID,
    amount lnwire.MilliSatoshi) (lnwire.MilliSatoshi, bool) {

    // 检查是否是本地通道
    if _, ok := b.localChans[cid]; !ok {
        return 0, false
    }

    // 获取带宽
    bandwidth, err := b.getBandwidth(cid, amount)
    if err != nil {
        return 0, false
    }

    return bandwidth, true
}
```

### 5.4 HTLC Switch 带宽接口

```go
// ChannelLink 提供带宽信息
type ChannelLink interface {
    // Bandwidth 返回当前可用带宽
    Bandwidth() lnwire.MilliSatoshi

    // MayAddOutgoingHtlc 检查是否可以添加新的外出 HTLC
    MayAddOutgoingHtlc(htlcAmt lnwire.MilliSatoshi) error

    // EligibleToForward 检查是否可以转发
    EligibleToForward() bool

    // AuxBandwidth 获取辅助带宽信息 (用于流量整形)
    AuxBandwidth(amount lnwire.MilliSatoshi, ...) (lnwire.MilliSatoshi, bool)
}
```

---

## 6. 数据持久化与迁移

### 6.1 数据库后端

LND 支持两种数据库后端：

| 后端 | 说明 |
|------|------|
| KVDB (bbolt) | 键值存储，闪电网络原始存储格式 |
| SQL (SQLite/PostgreSQL) | 关系型数据库，支持更复杂查询 |

### 6.2 KV 数据库结构

```
graph/
├── node-bucket/
│   └── {pubkey} -> Node
│
├── edge-bucket/
│   └── {pubkey}||{chanID} -> ChannelEdgePolicy
│
├── edge-index-bucket/
│   └── {chanID} -> ChannelEdgeInfo
│
├── closed-scid-bucket/
│   └── {chanID} -> timestamp
│
├── prune-log-bucket/
│   └── {blockHeight} -> blockHash
│
└── zombie-bucket/
    └── {chanID} -> lastUpdate
```

### 6.3 SQL 迁移 (`graph/db/sql_migration.go`)

LND 正在从 KV 数据库迁移到 SQL 数据库以提高性能：

```go
func MigrateGraphToSQL(ctx context.Context, cfg *SQLStoreConfig,
    kvBackend kvdb.Backend, sqlDB SQLQueries) error {

    // 迁移进度跟踪
    migrationCfg := &migrationConfig{
        BatchSize:    1000,
        ProgressHook: func(stage string, count, total int64) {},
    }

    // 1. 迁移所有节点
    log.Info("Migrating nodes...")
    nodesTotal, err := migrateNodes(ctx, migrationCfg, kvBackend, sqlDB)
    if err != nil {
        return fmt.Errorf("failed migrating nodes: %w", err)
    }

    // 2. 迁移源节点
    log.Info("Migrating source node...")
    if err := migrateSourceNode(ctx, kvBackend, sqlDB); err != nil {
        return fmt.Errorf("failed migrating source node: %w", err)
    }

    // 3. 迁移通道和策略
    log.Info("Migrating channels and policies...")
    chansTotal, err := migrateChannelsAndPolicies(ctx, migrationCfg,
        kvBackend, sqlDB)
    if err != nil {
        return fmt.Errorf("failed migrating channels: %w", err)
    }

    // 4. 迁移修剪日志
    log.Info("Migrating prune log...")
    if err := migratePruneLog(ctx, migrationCfg, kvBackend, sqlDB); err != nil {
        return fmt.Errorf("failed migrating prune log: %w", err)
    }

    // 5. 迁移关闭 SCID 索引
    log.Info("Migrating closed SCID index...")
    if err := migrateClosedSCIDIndex(ctx, migrationCfg, kvBackend, sqlDB); err != nil {
        return fmt.Errorf("failed migrating closed SCID index: %w", err)
    }

    // 6. 迁移僵尸索引
    log.Info("Migrating zombie index...")
    if err := migrateZombieIndex(ctx, migrationCfg, kvBackend, sqlDB); err != nil {
        return fmt.Errorf("failed migrating zombie index: %w", err)
    }

    log.Infof("Migration complete: %d nodes, %d channels",
        nodesTotal, chansTotal)
    return nil
}
```

### 6.4 迁移验证

```go
// validateMigratedChannels 批量验证迁移正确性
func validateMigratedChannels(ctx context.Context, cfg *SQLStoreConfig,
    sqlDB SQLQueries, batch map[int64]*migChanInfo) error {

    // 1. 批量获取通道
    dbChanIDs := make([]int64, 0, len(batch))
    for _, info := range batch {
        dbChanIDs = append(dbChanIDs, info.ChanID)
    }

    rows, err := sqlDB.GetChannelsByIDs(ctx, dbChanIDs)
    if err != nil {
        return fmt.Errorf("failed to fetch channels: %w", err)
    }

    // 2. 批量获取策略
    dbPolicyIDs := make([]int64, 0, len(dbChanIDs)*2)
    for _, id := range dbChanIDs {
        dbPolicyIDs = append(dbPolicyIDs, id*2, id*2+1)
    }

    policyRows, err := sqlDB.GetPoliciesByIDs(ctx, dbPolicyIDs)

    // 3. 对比验证
    for _, row := range rows {
        migInfo := batch[row.ID]

        // 验证通道信息
        if err := sqldb.CompareRecords(row, migInfo.info); err != nil {
            return fmt.Errorf("channel mismatch for ID %d: %w",
                row.ID, err)
        }
    }

    return nil
}
```

### 6.5 MissionControl 持久化

```go
// missionControlStore 持久化 MissionControl 状态
type missionControlStore struct {
    db       kvdb.Backend
    namespace string
}

// Flush 将内存状态刷新到数据库
func (s *missionControlStore) Flush(ctx context.Context,
    state *missionControlState) error {

    return s.db.Update(func(tx kvdb.RwTx) error {
        // 1. 清空旧数据
        mcBucket := tx.ReadWriteBucket(mcResultsKey)
        if err := mcBucket.DeleteAll(); err != nil {
            return err
        }

        // 2. 写入新数据
        for pair, result := range state.pairs {
            key := encodePairKey(pair)
            value := encodePairResult(result)

            if err := mcBucket.Put(key, value); err != nil {
                return err
            }
        }

        return nil
    }, func() {})
}
```

---

## 7. 寻路数据流总览

```
┌──────────────────────────────────────────────────────────────────┐
│                      寻路请求入口 (FindRoute)                     │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  1. BandwidthManager 初始化                                      │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ • 从 GraphCache 收集本地通道                            │ │
│     │ • 建立 ShortChannelID 到带宽的映射                       │ │
│     │ • 设置 HTLC Switch 回调                                 │ │
│     └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  2. 路径搜索 (Dijkstra)                                        │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ 对于每个候选节点:                                        │ │
│     │                                                          │ │
│     │ a) 从 GraphCache 获取通道                                │ │
│     │    nodeChannels[node] -> map[chanID]*DirectedChannel    │ │
│     │                                                          │ │
│     │ b) 查询带宽                                              │ │
│     │    bandwidthHints.availableChanBandwidth(chanID)         │ │
│     │    -> 从 HTLC Switch 获取实时带宽                         │ │
│     │                                                          │ │
│     │ c) 获取策略                                              │ │
│     │    Policy1 / Policy2 (根据方向选择)                       │ │
│     │    -> FeeBaseMSat, FeeRate, TimeLockDelta, MinHTLC...   │ │
│     │                                                          │ │
│     │ d) 计算概率                                              │ │
│     │    MissionControl.GetProbability(from, to, amt, cap)      │ │
│     │    -> 从历史记录计算成功概率                              │ │
│     └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  3. 数据来源汇总                                                │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ 内存缓存 (GraphCache)                                    │ │
│     │ ├── nodeChannels: 节点 → 通道映射                        │ │
│     │ └── nodeFeatures: 节点特性向量                          │ │
│     ├─────────────────────────────────────────────────────────┤ │
│     │ HTLC Switch (实时状态)                                  │ │
│     │ ├── Bandwidth(): 当前可用带宽                            │ │
│     │ └── EligibleToForward(): 通道是否可转发                 │ │
│     ├─────────────────────────────────────────────────────────┤ │
│     │ 图数据库 (GraphDB)                                       │ │
│     │ ├── ChannelEdgeInfo: 通道静态信息                       │ │
│     │ ├── ChannelEdgePolicy: 有向策略                         │ │
│     │ └── Node: 节点信息                                       │ │
│     ├─────────────────────────────────────────────────────────┤ │
│     │ MissionControl (历史)                                    │ │
│     │ └── TimedPairResult: 节点对历史                         │ │
│     └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│  4. 路由输出                                                    │
│     ┌─────────────────────────────────────────────────────────┐ │
│     │ • Route: 包含 Hops 的完整路径                           │ │
│     │ • Probability: 路径成功概率                              │ │
│     │ • TotalFees: 估算总费用                                 │ │
│     └─────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
```

---

## 8. 相关文件索引

### 8.1 核心文件

| 文件 | 说明 |
|------|------|
| `lib/lndv0.20.1/routing/router.go` | ChannelRouter 主文件 |
| `lib/lndv0.20.1/routing/pathfind.go` | 寻路算法实现 |
| `lib/lndv0.20.1/routing/bandwidth.go` | 带宽管理器 |
| `lib/lndv0.20.1/routing/missioncontrol.go` | MissionControl |

### 8.2 图数据相关

| 文件 | 说明 |
|------|------|
| `lib/lndv0.20.1/graph/db/graph.go` | 图数据库接口 |
| `lib/lndv0.20.1/graph/db/kv_store.go` | KV 存储实现 |
| `lib/lndv0.20.1/graph/db/graph_cache.go` | 图缓存 |
| `lib/lndv0.20.1/graph/db/models/channel_edge_info.go` | 通道信息模型 |
| `lib/lndv0.20.1/graph/db/models/channel_edge_policy.go` | 通道策略模型 |
| `lib/lndv0.20.1/graph/db/models/node.go` | 节点模型 |
| `lib/lndv0.20.1/graph/builder.go` | 图构建器 |

### 8.3 Gossip 协议相关

| 文件 | 说明 |
|------|------|
| `lib/lndv0.20.1/discovery/gossiper.go` | Gossip 协议主文件 |
| `lib/lndv0.20.1/discovery/netann.go` | 网络公告处理 |

### 8.4 本地通道管理

| 文件 | 说明 |
|------|------|
| `lib/lndv0.20.1/routing/localchans/manager.go` | 本地通道管理器 |
| `lib/lndv0.20.1/htlcswitch/link.go` | HTLC Switch 链路 |

---

## 附录 A: 数据新鲜度参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultChannelPruneExpiry` | 14 天 | 通道过期时间 |
| `NodePruneDelay` | 1 周 | 节点公告新鲜度要求 |
| `ChannelUpdatePruneInterval` | 15 分钟 | 通道更新检查间隔 |

## 附录 B: 数据库版本历史

| 版本 | 迁移内容 |
|------|----------|
| v1 | 初始 KV 结构 |
| ... | ... |
| v32 | 添加 MissionControl 迁移支持 |
| v33 | Graph SQL 迁移支持 |

---

*文档结束*
