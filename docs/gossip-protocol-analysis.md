# LND Gossip 协议深度分析文档

> 生成时间: 2026年5月6日
> 项目: Lightning Terminal (LiT) / LND v0.20.1

---

## 目录

1. [协议概述](#1-协议概述)
2. [核心组件与架构](#2-核心组件与架构)
3. [消息类型](#3-消息类型)
4. [同步流程](#4-同步流程)
5. [数据验证](#5-数据验证)
6. [隐私保护](#6-隐私保护)
7. [相关文件索引](#7-相关文件索引)

---

## 1. 协议概述

### 1.1 协议用途

Gossip 协议是 Lightning Network 中用于传播网络拓扑信息的核心机制。根据 BOLT#7 规范，节点通过 Gossip 协议分享：

- **通道的存在性和所有权** - 谁与谁有通道
- **通道的路由策略** - 手续费、时间锁、最小/最大 HTLC 等
- **节点的网络地址和元数据** - IP 地址、别名、颜色等

这种去中心化的信息传播方式确保了 Lightning Network 的抗审查性和无需信任的特性。

### 1.2 核心设计目标

```
┌──────────────────────────────────────────────────────────────────┐
│                    Gossip 协议设计目标                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 去中心化                                                     │
│     └── 无需中央权威，所有节点共同维护网络视图                       │
│                                                                  │
│  2. 最终一致性                                                   │
│     └── 消息最终会被所有诚实节点接收                                │
│                                                                  │
│  3. 隐私保护                                                     │
│     └── 支持谣言机制，节点可选择性接收更新                           │
│                                                                  │
│  4. 效率优先                                                     │
│     └── 增量同步、批量广播、压缩编码                               │
│                                                                  │
│  5. 抗审查                                                     │
│     └── 多路径传播，无法轻易屏蔽节点                               │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 1.3 与其他组件的关系

```
┌──────────────────────────────────────────────────────────────────┐
│                      LND 组件关系图                               │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐                                               │
│  │  Authenticated│                                               │
│  │  Gossiper    │◄──────── Gossip 协议核心                       │
│  └──────┬───────┘                                               │
│         │                                                        │
│  ┌──────▼───────┐                                               │
│  │  SyncManager  │◄─────── 管理多个同步器                          │
│  └──────┬───────┘                                               │
│         │                                                        │
│  ┌──────▼───────┐                                               │
│  │ GossipSyncer │◄─────── 每个对等体一个同步器                    │
│  └──────┬───────┘                                               │
│         │                                                        │
│  ┌──────▼───────┐     ┌──────────────┐                         │
│  │  Broadcast() │────►│   lnpeer    │◄─── 底层 P2P 通信        │
│  └──────────────┘     └──────────────┘                         │
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐                         │
│  │  ChannelGraph│◄────│   Router    │◄─── 路由查找               │
│  └──────────────┘     └──────────────┘                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. 核心组件与架构

### 2.1 AuthenticatedGossiper 主结构

**位置**: `lib/lndv0.20.1/discovery/gossiper.go`

`AuthenticatedGossiper` 是 Gossip 系统的核心入口，负责协调所有 Gossip 相关的活动：

```go
// AuthenticatedGossiper 是 Gossip 系统的核心协调器
type AuthenticatedGossiper struct {
    // 配置
    cfg *Config

    // 区块高度跟踪
    blockEpochs       *chainntnfs.BlockEpochEvent
    bestHeight        uint32

    // 消息缓存
    // 尚未关联到通道的通道更新会被缓存
    prematureChannelUpdates *lru.Cache[uint64, *cachedNetworkMsg]

    // 未来消息缓存 (premature check)
    futureMsgs *futureMsgCache

    // 同步管理器
    syncMgr *SyncManager

    // 可靠消息发送器
    reliableSender *reliableSender

    // 验证屏障
    vb *ValidationBarrier

    // 速率限制
    // 每个通道方向每分钟最多 10 次更新
    chanUpdateRateLimiter map[uint64][2]*rate.Limiter

    // 通道策略更新通道
    chanPolicyUpdates chan *chanPolicyUpdateRequest

    // 消息队列
    networkMsgs chan *networkMsg

    // 最近拒绝缓存 (防止重放)
    recentRejects *lru.Cache[rejectCacheKey, *cachedReject]

    // 通道锁 (防止并发更新)
    channelMtx *multimutex.Mutex[uint64]
}
```

### 2.2 配置结构

```go
type Config struct {
    // 通道图源
    Graph graph.ChannelGraphSource

    // 区块链查询
    ChainIO lnwallet.BlockChainIO

    // 通道时间序列
    ChanSeries ChannelGraphTimeSeries

    // 区块通知
    Notifier chainntnfs.ChainNotifier

    // 广播函数 (发送到对等体)
    Broadcast func(ctx context.Context, peer route.Vertex,
        msg ...lnwire.Message) error

    // 公告签名器
    AnnSigner lnwallet.MessageSigner

    // 通道广播范围
    ChannelPruneExpiry time.Duration

    // 重广播间隔
    RebroadcastInterval time.Duration

    // 拒绝缓存大小
   RejectCacheSize int

    // Premature 缓存大小
    PrematureCacheSize int

    // 路由器服务
    RouterService interface {
        ProcessStoredNetworkAtHeight(ctx context.Context, height uint32) error
    }
}
```

### 2.3 消息处理循环

```go
// 核心消息处理循环 (gossiper.go:1465-1624)
func (d *AuthenticatedGossiper) networkHandler(ctx context.Context) {
    var announcements *networkMsgBatch

    for {
        select {
        case <-ctx.Done():
            return

        // 处理通道策略更新请求
        case policyUpdate := <-d.chanPolicyUpdates:
            d.processChanPolicyUpdate(ctx, policyUpdate)

        // 处理收到的网络公告
        case announcement := <-d.networkMsgs:
            switch msg := announcement.msg.(type) {

            // AnnounceSignatures 需要串行处理
            case *lnwire.AnnounceSignatures1:
                d.handleAnnounceSignatures(ctx, announcement)

            default:
                // 其他公告通过验证屏障处理
                // 以并行方式处理独立的公告
                jobID, dependencies := d.vb.InitJobDependencies(msg)

                go d.handleNetworkMessages(ctx, announcement,
                    &announcements, jobID)
            }

        // 周期性批量广播
        case <-trickleTimer.C:
            announcementBatch := announcements.Emit()
            d.splitAndSendAnnBatch(ctx, announcementBatch)
        }
    }
}
```

### 2.4 组件交互图

```
┌──────────────────────────────────────────────────────────────────┐
│                    AuthenticatedGossiper                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌─────────────────┐    ┌─────────────────┐                   │
│  │   SyncManager   │    │reliableSender   │                   │
│  │                 │    │                 │                   │
│  │  管理多个        │    │  离线重连       │                   │
│  │  GossipSyncer   │    │  消息重试       │                   │
│  └────────┬────────┘    └────────┬────────┘                   │
│           │                      │                              │
│  ┌────────▼────────┐    ┌──────▼──────────┐                  │
│  │  GossipSyncer   │    │  MessageStore   │                  │
│  │  (per-peer)    │    │                 │                  │
│  │                 │    │  消息持久化     │                  │
│  │  状态机管理     │    │  去重          │                  │
│  └────────┬────────┘    └─────────────────┘                  │
│           │                                                   │
│  ┌────────▼────────────────────────────────────────┐          │
│  │              ValidationBarrier                   │          │
│  │                                                │          │
│  │  • 并发控制                                    │          │
│  │  • 依赖排序                                    │          │
│  │  • ChannelUpdate 等待 ChannelAnnouncement       │          │
│  └────────────────────────────────────────────────┘          │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
         │                        ▲
         ▼                        │
┌──────────────────┐    ┌─────────┴────────┐
│   ChannelGraph   │    │  Broadcast()     │
│   (GraphDB)     │    │  (lnpeer)       │
└──────────────────┘    └──────────────────┘
```

---

## 3. 消息类型

### 3.1 Channel Announcement (通道公告)

通道公告由两个节点共同签名，声明一条新通道的存在。

**位置**: `lib/lndv0.20.1/lnwire/channel_announcement.go`

```go
type ChannelAnnouncement1 struct {
    // 四重签名确保通道所有权
    // 必须两个节点和两个比特币密钥都签名
    NodeSig1   Sig      // 节点1的签名
    NodeSig2   Sig      // 节点2的签名
    BitcoinSig1 Sig     // 节点1的比特币签名
    BitcoinSig2 Sig     // 节点2的比特币签名

    // 通道特性
    Features *RawFeatureVector

    // 链标识 (比特币主网 / 测试网)
    ChainHash chainhash.Hash

    // 通道短 ID
    // 格式: 区块高度(3字节) || 交易索引(3字节) || 输出索引(2字节)
    ShortChannelID ShortChannelID

    // 两个节点的压缩公钥 (按数值排序)
    // NodeID1 < NodeID2 (按字节序)
    NodeID1   [33]byte
    NodeID2   [33]byte

    // 两个节点的比特币资助脚本公钥
    BitcoinKey1 [33]byte
    BitcoinKey2 [33]byte

    // 扩展数据
    ExtraOpaqueData ExtraOpaqueData
}
```

**验证流程**:

```
┌──────────────────────────────────────────────────────────────────┐
│              Channel Announcement 验证流程                            │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 链哈希验证                                                  │
│     └── msg.ChainHash 必须匹配本地链                              │
│                                                                  │
│  2. Premature 检查                                              │
│     └── 区块高度不能在未来                                       │
│                                                                  │
│  3. 僵尸通道检测                                                │
│     └── 检查是否已标记为僵尸                                      │
│                                                                  │
│  4. 签名验证 (netann.ValidateChannelAnn)                        │
│     ├── 验证 NodeSig1 (使用 NodeID1)                            │
│     ├── 验证 NodeSig2 (使用 NodeID2)                            │
│     ├── 验证 BitcoinSig1 (使用 BitcoinKey1)                     │
│     └── 验证 BitcoinSig2 (使用 BitcoinKey2)                     │
│                                                                  │
│  5. 区块链确认检查                                              │
│     ├── 资助交易必须已在区块链中                                 │
│     ├── 资助输出必须是 2-of-2 多签                              │
│     └── 金额必须匹配                                             │
│                                                                  │
│  6. 添加到通道图                                                │
│     └── AddChannelEdge()                                         │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 3.2 Channel Update (通道更新)

通道更新由单个节点发布，用于更新转发策略。

**位置**: `lib/lndv0.20.1/lnwire/channel_update.go`

```go
type ChannelUpdate1 struct {
    // 签名
    Signature Sig

    // 链标识
    ChainHash chainhash.Hash

    // 通道短 ID
    ShortChannelID ShortChannelID

    // 时间戳 (用于排序和新鲜度检查)
    Timestamp uint32

    // 消息标志
    // bit 0: 是否包含 max_htlc 字段
    MessageFlags ChanUpdateMsgFlags

    // 通道标志
    // bit 0: 方向 (0=NodeID1->NodeID2, 1=反向)
    // bit 1: 禁用标志 (1=通道已禁用)
    ChannelFlags ChanUpdateChanFlags

    // 时间锁增量
    // 转发 HTLC 需要满足的最小 CLTV 时间差
    TimeLockDelta uint16

    // 最小 HTLC 值
    HtlcMinimumMsat MilliSatoshi

    // 基础手续费 (固定费用)
    BaseFee uint32

    // 比例手续费 (每百万 sat)
    // 实际费率 = FeeRate / 1,000,000
    FeeRate uint32

    // 最大 HTLC 值 (MessageFlags 指定是否包含)
    HtlcMaximumMsat MilliSatoshi

    // 入站手续费 (可选)
    InboundFee tlv.OptionalRecordT[tlv.TlvType1, Fee]
}
```

**处理流程**:

```go
func (d *AuthenticatedGossiper) handleChanUpdate(ctx context.Context,
    msg *lnwire.ChannelUpdate1, source route.Vertex) (bool, error) {

    // 1. 链哈希验证
    if msg.ChainHash != d.cfg.ChainIO.GenesisHash() {
        return false, fmt.Errorf("wrong chain")
    }

    // 2. Premature 检查
    if isPremature, err := d.isPrematureMsg(msg); err != nil || isPremature {
        return isPremature, err
    }

    // 3. 时间戳新鲜度检查
    timestamp := time.Unix(int64(msg.Timestamp), 0)
    if time.Since(timestamp) > d.cfg.ChannelPruneExpiry {
        return false, nil // 陈旧更新
    }

    // 4. 僵尸通道复活处理
    if err := d.processZombieUpdate(msg); err != nil {
        log.Debugf("Not a zombie revival: %v", err)
    }

    // 5. 速率限制检查
    if !d.checkRateLimit(msg.ShortChannelID, msg.ChannelFlags) {
        return false, nil // 丢弃
    }

    // 6. 签名验证
    err := netann.ValidateChannelUpdateAnn(pubKey, capacity, msg)
    if err != nil {
        return false, err
    }

    // 7. 更新通道图
    err = d.cfg.Graph.UpdateEdgePolicy(update, isFirst)

    return true, err
}
```

### 3.3 Node Announcement (节点公告)

节点公告由节点自己签名，包含其网络地址和元数据。

**位置**: `lib/lndv0.20.1/lnwire/node_announcement.go`

```go
type NodeAnnouncement1 struct {
    // 签名
    Signature Sig

    // 特性向量
    Features *RawFeatureVector

    // 时间戳 (用于排序)
    Timestamp uint32

    // 节点公钥
    NodeID [33]byte

    // RGB 颜色 (用于地图显示)
    RGBColor color.RGBA

    // 别名 (32字节 UTF-8，用于地图显示)
    Alias NodeAlias

    // 网络地址列表
    // 支持: .onion v2/v3, IPv4, IPv6
    Addresses []net.Addr
}
```

### 3.4 GossipQuery 消息

GossipQuery 用于查询对等体的通道信息。

```go
// 查询区块范围内的通道
// lib/lndv0.20.1/lnwire/query_channel_range.go
type QueryChannelRange struct {
    ChainHash        chainhash.Hash
    FirstBlockHeight uint32      // 查询起始区块
    NumBlocks        uint32      // 查询区块数量
    QueryOptions     *QueryOptions // TLV 选项
}

// 按 ShortChannelID 查询通道
// lib/lndv0.20.1/lnwire/query_short_chan_ids.go
type QueryShortChanIDs struct {
    ChainHash    chainhash.Hash
    EncodingType QueryEncoding   // SortedPlain 或 SortedZlib
    ShortChanIDs []ShortChannelID
}
```

**响应消息**:

```go
// lib/lndv0.20.1/lnwire/reply_channel_range.go
type ReplyChannelRange struct {
    ChainHash        chainhash.Hash
    FirstBlockHeight uint32
    NumBlocks        uint32
    Complete         uint8      // 1=完成, 0=还有更多
    EncodingType     QueryEncoding
    ShortChanIDs     []ShortChannelID
    Timestamps       Timestamps  // 可选的更新时间戳
}
```

### 3.5 GossipTimestampFilter

此消息用于设置接收 Gossip 消息的时间窗口，实现谣言机制。

**位置**: `lib/lndv0.20.1/lnwire/gossip_timestamp_range.go`

```go
type GossipTimestampRange struct {
    ChainHash       chainhash.Hash
    FirstTimestamp  uint32    // 时间窗口起始 (Unix 时间戳)
    TimestampRange  uint32    // 时间窗口范围 (秒)
    FirstBlockHeight tlv.OptionalRecordT[tlv.TlvType0, uint32]
    BlockRange      tlv.OptionalRecordT[tlv.TlvType1, BlockRange]
}

// 示例:
// 只想接收过去 2 小时的更新
GossipTimestampRange{
//     FirstTimestamp:  time.Now().Add(-2*time.Hour).Unix(),
//     TimestampRange:   2*60*60,
// }
```

---

## 4. 同步流程

### 4.1 同步状态机 (GossipSyncer)

`GossipSyncer` 为每个对等体实现了完整的状态机：

**位置**: `lib/lndv0.20.1/discovery/syncer.go`

```go
// syncerState 定义同步状态
type syncerState uint32

const (
    syncingChans syncerState = iota      // 0: 初始同步状态
    waitingQueryRangeReply               // 1: 等待 QueryChannelRange 回复
    queryNewChannels                     // 2: 查询新通道
    waitingQueryChanReply                // 3: 等待 QueryShortChanIDs 回复
    chansSynced                          // 4: 同步完成
    syncerIdle                          // 5: 空闲状态
)
```

**状态转换图**:

```
┌──────────────────────────────────────────────────────────────────┐
│                    GossipSyncer 状态转换图                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐                                           │
│  │   syncingChans   │◄─── 新对等体连接                          │
│  └────────┬─────────┘                                           │
│           │                                                      │
│           │ QueryChannelRange                                    │
│           ▼                                                      │
│  ┌─────────────────────────────┐                                │
│  │  waitingQueryRangeReply      │                                │
│  └─────────────┬───────────────┘                                │
│                │                                                │
│     ReplyChannelRange (流式)                                      │
│                │                                                │
│                ▼                                                │
│  ┌──────────────────┐     ┌──────────────────┐                │
│  │ queryNewChannels │◄─────│ 定期轮换         │                │
│  └────────┬─────────┘     └──────────────────┘                │
│           │                                                      │
│           │ QueryShortChanIDs                                    │
│           ▼                                                      │
│  ┌─────────────────────────┐                                    │
│  │  waitingQueryChanReply  │                                    │
│  └────────────┬────────────┘                                    │
│               │                                                  │
│   ReplyShortChanIDsEnd                                            │
│               │                                                  │
│               └────────────────┐                                 │
│                                │                                 │
│                                ▼                                 │
│                       ┌──────────────────┐                     │
│                       │   chansSynced     │                     │
│                       └────────┬─────────┘                     │
│                                │                                 │
│                      定期轮换   │                                 │
│                                │                                 │
│                       ┌────────▼─────────┐                      │
│                       │  syncerIdle     │                      │
│                       └─────────────────┘                      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 4.2 同步类型

```go
// SyncerType 定义同步器类型
type SyncerType uint8

const (
    ActiveSync SyncerType = iota  // 主动同步: 接收新消息
    PassiveSync                   // 被动同步: 不接收新消息
    PinnedSync                    // 固定同步: 始终主动
)
```

### 4.3 SyncManager 管理器

`SyncManager` 管理多个 `GossipSyncer` 实例，实现主动同步器的轮换：

**位置**: `lib/lndv0.20.1/discovery/sync_manager.go`

```go
type SyncManager struct {
    // 主动同步器 (最多 NumActiveSyncers=50 个)
    activeSyncers map[route.Vertex]*GossipSyncer

    // 被动同步器
    inactiveSyncers map[route.Vertex]*GossipSyncer

    // 固定同步器 (不受数量限制)
    pinnedActiveSyncers map[route.Vertex]*GossipSyncer

    // 同步器轮换定时器
    RotateTicker ticker.Ticker

    // 历史同步定时器
    HistoricalSyncTicker ticker.Ticker

    // 图数据源
    graph graph.Source
}
```

### 4.4 同步器轮换逻辑

```go
// rotateActiveSyncerCandidate 轮换同步器
// 确保从不同对等体获取网络更新
func (s *SyncManager) rotateActiveSyncerCandidate(ctx context.Context) {
    // 1. 找一个主动同步器转为被动
    for pubKey, syncer := range s.activeSyncers {
        if _, ok := s.pinnedActiveSyncers[pubKey]; ok {
            continue // 跳过固定的
        }

        // 转换为被动
        err := syncer.setSyncType(PassiveSync)
        s.inactiveSyncers[pubKey] = syncer
        delete(s.activeSyncers, pubKey)
        break
    }

    // 2. 找一个被动同步器转为主动
    for pubKey, syncer := range s.inactiveSyncers {
        if len(s.activeSyncers) >= NumActiveSyncers {
            break
        }

        // 转换为主动
        err := syncer.setSyncType(ActiveSync)
        s.activeSyncers[pubKey] = syncer
        delete(s.inactiveSyncers, pubKey)
        break
    }
}
```

### 4.5 主动同步流程 (新对等体)

```
┌──────────────────────────────────────────────────────────────────┐
│                    主动同步流程 (新对等体连接)                     │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  连接建立                                                         │
│      │                                                           │
│      ▼                                                           │
│  ┌──────────────────┐                                           │
│  │   syncingChans   │                                           │
│  └────────┬─────────┘                                           │
│           │                                                      │
│           │ 生成 QueryChannelRange                                │
│           │ (从创世区块到当前高度)                                │
│           ▼                                                      │
│  ┌─────────────────────────────┐                                │
│  │  waitingQueryRangeReply      │                                │
│  │                              │                                │
│  │  ┌───────────────────────┐  │                                │
│  │  │  ReplyChannelRange    │  │                                │
│  │  │  (流式接收)           │  │                                │
│  │  │                       │  │                                │
│  │  │  • 过滤陈旧 SCID      │  │                                │
│  │  │  • 缓存新 SCID       │  │                                │
│  │  └───────────────────────┘  │                                │
│  └─────────────┬───────────────┘                                │
│                │                                               │
│                │ ReplyChannelRange.Complete = 1                 │
│                ▼                                               │
│  ┌──────────────────┐                                           │
│  │ queryNewChannels │                                           │
│  │                  │                                           │
│  │  生成 QueryShortChanIDs                                       │
│  │  (分批, 每批最多 10000 个)                                    │
│  └────────┬─────────┘                                           │
│           │                                                      │
│           ▼                                                      │
│  ┌─────────────────────────┐                                    │
│  │  waitingQueryChanReply   │                                    │
│  │                          │                                    │
│  │  ┌───────────────────┐  │                                    │
│  │  │ ReplyShortChanIDs │  │                                    │
│  │  │                   │  │                                    │
│  │  │ • 验证签名        │  │                                    │
│  │  │ • 添加到图        │  │                                    │
│  │  └───────────────────┘  │                                    │
│  └────────────┬────────────┘                                    │
│               │                                                  │
│   ReplyShortChanIDsEnd                                           │
│               │                                                  │
│               ▼                                                  │
│      ┌──────────────────┐                                      │
│      │   chansSynced    │                                      │
│      └──────────────────┘                                      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### 4.6 增量同步

```go

// syncer.go:916-1103
func (g *GossipSyncer) processChanRangeReply() error {
    // 1. 解析收到的 ShortChanIDs
    for _, scid := range msg.ShortChanIDs {
        // 2. 过滤陈旧/未来时间戳
        if g.isStaleOrFuture(scID, timestamp) {
            continue
        }

        // 3. 缓存响应
        g.cachedChanResponses = append(g.cachedChanResponses, scid)
    }

    // 4. 调用 FilterKnownChanIDs 找出真正的新通道
    newChans, err := g.cfg.channelSeries.FilterKnownChanIDs(
        ctx, msg.FirstBlockHeight, msg.NumBlocks, g.cachedChanResponses,
    )

    // 5. 过滤已存在的通道
    newChansToQuery := filterExistingChannels(newChans)

    // 6. 更新待查询列表
    g.newChansToQuery = append(g.newChansToQuery, newChansToQuery...)

    // 7. 进入查询新通道状态
    g.setSyncState(queryNewChannels)

    return nil
}
```

---

## 5. 数据验证

### 5.1 验证屏障 (ValidationBarrier)

`ValidationBarrier` 确保消息按正确依赖顺序验证：

**位置**: `lib/lndv0.20.1/discovery/validation_barrier.go`

```go
type ValidationBarrier struct {
    // 并发控制信号量
    validationSemaphore chan struct{}

    // 任务信息
    jobInfoMap map[string]*jobInfo

    // 任务依赖关系
    // 例如: ChannelUpdate 依赖 ChannelAnnouncement
    jobDependencies map[JobID]fn.Set[JobID]

    // 子任务完成通知通道
    childJobChans map[JobID]chan struct{}

    mu sync.Mutex
}

// 依赖关系:
// ChannelUpdate1 -> ChannelAnnouncement1
// NodeAnnouncement1 -> ChannelAnnouncement1
```

**依赖检查流程**:

```go
func (vb *ValidationBarrier) InitJobDependencies(msg lnwire.Message) (
    JobID, fn.Set[JobID], error) {

    var dependencies fn.Set[JobID]

    switch msg.(type) {
    case *lnwire.ChannelUpdate1:
        // 获取对应的通道公告
        scid := msg.(*lnwire.ChannelUpdate1).ShortChannelID
        depJobID := channelAnnouncementJobID(scid)
        dependencies = fn.NewSet[JobID](depJobID)

    case *lnwire.NodeAnnouncement1:
        // 需要至少一个通道公告
        nodeID := msg.(*lnwire.NodeAnnouncement1).NodeID
        depJobIDs := getChannelAnnouncementJobIDs(nodeID)
        dependencies = depJobIDs

    case *lnwire.ChannelAnnouncement1:
        // 无依赖
        dependencies = nil
    }

    return jobID, dependencies, nil
}
```

### 5.2 签名验证

```go
// netann.ValidateChannelAnn 验证通道公告签名
// lib/lndv0.20.1/discovery/netann/netann.go
func ValidateChannelAnn(ctx context.Context, msg *lnwire.ChannelAnnouncement1,
    graph graph.Source) error {

    // 1. 验证节点签名
    err := validateNodeSig(msg.NodeSig1, msg.NodeID1[:], msg)
    err = validateNodeSig(msg.NodeSig2, msg.NodeID2[:], msg)

    // 2. 验证比特币签名
    // 证明节点控制资助输出的 BTC 密钥
    err = validateBitcoinSig(msg.BitcoinSig1, msg.BitcoinKey1[:], msg)
    err = validateBitcoinSig(msg.BitcoinSig2, msg.BitcoinKey2[:], msg)

    return nil
}
```

```go
// netann.ValidateChannelUpdateAnn 验证通道更新签名
func ValidateChannelUpdateAnn(pubKey *btcec.PublicKey,
    capacity btcutil.Amount, msg *lnwire.ChannelUpdate1) error {

    // 1. 获取对应节点
    // 2. 验证签名
    return validateSig(msg.Signature, pubKey, msg)
}
```

### 5.3 区块链确认检查

```go
// validateFundingTransaction 验证资助交易
// gossiper.go:2848-2935
func (d *AuthenticatedGossiper) validateFundingTransaction(
    chanInfo *models.ChannelEdgeInfo) error {

    // 1. 获取资助交易
    fundingTx, err := d.cfg.ChainIO.GetTransaction(chanInfo.ChannelPoint.Hash)
    if err != nil {
        return fmt.Errorf("funding tx not found: %w", err)
    }

    // 2. 验证输出存在
    fundingOutput := fundingTx.TxOut[chanInfo.ChannelPoint.Index]
    if fundingOutput.Value != int64(chanInfo.Capacity) {
        return fmt.Errorf("capacity mismatch")
    }

    // 3. 验证是 2-of-2 多签
    class, _, _, err := txscript.ExtractPkScriptAddrs(
        fundingOutput.PkScript, d.activeNetParams.Params)
    if class != txscript.MultiSigTy {
        return fmt.Errorf("not multisig")
    }

    // 4. 验证双方签名密钥
    // ...
}
```

### 5.4 新鲜度检查

```go
// 通道更新新鲜度
// syncer.go:922-930
isStale := func(timestamp time.Time) bool {
    return time.Since(timestamp) > DefaultChannelPruneExpiry
}

isFuture := func(timestamp time.Time) bool {
    return time.Until(timestamp) > DefaultChannelPruneExpiry
}

// 节点公告新鲜度
// gossiper.go:2206-2258
func (d *AuthenticatedGossiper) isStaleNodeAnn(nodeID route.Vertex,
    timestamp uint32) bool {

    lastUpdate, _, err := d.cfg.Graph.GetNode(nodeID)
    if err != nil || lastUpdate == nil {
        return true
    }

    return timestamp <= lastUpdate
}
```

### 5.5 Premature 消息处理

当消息涉及尚未确认的区块时，会被缓存直到区块确认：

```go
// isPremature 检查消息是否 premature
// gossiper.go:2178-2233
func (d *AuthenticatedGossiper) isPrematureMsg(msg lnwire.Message) (
    bool, error) {

    // 获取消息涉及的区块高度
    var msgHeight uint32
    switch m := msg.(type) {
    case *lnwire.ChannelAnnouncement1:
        msgHeight = m.ShortChannelID.BlockHeight

    case *lnwire.ChannelUpdate1:
        // 需要查询数据库获取通道公告的区块高度
        scid := m.ShortChannelID.ToUint64()
        chanInfo, _, _, err := d.cfg.Graph.HasChannelEdge(scid)
        if err != nil {
            return false, err
        }
        msgHeight = lnwire.NewShortChanIDFromUint64(scid).BlockHeight
    }

    // 如果区块高度在未来，缓存
    if msgHeight > d.bestHeight {
        d.futureMsgs.Put(nextMsgID, cachedMsg)
        return true, nil
    }

    return false, nil
}
```

---

## 6. 隐私保护

### 6.1 谣言机制 (Gossip Filtering)

GossipTimestampFilter 实现谣言机制，允许节点选择性接收更新：

**位置**: `lib/lndv0.20.1/discovery/syncer.go`

```go
// ApplyGossipFilter 设置谣言过滤器
func (g *GossipSyncer) ApplyGossipFilter(ctx context.Context,
    filter *lnwire.GossipTimestampRange) error {

    // 1. 保存过滤器参数
    g.filterStartTime = filter.FirstTimestamp
    g.filterEndTime = filter.FirstTimestamp + filter.TimestampRange

    // 2. 异步发送匹配的更新
    go g.sendMatchingUpdates(ctx, g.filterStartTime, g.filterEndTime)

    return nil
}

// sendMatchingUpdates 发送匹配过滤器的更新
func (g *GossipSyncer) sendMatchingUpdates(ctx context.Context,
    startTime, endTime time.Time) {

    // 从时间序列获取匹配的更新
    iter, err := g.cfg.ChanSeries.UpdatesInHorizon(startTime, endTime)
    if err != nil {
        return
    }

    defer iter.Close()

    for iter.Next() {
        msg := iter.Message()
        g.sendToPeerSync(ctx, msg)
    }
}
```

### 6.2 重放保护

**Recent Rejects Cache** 防止处理已被拒绝的消息：

```go
// gossiper.go:1786-1804
func (d *AuthenticatedGossiper) isRecentlyRejectedMsg(scid uint64,
    peer route.Vertex) bool {

    key := rejectCacheKey{scid: scid, peer: peer}
    _, err := d.recentRejects.Get(key)
    return err == nil // 如果存在则已拒绝
}

// 添加到拒绝缓存
func (d *AuthenticatedGossiper) addToRecentRejects(msg *lnwire.Message) {
    // ...
    d.recentRejects.Add(key, struct{}{})
}
```

### 6.3 速率限制

```go
// 通道更新速率限制
// 每个通道方向每分钟最多 10 次更新
// gossiper.go:3363-3409

type chanUpdateRateLimiter struct {
    limiters map[uint64][2]*rate.Limiter  // [scid][direction]
}

func (r *chanUpdateRateLimiter) Allow(scid uint64, direction uint8) bool {
    limiter := r.limiters[scid][direction]
    return limiter.Allow()
}

// 使用
if !d.chanUpdateRateLimiter.Allow(msg.ShortChannelID, direction) {
    return false, nil // 丢弃
}
```

### 6.4 Keep-Alive 更新处理

Keep-alive 更新用于保持通道活跃，防止被标记为僵尸：

```go
// gossiper.go:3368-3376
if IsKeepAliveUpdate(msg, edgeToUpdate) {
    // keep-alive 更新必须间隔一天才能传播
    keepAliveInterval := 24 * time.Hour

    if timeSinceLastUpdate < keepAliveInterval {
        return false, nil // 丢弃
    }
}
```

### 6.5 僵尸通道复活

只有当更新来自有效节点时，僵尸通道才能复活：

```go
// syncer.go:2291-2344
func (d *AuthenticatedGossiper) processZombieUpdate(
    msg *lnwire.ChannelUpdate1) error {

    // 获取通道信息
    chanInfo, _, _, err := d.cfg.Graph.GetChannelByID(scid)
    if err != nil {
        return err
    }

    // 确定签名节点
    var pubKey *btcec.PublicKey
    if isNode1 {
        pubKey = chanInfo.NodeKey1()
    } else {
        pubKey = chanInfo.NodeKey2()
    }

    // 验证签名来自有效节点
    err = netann.VerifyChannelUpdateSignature(msg, pubKey)
    if err != nil {
        return fmt.Errorf("invalid signature: %w", err)
    }

    // 从僵尸索引移除
    return d.cfg.Graph.MarkEdgeLive(scid)
}
```

---

## 7. 相关文件索引

### 7.1 核心实现文件

| 文件路径 | 说明 |
|----------|------|
| `lib/lndv0.20.1/discovery/gossiper.go` | AuthenticatedGossiper 主结构 |
| `lib/lndv0.20.1/discovery/syncer.go` | GossipSyncer 状态机 |
| `lib/lndv0.20.1/discovery/sync_manager.go` | SyncManager 管理器 |
| `lib/lndv0.20.1/discovery/validation_barrier.go` | 验证屏障 |
| `lib/lndv0.20.1/discovery/reliable_sender.go` | 可靠消息发送 |
| `lib/lndv0.20.1/discovery/netann/netann.go` | 网络公告验证 |

### 7.2 消息定义文件

| 文件路径 | 说明 |
|----------|------|
| `lib/lndv0.20.1/lnwire/channel_announcement.go` | 通道公告消息 |
| `lib/lndv0.20.1/lnwire/channel_update.go` | 通道更新消息 |
| `lib/lndv0.20.1/lnwire/node_announcement.go` | 节点公告消息 |
| `lib/lndv0.20.1/lnwire/gossip_timestamp_range.go` | Gossip 时间过滤器 |
| `lib/lndv0.20.1/lnwire/query_channel_range.go` | 通道范围查询 |
| `lib/lndv0.20.1/lnwire/query_short_chan_ids.go` | SCID 查询 |
| `lib/lndv0.20.1/lnwire/reply_channel_range.go` | 通道范围响应 |
| `lib/lndv0.20.1/lnwire/announce_signatures.go` | 签名公告 |

### 7.3 配置常量

```go
// gossiper.go 或 netparams.go
const (
    // 同步器数量限制
    NumActiveSyncers = 50

    // 僵尸通道过期时间
    DefaultChannelPruneExpiry = 14 * 24 * time.Hour

    // 重广播间隔
    DefaultRebroadcastInterval = time.Minute

    // 速率限制
    ChanUpdateRateLimit = 10 // 每分钟每通道方向

    // Keep-alive 最小间隔
    KeepAliveInterval = 24 * time.Hour

    // 查询批处理大小
    QueryBatchSize = 10000
)
```

---

## 附录 A: BOLT#7 规范对照

| 消息类型 | BOLT#7 Section | 说明 |
|----------|-----------------|------|
| `ChannelAnnouncement` | Section 4 | 通道公告 |
| `ChannelUpdate` | Section 4 | 通道更新 |
| `NodeAnnouncement` | Section 4 | 节点公告 |
| `GossipQuery` | Section 6 | Gossip 查询 |
| `ReplyChannelRange` | Section 6 | 通道范围响应 |
| `QueryShortChannelIDs` | Section 6 | SCID 查询 |
| `ReplyShortChannelIDsEnd` | Section 6 | SCID 查询结束 |
| `GossipTimestampFilter` | Section 6 | 时间过滤器 |

---

## 附录 B: 错误处理

| 错误类型 | 处理方式 |
|----------|----------|
| 签名验证失败 | 忽略，添加到拒绝缓存 |
| 区块链确认不足 | 缓存到 prematureMessages |
| 时间戳陈旧 | 忽略 |
| 速率限制触发 | 丢弃消息 |
| 依赖未满足 | 等待依赖完成 |

---

## 附录 C: 消息处理流程图

```
┌──────────────────────────────────────────────────────────────────┐
│                    消息处理总流程                                 │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  接收消息                                                        │
│      │                                                           │
│      ▼                                                           │
│  ┌─────────────┐                                                │
│  │ 解析消息    │                                                │
│  └──────┬──────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────┐     Premature?     ┌──────────────────┐      │
│  │ 获取依赖    │───────────────────►│ FutureMsgCache    │      │
│  └──────┬─────┘                    │ (等待区块确认)    │      │
│         │                          └──────────────────┘      │
│         ▼                                                        │
│  ┌─────────────┐                                                │
│  │ 检查依赖    │                                                │
│  └──────┬─────┘                                                │
│         │                                                        │
│    ┌────┴────┐                                                 │
│    │ 依赖满足?│                                                 │
│    └────┬────┘                                                 │
│      Yes │ No                                                   │
│     ┌────┴────┐                                                │
│     │         │                                                 │
│     ▼         ▼                                                 │
│  ┌──────┐  ┌──────────┐                                        │
│  │ 验证 │  │ 等待依赖  │                                        │
│  │ 屏障 │  │  释放    │                                        │
│  └──┬───┘  └──────────┘                                        │
│     │                                                             │
│     ▼                                                             │
│  ┌─────────────┐                                                │
│  │ 签名验证    │                                                │
│  └──────┬─────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────┐                                                │
│  │ 区块链确认  │                                                │
│  └──────┬─────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────┐                                                │
│  │ 添加到图    │                                                │
│  └──────┬─────┘                                                │
│         │                                                        │
│         ▼                                                        │
│  ┌─────────────┐                                                │
│  │ 广播到其他 │                                                │
│  │ 对等体    │                                                │
│  └─────────────┘                                                │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

---

*文档结束*

---

## 附录 D: Gossip 与节点自身通道的关系

### D.1 通道公告生命周期

Gossip 协议与节点自身通道的创建和公告紧密相关：

```
┌──────────────────────────────────────────────────────────────────┐
│                    通道公告生命周期                                    │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 通道创建 (Funding Flow)                                      │
│     ├── Funding Transaction 广播到 Bitcoin 网络                    │
│     └── 等待 6 个区块确认 (ProofMatureDelta = 6)                │
│            │                                                      │
│            ▼                                                      │
│  2. 签名交换 (AnnounceSignatures)                                │
│     ├── 节点发送 AnnounceSignatures1 到对等体                     │
│     │   包含: ChannelID, ShortChannelID, NodeSignature, BitcoinSig│
│     └── 需要双方交换签名才能构造完整 ChannelAnnouncement           │
│            │                                                      │
│            ▼                                                      │
│  3. 通道公告 (ChannelAnnouncement)                               │
│     ├── 四重签名验证 (两个节点 + 两个 BTC 密钥)                   │
│     ├── 区块链确认检查 (资助交易存在且为 2-of-2 多签)             │
│     └── 添加到 ChannelGraph                                      │
│            │                                                      │
│            ▼                                                      │
│  4. 策略公告 (ChannelUpdate)                                     │
│     ├── 公告路由策略 (手续费、时间锁、HTLC 限制)                  │
│     ├── 初始策略在 funding/manager.go 中定义                     │
│     └── 可随时更新并重新广播                                       │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### D.2 AnnounceSignatures 处理流程

**位置**: `lib/lndv0.20.1/discovery/gossiper.go:3542-3655`

```go
func (d *AuthenticatedGossiper) handleAnnSig(ctx context.Context,
    nMsg *networkMsg, ann *lnwire.AnnounceSignatures1) {

    // 检查确认数是否足够
    needBlockHeight := ann.ShortChannelID.BlockHeight + d.cfg.ProofMatureDelta

    // Premature 检查: 等待足够确认
    premature := d.isPremature(ann.ShortChannelID, d.cfg.ProofMatureDelta, nMsg)
    if premature {
        // 缓存消息直到区块确认
        return
    }

    // 交换签名后构造并广播 ChannelAnnouncement
}
```

### D.3 初始转发策略定义

**位置**: `lib/lndv0.20.1/funding/manager.go`

```go
// 在通道建立时保存初始转发策略
err = f.saveInitialForwardingPolicy(cid.chanID, &forwardingPolicy)

// defaultForwardingPolicy 定义:
forwardingPolicy := f.defaultForwardingPolicy(
    ourContribution.ChannelStateBounds,
)
```

### D.4 本地通道与 Gossip 同步

**位置**: `lib/lndv0.20.1/routing/localchans/manager.go`

```go
// 当本地通道状态变化时，更新图拓扑
AddEdge: func(ctx context.Context, edge *models.ChannelEdgeInfo) error {
    return s.graphBuilder.AddEdge(ctx, edge)
}
```

---

## 附录 E: Gossip 与闪电通道交易的关系

### E.1 组件职责划分

Gossip 协议**仅传播网络拓扑和路由策略**，不参与实际的 HTLC 转账交易：

```
┌──────────────────────────────────────────────────────────────────┐
│                    闪电网络组件职责划分                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Gossip 协议 (只负责拓扑)                                         │
│  ├── ChannelAnnouncement: 通道存在性证明                          │
│  ├── ChannelUpdate: 路由策略 (手续费、时间锁等)                   │
│  └── NodeAnnouncement: 节点元数据                                │
│                                                                  │
│  HTLC Switch (负责实际交易)                                       │
│  ├── 转发 HTLC                                                   │
│  ├── 管理通道状态                                                 │
│  └── ForwardingPolicy 决定是否接受 HTLC                          │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### E.2 ChannelUpdate 与 ForwardingPolicy 映射

**位置**: `lib/lndv0.20.1/peer/brontide.go`

```go
// 从 ChannelUpdate 构建 ForwardingPolicy
if selfPolicy != nil {
    forwardingPolicy = &models.ForwardingPolicy{
        MinHTLCOut:    selfPolicy.MinHTLC,
        MaxHTLC:       selfPolicy.MaxHTLC,
        BaseFee:       selfPolicy.FeeBaseMSat,
        FeeRate:       selfPolicy.FeeRate,
        TimeLockDelta: selfPolicy.TimeLockDelta,
    }
}
```

### E.3 ForwardingPolicy 结构

**位置**: `lib/lndv0.20.1/lnwire/interfaces.go`

```go
type ForwardingPolicy struct {
    TimeLockDelta uint32      // CLTV 时间锁增量
    BaseFee       MilliSatoshi // 固定手续费
    FeeRate       MilliSatoshi // 比例手续费 (每百万 sat)
    MinHTLCOut    MilliSatoshi // 最小 HTLC
    MaxHTLC       MilliSatoshi // 最大 HTLC
    // ...
}
```

### E.4 HTLC Switch 中的策略检查

**位置**: `lib/lndv0.20.1/htlcswitch/link.go`

```go
// canSendHtlc 检查 HTLC 是否满足转发策略
func (l *channelLink) canSendHtlc(policy models.ForwardingPolicy,
    payHash [32]byte, amt lnwire.MilliSatoshi, ...) *LinkError {

    // 检查金额是否在 MinHTLCOut 和 MaxHTLC 之间
    if amt < policy.MinHTLCOut || amt > policy.MaxHTLC {
        return NewLinkError(...)
    }
}
```

### E.5 ShortChannelID 与链上交易关联

```
┌──────────────────────────────────────────────────────────────────┐
│                    ShortChannelID 结构                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ShortChannelID (8 bytes = 64 bits):                            │
│  ┌───────────────┬───────────────┬───────────────┐              │
│  │ Block Height  │ Tx Index      │ Output Index  │              │
│  │ (3 bytes)     │ (3 bytes)    │ (2 bytes)    │              │
│  │ 区块高度       │ 交易索引       │ 输出索引       │              │
│  └───────────────┴───────────────┴───────────────┘              │
│                                                                  │
│  用途:                                                            │
│  • 唯一标识 Bitcoin 链上某个具体的 funding output                 │
│  • Gossip 消息通过 SCID 引用通道                                 │
│  • 路由时通过 SCID 定位通道                                      │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### E.6 区块链确认验证

**位置**: `lib/lndv0.20.1/discovery/gossiper.go`

```go
func (d *AuthenticatedGossiper) validateFundingTransaction(
    chanInfo *models.ChannelEdgeInfo) error {

    // 1. 获取资助交易
    fundingTx, err := d.cfg.ChainIO.GetTransaction(chanInfo.ChannelPoint.Hash)

    // 2. 验证输出存在
    fundingOutput := fundingTx.TxOut[chanInfo.ChannelPoint.Index]

    // 3. 验证是 2-of-2 多签
    class, _, _, err := txscript.ExtractPkScriptAddrs(...)
    if class != txscript.MultiSigTy {
        return fmt.Errorf("not multisig")
    }
}
```

---

## 附录 F: 完整数据流总结

```
┌──────────────────────────────────────────────────────────────────┐
│                    完整数据流                                         │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. 通道创建                                                      │
│     Bitcoin TX ──► funding output ──► ShortChannelID             │
│                                         │                        │
│                                         ▼                        │
│  2. Gossip 公告                                                  │
│     AnnounceSignatures ──► ChannelAnnouncement ──► ChannelGraph │
│                                         │                        │
│                                         ▼                        │
│  3. 策略更新                                                      │
│     ChannelUpdate ──► ForwardingPolicy ──► ChannelLink          │
│                                         │                        │
│                                         ▼                        │
│  4. 路由转发                                                      │
│     Router ──► Gossip 数据 ──► 路径选择 ──► HTLC Switch ──► 转发│
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### F.1 Gossip 与 HTLC Switch 对比

| 方面           | Gossip 协议                  | HTLC Switch               |
|----------------|------------------------------|---------------------------|
| 职责           | 网络拓扑传播                 | 实际支付转发               |
| 消息           | ChannelAnn, Update, NodeAnn  | update_add_htlc 等        |
| 数据           | 通道存在性、路由策略         | HTLC 金额、支付哈希        |
| 存储           | ChannelGraph (图数据库)      | ChannelState (状态机)      |
| 更新频率       | 按需广播策略变更             | 每次 HTLC 更新            |

---

## 附录 G: Gossip 功能停止的影响分析

### G.1 影响概述

```
┌──────────────────────────────────────────────────────────────────┐
│                    Gossip 功能停止的层级影响                           │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  完全不影响 (本地操作)                                              │
│  ├── 本地通道状态管理 (开启/关闭/状态更新)                          │
│  ├── HTLC 转发 (已建立的通道内)                                    │
│  └── 链上交易 (funding/closing)                                   │
│                                                                  │
│  ─────────────────────────────────────────────────────────────  │
│                                                                  │
│  短期影响 (通道仍可工作，但无法发现新路由)                          │
│  ├── 无法接收新的 ChannelAnnouncement                              │
│  ├── 无法接收新的 ChannelUpdate                                   │
│  ├── 无法发现新通道                                               │
│  └── 无法更新其他节点的策略                                        │
│                                                                  │
│  ─────────────────────────────────────────────────────────────  │
│                                                                  │
│  长期影响 (网络可用性逐渐下降)                                     │
│  ├── 路由路径逐渐失效                                             │
│  ├── 无法与新节点建立连接                                         │
│  ├── 通道策略变为陈旧                                             │
│  └── 网络视图与实际网络脱节                                        │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### G.2 对本地通道的影响

#### G.2.1 完全不受影响的功能

**本地通道操作**：
- 开启新通道 (funding flow)
- 关闭通道 (cooperative/force close)
- 通道状态同步 (channel reestablish)
- HTLC 转发

**原因**：这些功能使用直接的 P2P 消息，不依赖 Gossip 协议。

#### G.2.2 受影响的功能

| 功能 | 影响程度 | 说明 |
|------|---------|------|
| 通道公告广播 | **完全失效** | 无法向网络广播新的 ChannelAnnouncement |
| 通道策略更新 | **完全失效** | 无法发送 ChannelUpdate 更新路由策略 |
| 节点公告 | **完全失效** | 无法广播 NodeAnnouncement |

**关键代码** (`discovery/gossiper.go`):

```go
// Gossip 停止后，这些消息无法被传播
case *lnwire.AnnounceSignatures1:
    // 需要通过 gossiper 处理和广播
    d.handleAnnSig(ctx, announcement)

case *lnwire.ChannelUpdate1:
    // 依赖 gossiper 进行验证和广播
    d.handleChanUpdate(ctx, announcement)
```

### G.3 对闪电网络交易的影响

#### G.3.1 交易转发能力

**仍可工作的场景**：
- 已建立的通道内的 HTLC 转发
- 与直接相连对等体之间的支付

**无法工作的场景**：
- 多跳支付（需要路由发现）
- 向未知节点支付（需要发现路由路径）

#### G.3.2 路由发现依赖

```
┌──────────────────────────────────────────────────────────────────┐
│                    路由发现流程 (依赖 Gossip)                        │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Router 使用 ChannelGraph 查询可达路径                          │
│  2. ChannelGraph 数据来源于 Gossip 同步                           │
│  3. Gossip 停止 → ChannelGraph 不更新                            │
│  4. 新通道/策略 → 不可见                                          │
│  5. 路由失败概率增加                                              │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**相关代码** (`routing/pathfind.go`):

```go
// 路由查找依赖 ChannelGraph
func (r *Router) findPath(...) (route.Route, error) {
    // 使用 ChannelGraph 寻找路径
    // ChannelGraph 数据来自 Gossip 同步
    graph := r.graph

    // 如果 Gossip 停止，新通道不可见
    // 路径查找将失败
}
```

### G.4 同步状态机的影响

#### G.4.1 GossipSyncer 状态

**位置**: `lib/lndv0.20.1/discovery/syncer.go`

```go
const (
    syncingChans syncerState = iota      // 0: 初始同步状态
    waitingQueryRangeReply               // 1: 等待 QueryChannelRange 回复
    queryNewChannels                     // 2: 查询新通道
    waitingQueryChanReply                // 3: 等待 QueryShortChanIDs 回复
    chansSynced                          // 4: 同步完成
    syncerIdle                          // 5: 空闲状态
)
```

**Gossip 停止后的状态**：
- 已同步的通道 → 保持 `chansSynced` 状态
- 新对等体连接 → 无法同步新通道信息
- 历史同步 → 无法完成

#### G.4.2 僵尸通道检测

**位置**: `lib/lndv0.20.1/discovery/gossiper.go`

```go
// ChannelPruneExpiry = 14 天
// 超过 14 天没有更新 → 标记为僵尸
const DefaultChannelPruneExpiry = 14 * 24 * time.Hour
```

**Gossip 停止后的影响**：
- 无法接收新的 ChannelUpdate
- 所有通道在 14 天后被标记为僵尸
- 路由查找时排除僵尸通道

### G.5 私有通道的特殊情况

```
┌──────────────────────────────────────────────────────────────────┐
│                    私有通道 vs 公有通道                              │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  私有通道:                                                        │
│  ├── 不通过 Gossip 广播                                           │
│  ├── 仅对直接对等体可见                                           │
│  ├── 通道公告不被发送                                             │
│  └── Gossip 停止不影响其基本功能                                   │
│                                                                  │
│  公有通道:                                                        │
│  ├── 通过 Gossip 广播到全网                                       │
│  ├── 需要 ChannelAnnouncement                                    │
│  └── Gossip 停止 → 无法被发现                                    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**相关代码** (`funding/manager.go`):

```go
// private channels do not announce the channel policy to the network
// but still need to delete them from the database
err = f.deleteInitialForwardingPolicy(chanID)
```

### G.6 影响时间线

| 影响维度 | Gossip 停止瞬间 | 1 天后 | 7 天后 | 14 天后 |
|---------|---------------|--------|--------|---------|
| 新通道发现 | 受限 | 受限 | 受限 | 受限 |
| 策略更新 | 失败 | 失败 | 失败 | 失败 |
| 多跳支付 | 部分失败 | 失败率增加 | 显著失败 | 严重失败 |
| 直接通道支付 | 正常 | 正常 | 正常 | 正常 |
| 本地通道操作 | 正常 | 正常 | 正常 | 正常 |
| 僵尸通道 | 0 | 0 | 增加 | 全部标记 |

### G.7 恢复策略

如果 Gossip 功能恢复：

1. **自动同步**：GossipSyncer 会重新进入 `syncingChans` 状态
2. **增量同步**：使用 QueryChannelRange 同步新通道
3. **策略恢复**：接收新的 ChannelUpdate 更新本地策略
4. **僵尸复活**：接收到新更新的通道自动标记为活跃

---

## 附录 H: 停止 Gossip 服务的方法

### H.1 配置选项（推荐）

#### H.1.1 设置零同步对等体

将 `numgraphsyncpeers` 设置为 0，停止自动 Gossip 同步：

```ini
[Application Options]

# 将图同步对等体数量设为 0
numgraphsyncpeers=0
```

#### H.1.2 关闭历史同步

```ini
# 将历史同步间隔设为最大值 (实际禁用)
historicalsyncinterval=0

# 忽略历史 Gossip 过滤器
ignore-historical-gossip-filters=true
```

#### H.1.3 限制 Gossip 消息速率

```ini
[gossip]

# 将消息速率设为极低值
gossip.msg-rate-bytes=1

# 将突发限制设为最小值
gossip.msg-burst-bytes=1

# 关闭公告广播
gossip.announcement-conf=999999
```

#### H.1.4 完整配置示例

```ini
[Application Options]

# 停止 Gossip 同步
numgraphsyncpeers=0

# 禁用历史同步
historicalsyncinterval=0
ignore-historical-gossip-filters=true

[gossip]

# 极低的消息速率
gossip.msg-rate-bytes=1
gossip.msg-burst-bytes=1

# 不公告通道
gossip.announcement-conf=999999
```

### H.2 API/CLI 方法

#### H.2.1 断开 Gossip 连接

```bash
# 断开所有对等体连接
lncli disconnect --pubkey=<peer_pubkey> --force
```

#### H.2.2 关闭公告广播

```bash
# 使用 debug 工具停止公告广播
lncli debuglevel --show
```

### H.3 代码级别控制

**位置**: `lib/lndv0.20.1-beta/discovery/gossiper.go`

```go
// AuthenticatedGossiper 提供了 Stop 方法
// 可以通过 RPC 调用停止 Gossip 服务

// 停止 Gossip 服务
func (d *AuthenticatedGossiper) Stop() error {
    d.stopped.Do(func() {
        log.Info("Authenticated gossiper shutting down...")
        d.stop()
    })
    return nil
}
```

### H.4 最小化 Gossip 配置对比

| 配置项 | 正常值 | 最小化值 | 效果 |
|--------|--------|---------|------|
| `numgraphsyncpeers` | 3 | 0 | 停止自动同步 |
| `gossip.msg-rate-bytes` | 1024000 | 1 | 极低速率 |
| `historicalsyncinterval` | 1h | 0 | 禁用历史同步 |
| `gossip.announcement-conf` | 6 | 999999 | 几乎不公告 |

### H.5 注意事项

```
┌──────────────────────────────────────────────────────────────────┐
│                    停止 Gossip 的影响                                │
├──────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ❌ 警告: 完全停止 Gossip 会导致:                                │
│                                                                  │
│  ├── 无法发现新通道                                                │
│  ├── 无法接收通道更新                                             │
│  ├── 无法广播你的通道                                             │
│  ├── 路由能力大幅下降                                             │
│  └── 网络视图逐渐陈旧                                             │
│                                                                  │
│  ✅ 建议:                                                        │
│                                                                  │
│  ├── 保留少量 pinned syncers                                     │
│  ├── 设置极低的速率限制而非完全停止                                 │
│  └── 考虑使用私有通道                                             │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### H.6 推荐方案

如果你需要"最小化 Gossip"而不是完全停止，推荐以下配置：

```ini
[Application Options]

# 减少同步对等体数量
numgraphsyncpeers=1

# 保留历史同步用于恢复
historicalsyncinterval=24h

[gossip]

# 设置 pinned syncer 只同步特定节点
# gossip.pinned-syncers=<your-trusted-node-pubkey>

# 降低消息速率节省带宽
gossip.msg-rate-bytes=102400
gossip.msg-burst-bytes=204800

# 标准公告确认数
gossip.announcement-conf=6
```

### H.7 重启生效

```
配置修改后需要重启 lnd:
  systemctl restart lnd
  # 或
  killall lnd && lnd
```
