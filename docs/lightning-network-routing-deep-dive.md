# 闪电网络路由深度剖析

> 生成时间: 2026年5月26日
> 项目: Lightning Terminal (LiT)
> 依赖版本: LND v0.20.1

---

## 目录

1. [核心概念](#1-核心概念)
2. [HTLC 哈希时间锁合约](#2-htlc-哈希时间锁合约)
3. [洋葱路由协议](#3-洋葱路由协议)
4. [路由发现机制](#4-路由发现机制)
5. [寻路算法](#5-寻路算法)
6. [Mission Control](#6-mission-control)
7. [路由构建过程](#7-路由构建过程)
8. [隐私保护机制](#8-隐私保护机制)
9. [失败处理与重试](#9-失败处理与重试)
10. [高级特性](#10-高级特性)
11. [总结](#11-总结)

---

## 1. 核心概念

闪电网络路由的核心创新在于将 **HTLC 合约** 与 **洋葱路由** 相结合，实现了：

- **无需信任中间节点** - 所有条件都由合约强制执行
- **隐私保护** - 每跳只知道相邻节点信息
- **原子性** - 支付要么完全成功，要么完全失败
- **可扩展性** - 路径长度不受限制，只与费用相关

---

## 2. HTLC 哈希时间锁合约

### 2.1 HTLC 工作原理

```
┌─────────────────────────────────────────────────────────────────┐
│                    HTLC 工作原理                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Alice ──── HTLC ──── Bob ──── HTLC ──── Carol ──── HTLC ──── Dave
│   │                                │                             │
│   │  Payment Hash: H(R)           │  Reveal R                   │
│   │  Amount: 1000 sats             │  (R = Preimage)             │
│   │  CLTV Timeout: 40 blocks       │                             │
│   │                                │                             │
│   │  如果 Carol 在 40 个区块内      │                             │
│   │  提供正确的 R，她获得 1000 sats  │                             │
│   │  否则，资金退回 Alice           │                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 HTLC 关键字段

```protobuf
message HTLC {
    // 支付哈希（由接收方生成）
    bytes payment_hash = 1;
    
    // CLTV 时间锁到期值（相对时间）
    uint32 cltv_expiry = 2;
    
    // 金额（毫聪）
    int64 amount = 3;
    
    // 洋葱路由信息
    OnionHopNodeFormat onion_routing_packet = 4;
}
```

### 2.3 HTLC 状态机

```
┌─────────────────────────────────────────────────────────────────┐
│                    HTLC 状态机                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│     ┌─────────┐                                                │
│     │ ADDED   │ ← 发起 HTLC                                    │
│     └────┬────┘                                                │
│          │                                                      │
│          ▼                                                      │
│     ┌─────────┐                                                │
│     │ SENT    │ ← 发送给下一跳                                 │
│     └────┬────┘                                                │
│          │                                                      │
│     ┌────┴────┐                                                │
│     │         │                                                 │
│     ▼         ▼                                                 │
│ ┌──────┐  ┌────────┐                                           │
│ │FULFILLED│ │TIMEDOUT│                                         │
│ │  R 揭示 │  │  退回  │                                         │
│ └──────┘  └────────┘                                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 洋葱路由协议

### 3.1 Sphinx Onion 结构

闪电网络使用改良版的洋葱路由协议（Sphinx）。

```
┌─────────────────────────────────────────────────────────────────┐
│                      Sphinx Onion 结构                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  Layer 0: Alice → Bob                                   │    │
│  │  ┌───────────────────────────────────────────────────┐ │    │
│  │  │  Layer 1: Bob → Carol                              │ │    │
│  │  │  ┌───────────────────────────────────────────────┐ │ │    │
│  │  │  │  Layer 2: Carol → Dave                       │ │ │    │
│  │  │  │  ┌─────────────────────────────────────────┐ │ │ │    │
│  │  │  │  │  Payload: {next_hop, amount, fee, ...} │ │ │ │    │
│  │  │  │  │  HMAC: 认证信息                         │ │ │ │    │
│  │  │  │  └─────────────────────────────────────────┘ │ │ │    │
│  │  │  └───────────────────────────────────────────────┘ │ │    │
│  │  └───────────────────────────────────────────────────┘ │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  每层只知道自己前后节点，不知道完整路径                           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 洋葱数据包结构

```go
// lib/lnd-0.20.1-beta/channeldb/hop.go
type Hop struct {
    // 下一跳的节点公钥
    PubKeyBytes []byte
    
    // 通道 ID
    ChannelID uint64
    
    // 发送给下一跳的加密数据包
    EncryptedData []byte
    
    // HMAC 用于验证完整性
    HMAC []byte
    
    // 时间锁增量
    CLTVDelta uint16
    
    // 手续费
    FeeMsat int64
}
```

### 3.3 洋葱构建代码

```go
// lib/lnd-0.20.1-beta/routing/onion.go

type SphinxOnion struct {
    // 密钥材料
    sessionKey *btcec.PrivateKey
    
    // 路由路径
    hops []*Hop
}

// 构建洋葱
func (s *SphinxOnion) EncodeOnionPacket(hops []*Hop) (*OnionPacket, error) {
    packet := &OnionPacket{}
    
    // 从最后一跳开始向前构建（反向构建）
    var mixKey [32]byte
    var currentHMAC [32]byte
    var currentStream []byte
    
    for i := len(hops) - 1; i >= 0; i-- {
        hop := hops[i]
        
        // 生成下一层的加密密钥
        nextKey := s.generateKey("rho", mixKey[:])
        nextHmacKey := s.generateKey("mu", mixKey[:])
        
        // 构建当前跳的负载
        payload := s.buildHopPayload(hop, currentHMAC[:], currentStream)
        
        // 使用密钥加密
        encryptedPayload := s.EncryptForward(payload, nextKey)
        
        // 设置 HMAC 用于认证
        hmac := s.HMAC(currentHmacKey[:], encryptedPayload)
        
        // 更新混合密钥
        mixKey = s.mixKey(mixKey[:], hop.PubKeyBytes)
        
        currentHMAC = hmac
        currentStream = encryptedPayload
    }
    
    return packet, nil
}

// 构建单跳负载
func (s *SphinxOnion) buildHopPayload(hop *Hop, nextHMAC, nextStream []byte) []byte {
    return PacketHop{
        // 下一跳的节点信息
        NextNode:    hop.PubKeyBytes,
        NextChannel: hop.ChannelID,
        
        // 金额和时间锁
        Amount:      hop.AmountMsat,
        CLTVExpiry:  hop.CLTVDelta,
        
        // 认证信息
        HMAC:        nextHMAC,
        Stream:      nextStream,
    }.Serialize()
}
```

---

## 4. 路由发现机制

### 4.1 Gossip 协议

节点通过 Gossip 协议共享通道和路由信息。

```
┌─────────────────────────────────────────────────────────────────┐
│                      Gossip 消息类型                            │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. node_announcement                                          │
│     ├─ node_key (公钥)                                          │
│     ├─ rgb_color                                               │
│     ├─ alias                                                   │
│     ├─ feature_bits                                            │
│     └─ addresses                                               │
│                                                                 │
│  2. channel_announcement                                       │
│     ├─ node1_key, node2_key                                    │
│     ├─ short_channel_id                                        │
│     ├─ bitcoin_chain_hash                                      │
│     ├─ node1_signature, node2_signature                        │
│     └─ bitcoin_signature1, bitcoin_signature2                   │
│                                                                 │
│  3. channel_update                                             │
│     ├─ short_channel_id                                        │
│     ├─ timestamp                                               │
│     ├─ message_flags (是否包含 max_htlc)                        │
│     ├─ channel_flags (方向)                                    │
│     ├─ htlc_minimum_msat                                       │
│     ├─ htlc_maximum_msat                                       │
│     ├─ fee_base_msat                                           │
│     ├─ fee_proportional_millionths                             │
│     └─ cltv_delta                                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 路径发现流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    路径发现流程                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Alice 想要支付给 Dave                                          │
│                                                                 │
│  Step 1: 获取图谱信息                                           │
│  ┌────────────────────────────────────────────┐                │
│  │  QueryGraph                                │                │
│  │  ├─ 获取所有已知通道                       │                │
│  │  ├─ 获取节点和通道更新                     │                │
│  │  └─ 计算到目的地的可能路径                  │                │
│  └────────────────────────────────────────────┘                │
│                                                                 │
│  Step 2: 构建候选路由列表                                       │
│  ┌────────────────────────────────────────────┐                │
│  │  过滤条件:                                 │                │
│  │  ├─ 通道容量 >= 支付金额                   │                │
│  │  ├─ htlc_min <= amount <= htlc_max         │                │
│  │  ├─ 节点在线                               │                │
│  │  └─ 时间窗口内有效                         │                │
│  └────────────────────────────────────────────┘                │
│                                                                 │
│  Step 3: 概率评分与选择                                         │
│  ┌────────────────────────────────────────────┐                │
│  │  MissionControl 评估:                      │                │
│  │  P(success) = f(history, liquidity, ...)   │                │
│  │                                            │                │
│  │  选择概率最高且费用合理的路径               │                │
│  └────────────────────────────────────────────┘                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 4.3 费用计算公式

```
Fee = base_fee + (amount × proportional_fee / 1,000,000)

例如：
- base_fee = 1 sat
- proportional_fee = 100 (0.01%)
- amount = 100,000 sats

Fee = 1 + (100000 × 100 / 1000000)
    = 1 + 10
    = 11 sats
```

**总路由费用计算：**

```go
func CalculateTotalFees(amount int64, route []*Hop) int64 {
    var totalFees int64 = 0
    remainingAmount := amount
    
    // 从目的地向回计算
    for i := len(route) - 1; i >= 0; i-- {
        hop := route[i]
        
        // 每跳的费用
        hopFee := hop.FeeBaseMsat + 
                  (remainingAmount * hop.FeeProportionalMillionths) / 1_000_000
        
        totalFees += hopFee
        
        // 更新剩余金额（下一跳需要转发的金额）
        remainingAmount += hopFee
    }
    
    return totalFees
}
```

---

## 5. 寻路算法

### 5.1 改进的 Dijkstra 算法

LND 使用**改进的 Dijkstra 算法**进行最短路径搜索。搜索是**从目标向源反向进行**，以便：

1. 正确计算沿途的手续费
2. 准确检查通道带宽
3. 避免金额下溢

### 5.2 寻路流程图

```
┌──────────────────────────────────────────────────────────────────┐
│                      寻路流程 (Pathfinding)                       │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 1. 初始化阶段                                                     │
│    - 获取当前区块高度                                              │
│    - 初始化距离堆(使用优先队列/小顶堆)                             │
│    - 创建距离映射表(distance map)                                 │
│    - 计算绝对CLTV限制 = CltvLimit + finalHtlcExpiry              │
│    - 计算尝试成本(Attempt Cost)                                   │
│    └─公式: attemptCost = AttemptCost + amt * AttemptCostPPM / 1M │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 2. 添加目标节点到搜索队列                                         │
│    - 目标节点: distance=0, weight=0                              │
│    - 接收金额 = 支付金额                                          │
│    - 初始概率 = 1.0                                              │
│    - 初始 CLTV = finalHtlcExpiry                                 │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 3. 主循环 (Dijkstra搜索)                                         │
│    ┌─────────────────────────────────────────────────────────┐   │
│    │ 3.1 从堆中取出当前最优节点(pivot)                        │   │
│    │       (最短距离的节点优先)                                │   │
│    │                                                          │   │
│    │ 3.2 检查是否到达源节点                                    │   │
│    │       → 是: 完成路径构建                                 │   │
│    │       → 否: 继续扩展                                     │   │
│    │                                                          │   │
│    │ 3.3 获取该节点的所有入边(policies)                       │   │
│    │       - 从通道图中查询                                    │   │
│    │       - 合并路由提示(additionalEdges)                     │   │
│    │                                                          │   │
│    │ 3.4 对每条边进行评估:                                    │   │
│    │       ┌──────────────────────────────────────────────┐   │   │
│    │       │ a) 计算入站手续费 (inbound fee)               │   │   │
│    │       │    inboundFee = edge.inboundFees.CalcFee()    │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ b) 计算转出金额 (amount to send)              │   │   │
│    │       │    amtToSend = receivedAmt + inboundFee       │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ c) 检查费用限制                                 │   │   │
│    │       │    totalFee = amtToSend - paymentAmt          │   │   │
│    │       │    if totalFee > FeeLimit → skip              │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ d) 获取边成功概率 (MissionControl)             │   │   │
│    │       │    prob = ProbabilitySource(from, to, ...)    │   │   │
│    │       │    if prob == 0 → skip                       │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ e) 检查CLTV限制                               │   │   │
│    │       │    newCltv = currentCltv + timeLockDelta     │   │   │
│    │       │    if newCltv > absoluteCltvLimit → skip      │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ f) 计算边权重 (edge weight)                   │   │   │
│    │       │    fee = inboundFee + outboundFee            │   │   │
│    │       │    weight = fee + timeLockPenalty            │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ g) 计算概率调整后的距离                        │   │   │
│    │       │    dist = weight + attemptCost / probability  │   │   │
│    │       ├──────────────────────────────────────────────┤   │   │
│    │       │ h) 检查routing info大小限制                    │   │   │
│    │       │    if routingInfoSize > MaxPayloadSize        │   │   │
│    │       │       → skip                                  │   │   │
│    │       └──────────────────────────────────────────────┘   │   │
│    │                                                          │   │
│    │ 3.5 更新最优路径                                          │   │
│    │       - 比较 tempDist 与 current.dist                     │   │
│    │       - 更新距离表和下一跳映射                             │   │
│    │                                                          │   │
│    │ 3.6 将候选节点加入堆中                                   │   │
│    └─────────────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│ 4. 路径构建 (newRoute)                                           │
│    - 从源节点沿 nextHop 指针回溯构建完整路径                      │
│    - 反向遍历计算每个hop的:                                       │
│      · AmtToForward (转发金额)                                   │
│      · OutgoingTimeLock (timelock值)                            │
│      · Fee (手续费)                                              │
│    - 处理盲化路由的特殊字段                                       │
│    - 生成 Sphinx onion 数据包                                     │
└──────────────────────────────────────────────────────────────────┘
```

### 5.3 边权重计算

**公式:**

```
边权重 = 手续费 + 时间锁惩罚

时间锁惩罚 = 锁定金额 × 时间锁增量 × 风险因子 / 10^9
```

**代码实现** (`pathfind.go`):

```go
func edgeWeight(lockedAmt lnwire.MilliSatoshi, fee lnwire.MilliSatoshi,
    timeLockDelta uint16) int64 {

    // 风险因子常量
    RiskFactorBillionths = 15

    // 时间锁惩罚 = 锁定金额 * 时间锁增量 * 风险因子 / 10^9
    timeLockPenalty := int64(lockedAmt) * int64(timeLockDelta) *
                       RiskFactorBillionths / 1000000000

    // 总权重 = 手续费 + 时间锁惩罚
    return int64(fee) + timeLockPenalty
}
```

### 5.4 概率调整后的距离

```go
func getProbabilityBasedDist(weight int64, probability float64,
    attemptCost float64) float64 {

    // 公式: weight + attemptCost / probability
    return float64(weight) + attemptCost/probability
}
```

**设计原理:**
- 低概率路径被赋予更高的"虚拟距离"
- 概率越低，需要的尝试成本越高
- 平衡费用最小化和成功率

---

## 6. Mission Control

### 6.1 概述

Mission Control 是 LND 的路径成功率预测系统。

```
┌─────────────────────────────────────────────────────────────────┐
│                 Mission Control 状态机                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│                    ┌──────────────┐                            │
│                    │   Initial    │                            │
│                    │  P = 0.5     │                            │
│                    └──────┬───────┘                            │
│                           │                                     │
│              ┌────────────┼────────────┐                        │
│              │ success    │            │ fail                  │
│              ▼            │            ▼                        │
│        ┌──────────┐       │      ┌──────────┐                   │
│        │   High   │       │      │   Low    │                   │
│        │ P *= 1.2 │       │      │ P *= 0.8 │                   │
│        │ P = min  │       │      └────┬─────┘                   │
│        │  (1.0)   │       │           │                         │
│        └────┬─────┘       │           │                         │
│             │            │           │                         │
│             └────────────┴───────────┘                         │
│                      │                                          │
│              ┌───────┴───────┐                                  │
│              │    Update     │                                  │
│              │  Probability  │                                  │
│              └───────────────┘                                  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 概率计算

```go
// lib/lnd-0.20.1-beta/routing/mission_control.go

type MissionControl struct {
    // 历史成功率记录
    history map[EdgePair]*PairProbability
}

type PairProbability struct {
    // 成功率 [0, 1]
    probability float64
    
    // 最后更新时间
    lastUpdate time.Time
    
    // 失败次数
    failures int
    
    // 成功次数  
    successes int
}

// 计算路径成功率
func (m *MissionControl) PathProbability(route []*Hop, amount int64) float64 {
    var totalProb float64 = 1.0
    
    for _, hop := range route {
        edgeProb := m.getEdgeProbability(hop)
        totalProb *= edgeProb
    }
    
    // 应用时间衰减
    return totalProb * m.timeDecayFactor()
}

// 更新成功/失败
func (m *MissionControl) RecordSuccess(hop *Hop) {
    m.updateProbability(hop, true)
}

func (m *MissionControl) RecordFailure(hop *Hop) {
    m.updateProbability(hop, false)
}
```

### 6.3 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultPenaltyHalfLife` | 1小时 | 半衰期，衰减历史数据影响 |
| `DefaultMaxMcHistory` | 1000 | 最大历史记录数 |
| `DefaultMcFlushInterval` | 1秒 | 状态刷新间隔 |
| `minSecondChanceInterval` | 1分钟 | 最小二次尝试间隔 |
| `DefaultAprioriHopProbability` | 0.6 | 单跳默认概率 |
| `DefaultAprioriWeight` | 0.5 | 先验权重 |

---

## 7. 路由构建过程

### 7.1 SendPaymentV2 完整流程

```
┌─────────────────────────────────────────────────────────────────┐
│              SendPaymentV2 完整流程                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 初始化支付                                                   │
│     ┌─────────────────────────────────────────────┐            │
│     │ PaymentSession                             │            │
│     │ ├─ 设置支付哈希 H                           │            │
│     │ ├─ 获取目标节点 pubkey                      │            │
│     │ └─ 创建支付隧道                             │            │
│     └─────────────────────────────────────────────┘            │
│                         ↓                                       │
│  2. 路径发现                                                     │
│     ┌─────────────────────────────────────────────┐            │
│     │ QueryRoutes                               │            │
│     │ ├─ 调用 FindRoutes                        │            │
│     │ ├─ 获取图谱快照                            │            │
│     │ └─ 使用 Dijkstra/A* 算法                   │            │
│     └─────────────────────────────────────────────┘            │
│                         ↓                                       │
│  3. 路由选择                                                     │
│     ┌─────────────────────────────────────────────┐            │
│     │ RouteSelection                             │            │
│     │ ├─ 过滤无效路径                            │            │
│     │ ├─ 使用 MissionControl 评分                │            │
│     │ └─ 选择最优路径                            │            │
│     └─────────────────────────────────────────────┘            │
│                         ↓                                       │
│  4. 洋葱加密                                                     │
│     ┌─────────────────────────────────────────────┐            │
│     │ Build onions                              │            │
│     │ ├─ 为每跳生成加密层                        │            │
│     │ ├─ 计算 HMAC 认证                         │            │
│     │ └─ 填充下一跳信息                          │            │
│     └─────────────────────────────────────────────┘            │
│                         ↓                                       │
│  5. HTLC 发起                                                    │
│     ┌─────────────────────────────────────────────┐            │
│     │ SendHTLC                                   │            │
│     │ ├─ 沿路径逐跳添加 HTLC                     │            │
│     │ ├─ 等待各方确认                            │            │
│     │ └─ 返回支付状态                            │            │
│     └─────────────────────────────────────────────┘            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 Route 数据结构

```go
type Route struct {
    // 累计时间锁 - 发送给第一跳的HTLC的CLTV值
    TotalTimeLock uint32

    // 路由总金额(含手续费)
    // 发送给第一跳的HTLC必须至少包含此金额
    TotalAmount   lnwire.MilliSatoshi

    // 起点公钥
    SourcePubKey  Vertex

    // 路径上的所有跳跃
    Hops          []*Hop
}

type Hop struct {
    // 目标节点公钥
    PubKeyBytes      Vertex

    // 通道ID (格式: 区块高度 + 交易索引 + 输出索引)
    ChannelID        uint64

    // 外出HTLC的timelock值
    OutgoingTimeLock uint32

    // 转发金额
    AmtToForward     lnwire.MilliSatoshi

    // 多路径支付数据
    MPP              *record.MPP

    // 原子多路径支付数据
    AMP              *record.AMP

    // 自定义TLV记录
    CustomRecords    record.CustomSet

    // 盲化路由加密数据
    EncryptedData    []byte

    // 盲化点
    BlindingPoint    *btcec.PublicKey
}
```

---

## 8. 隐私保护机制

### 8.1 洋葱路由的隐私保证

```
┌─────────────────────────────────────────────────────────────────┐
│                    隐私保护层级                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  问题                    │  解决方案                           │
│  ────────────────────────┼───────────────────────────────       │
│  路径可见                │  所有数据加密，只有下一跳可见        │
│  目的地可见              │  洋葱外层只含下一跳信息             │
│  金额可见                │  每跳只知道需要转发的金额           │
│  路径追踪                │  每跳 HMAC 认证，无法篡改           │
│  时间关联                │  随机延迟和假路径                   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 8.2 假路径（Phantom Routes）

```go
// lib/lnd-0.20.1-beta/routing/phantom.go

// 发送方可以添加"假跳"来混淆真实路径长度
type PhantomRoute struct {
    // 真实目的地
    RealDestination []byte
    
    // 假路径长度
    PhantomHopCount int
    
    // 假路径节点
    PhantomNodes [][]byte
}

// 应用假路径
func ApplyPhantomRoute(route []*Hop, phantom *PhantomRoute) []*Hop {
    // 在真实路径前面插入假跳
    phantomRoute := make([]*Hop, 0, len(route)+phantom.PhantomHopCount)
    
    // 添加假跳（只有 Alice 知道这是假的）
    for i := 0; i < phantom.PhantomHopCount; i++ {
        phantomRoute = append(phantomRoute, &Hop{
            PubKeyBytes: phantom.PhantomNodes[i],
        })
    }
    
    // 添加真实路径
    phantomRoute = append(phantomRoute, route...)
    
    return phantomRoute
}
```

### 8.3 致盲路由（Blinded Routes）

```go
// lib/lnd-0.20.1-beta/routing/blinded.go

// 接收方创建致盲路径
type BlindedRoute struct {
    // 短致盲节点（入口点）
    BlindedNodeID []byte
    
    // 加密的路径信息
    EncryptedData []byte
    
    // 时间限制
    CLTVExpiry uint32
    
    // 致盲路径长度
    PathLength int
}

// 生成致盲路径
func CreateBlindedRoute(target *btcec.PublicKey) (*BlindedRoute, error) {
    // 1. 生成随机路径
    path := generateRandomPath(target, hopCount)
    
    // 2. 计算路径的私密信息
    blindingPoint := generateBlindingPoint()
    
    // 3. 加密路径信息（只有目的地能解密）
    encrypted := encryptPathInfo(path, blindingPoint)
    
    return &BlindedRoute{
        BlindedNodeID: blindingPoint.SerializeCompressed(),
        EncryptedData: encrypted,
        PathLength:    len(path),
    }, nil
}
```

---

## 9. 失败处理与重试

### 9.1 HTLC 失败代码

```go
// lib/lnd-0.20.1-beta/lnwire/failcodes.go

const (
    // 永久失败
    CODE_INCORRECT_OR_UNKNOWN_PAYMENT_DETAILS = 0x4001
    CODE_INCORRECT_PAYMENT_AMOUNT              = 0x4002
    CODE_FINAL_INCORRECT_CLTV_EXPIRY           = 0x4003
    CODE_FINAL_INCORRECT_HTLC_AMOUNT           = 0x4004
    CODE_INVALID_ONION_VERSION                 = 0x4005
    CODE_INVALID_ONION_HMAC                    = 0x4006
    CODE_INVALID_ONION_KEY                     = 0x4007
    CODE_AMOUNT_BELOW_MINIMUM                  = 0x4008
    CODE_FEE_INSUFFICIENT                      = 0x4009
    CODE_INCORRECT_CLTV_EXPIRY                 = 0x400A
    CODE_HTLC_EXCEEDS_MAX                     = 0x400B
    CODE_TEMPORARY_CHANNEL_FAILURE             = 0x7001
    CODE_REQUIRED_NODE_FEATURE_MISSING          = 0x7002
    CODE_REQUIRED_CHANNEL_FEATURE_MISSING      = 0x7003
    CODE_UNKNOWN_NEXT_PEER                     = 0x7004
    CODE_MPP_TIMEOUT                           = 0x7005
)

// 临时失败 → 可以重试
// 永久失败 → 不重试或换路径
```

### 9.2 重试策略

```
┌─────────────────────────────────────────────────────────────────┐
│                    支付重试流程                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  支付失败                                                       │
│       │                                                        │
│       ├──[临时失败]──→ 等待 ──→ 重试 ──→ 重新路径发现           │
│       │                                                        │
│       ├──[永久失败]──→ 分析原因 ──→ 是否需要换路径？            │
│       │                           │                             │
│       │                      是   │   否                        │
│       │                           ▼                             │
│       │                    终止支付                             │
│       │                           │                             │
│       │                           ▼                             │
│       │                    返回错误给用户                       │
│       │                                                        │
│       └──[MPP 超时]──→ 检查碎片 ──→ 继续等待？──→ 终止          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 10. 高级特性

### 10.1 多路径支付（MPP）

```
┌─────────────────────────────────────────────────────────────────┐
│                  Multi-Path Payment (MPP)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Alice 想支付 100,000 sats 给 Dave                              │
│                                                                 │
│  路径 1: Alice → Bob → Dave     (40,000 sats)                   │
│  路径 2: Alice → Carol → Dave  (35,000 sats)                   │
│  路径 3: Alice → Eve → Dave    (25,000 sats)                   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  支付构建:                                               │    │
│  │  payment_hash = SHA256(preimage)                        │    │
│  │  所有分片使用相同的 payment_hash                         │    │
│  │  Dave 只在收到全部金额后释放 preimage                    │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  完成条件:                                               │    │
│  │  sum(已接收) >= 目标金额                                 │    │
│  │  或超时（通常 90 秒）                                     │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 10.2 AMP（原子化多路径支付）

```
┌─────────────────────────────────────────────────────────────────┐
│              AMP (Atomic Multi-Path Payment)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  与 MPP 的区别:                                                  │
│  - MPP: 所有分片使用相同的 payment_hash                         │
│  - AMP: 每个分片有不同的 payment_secret                         │
│                                                                 │
│  流程:                                                          │
│  1. 发送方生成根种子 R                                           │
│  2. 每个分片使用 R + 索引 生成不同的支付种子                     │
│  3. 接收方收集所有分片的 HTLC                                   │
│  4. 使用 R 原子化释放所有资金                                    │
│                                                                 │
│  优点:                                                          │
│  - 更高的隐私性 (每个分片独立追踪)                               │
│  - 更强的原子性保证                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 10.3 路由提示（Route Hints）

当收款方有私有通道时，需要在发票中包含路由提示：

```go
// lib/lnd-0.20.1-beta/zpay32/hophint.go
type HopHint struct {
    // 目标节点公钥
    NodeID                    *btcec.PublicKey
    
    // 通道ID
    ChannelID                 uint64
    
    // 基础费用
    FeeBaseMSat               int64
    
    // 费率 (每百万 sat)
    FeeProportionalMillionths int64
    
    // CLTV 时间锁增量
    CLTVExpiryDelta           uint16
}

// RouteHints 是多个路由提示的集合
type RouteHints [][]HopHint
```

### 10.4 通道策略

```go
type ChannelPolicy struct {
    // 基础费用: 任何支付都收取的固定费用
    BaseFee    lnwire.MilliSatoshi
    
    // 费率: 每百万 sat 收取的费用比例
    // 实际费率 = FeeRate / 1,000,000
    FeeRate    uint32
    
    // 时间锁增量: 转发支付时必须满足的最小时间差
    TimeLockDelta uint32
    
    // 最大HTLC: 可以转发的最大金额(含手续费)
    MaxHTLC     lnwire.MilliSatoshi
    
    // 最小HTLC: 可以转发的最小金额
    MinHTLC     *lnwire.MilliSatoshi
}
```

---

## 11. 总结

### 11.1 闪电网络路由全景图

```
┌─────────────────────────────────────────────────────────────────┐
│                  闪电网络路由全景图                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                      用户发起支付                        │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  1. Gossip 协议 → 获取网络拓扑                          │    │
│  │     node_announcement, channel_announcement,           │    │
│  │     channel_update                                      │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  2. 路径发现 → FindRoutes                              │    │
│  │     ├─ Dijkstra/A* 算法                                 │    │
│  │     ├─ MissionControl 概率评分                          │    │
│  │     └─ 费用优化                                          │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  3. 洋葱构建 → Sphinx Packet                           │    │
│  │     ├─ 每跳加密                                          │    │
│  │     ├─ HMAC 认证                                         │    │
│  │     └─ 隐私保护                                          │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  4. HTLC 执行 → 逐跳添加 HTLC                          │    │
│  │     ├─ 时间锁保护                                        │    │
│  │     ├─ 哈希锁验证                                        │    │
│  │     └─ 原子化转移                                        │    │
│  └─────────────────────────┬───────────────────────────────┘    │
│                            │                                     │
│                            ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │  5. 支付完成 → 揭示 Preimage                           │    │
│  │     └─ 逐跳解锁资金                                      │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 11.2 关键创新总结

| 创新 | 描述 |
|------|------|
| HTLC | 哈希时间锁实现无需信任的跨节点转账 |
| 洋葱路由 | Sphinx 协议保护路径隐私 |
| 源路由 | 发送方选择完整路径，中间节点无法知晓全局 |
| 反向搜索 | 从目标反向搜索，正确计算费用 |
| 概率加权 | MissionControl 根据历史数据评估成功率 |
| 多路径支付 | MPP/AMP 支持大额支付拆分 |

### 11.3 相关文件索引

```
lib/lnd-0.20.1-beta/
├── routing/
│   ├── router.go                 # ChannelRouter 主文件
│   ├── pathfind.go               # 寻路算法
│   ├── missioncontrol.go        # MissionControl
│   ├── payment_session.go       # 支付会话
│   ├── onion.go                  # 洋葱加密
│   ├── phantom.go               # 假路径
│   ├── blindedpath/
│   │   └── blinded_path.go      # 盲化路径
│   └── route/
│       └── route.go             # Route, Hop 数据结构
├── lnrpc/routerrpc/
│   ├── router_backend.go         # RPC 后端
│   └── router_server.go         # RPC 服务器
└── zpay32/
    ├── invoice.go                # BOLT 11 发票解析
    └── hophint.go                # HopHint 解析
```

---

*文档结束*
