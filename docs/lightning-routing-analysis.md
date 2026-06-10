# Lightning Terminal 闪电通道支付与路由分析文档

> 生成时间: 2026年5月6日
> 项目: Lightning Terminal (LiT)
> 依赖版本: LND v0.20.1

---

## 目录

1. [项目概述](#1-项目概述)
2. [整体架构](#2-整体架构)
3. [路由核心组件](#3-路由核心组件)
4. [寻路流程](#4-寻路流程)
5. [MissionControl](#5-missioncontrol)
6. [支付会话](#6-支付会话)
7. [通道策略](#7-通道策略)
8. [路由提示](#8-路由提示)
9. [盲化路由](#9-盲化路由)
10. [关键方法汇总](#10-关键方法汇总)
11. [关键配置参数](#11-关键配置参数)

---

## 1. 项目概述

**Lightning Terminal (LiT)** 是一个基于浏览器的 Lightning Network 节点管理界面。项目使用 Go 语言开发，主要依赖 LND (Lightning Network Daemon) 来处理核心的闪电网络功能。

### 关键依赖

| 依赖 | 说明 |
|------|------|
| `github.com/lightningnetwork/lnd` | Lightning Network Daemon (v0.20.1) |
| `github.com/lightninglabs/loop` | 潜艇互换服务 |
| `github.com/lightninglabs/pool` | 流动性市场 |
| `github.com/lightninglabs/faraday` | 节点会计服务 |
| `github.com/lightninglabs/taproot-assets` | Taproot Assets 支持 |

### 代码位置

路由相关代码主要位于:
- `lib/lndv0.20.1/routing/` - 核心路由逻辑
- `lib/lndv0.20.1/routing/route/` - 路由数据结构
- `lib/lndv0.20.1/lnrpc/routerrpc/` - RPC 接口定义

---

## 2. 整体架构

### 2.1 架构层次图

```
┌─────────────────────────────────────────────────────────────────┐
│                     LightningTerminal (LiT)                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │   LiT RPC   │  │   Session   │  │  RPC Middleware        │ │
│  │   Proxy     │  │   Server    │  │  Manager               │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐ │
│  │  Rule       │  │  Privacy    │  │  Account                │ │
│  │  Enforcer   │  │  Mapper     │  │  Service                │ │
│  └─────────────┘  └─────────────┘  └─────────────────────────┘ │
├─────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                     LND Client (lnd)                        ││
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                ││
│  │  │ Router   │  │ Channel  │  │ Payment  │                ││
│  │  │ Client   │  │ Manager  │  │ Manager  │                ││
│  │  └──────────┘  └──────────┘  └──────────┘                ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 各层职责

| 层次 | 组件 | 职责 |
|------|------|------|
| LiT 层 | RuleEnforcer | 执行防火墙规则、请求拦截 |
| LiT 层 | PrivacyMapper | 敏感信息混淆处理 |
| LiT 层 | SessionManager | 会话管理与权限控制 |
| LND 层 | ChannelRouter | 核心路由引擎，路径搜索 |
| LND 层 | MissionControl | 历史数据分析，概率估计 |
| LND 层 | PaymentSession | 支付生命周期管理 |
| LND 层 | ControlTower | 支付状态持久化 |

---

## 3. 路由核心组件

### 3.1 ChannelRouter

**位置**: `lib/lndv0.20.1/routing/router.go`

ChannelRouter 是闪电网络的第三层路由器，负责：
- 响应路径查询请求
- 回答关于网络可达性的问题
- 自动剪枝通道图

#### 核心配置 (Config 结构体)

```go
type Config struct {
    // 自身节点标识
    SelfNode      route.Vertex           // 自身节点公钥

    // 图数据源
    RoutingGraph  Graph                  // 用于路径查找的图数据源
    Chain         lnwallet.BlockChainIO // 区块链数据源

    // HTLC 发送器
    Payer         PaymentAttemptDispatcher

    // 支付状态追踪
    Control       ControlTower

    // 路径查找的概率估计
    MissionControl MissionControlQuerier

    // 支付会话源
    SessionSource PaymentSessionSource

    // 通道带宽查询
    GetLink       getLinkQuery

    // 唯一支付ID生成
    NextPaymentID func() (uint64, error)

    // 路径查找配置
    PathFindingConfig PathFindingConfig

    // 时间源
    Clock         clock.Clock
}
```

### 3.2 核心数据结构

#### RouteRequest (路由请求)

```go
type RouteRequest struct {
    // 路径起点和终点
    Source         route.Vertex
    Target         route.Vertex

    // 支付金额
    Amount         lnwire.MilliSatoshi

    // 时间偏好: -1=费用优先, 0=平衡, 1=可靠性优先
    TimePreference float64

    // 路径限制
    Restrictions   *RestrictParams

    // 自定义TLV记录
    CustomRecords  record.CustomSet

    // 路由提示(私有通道)
    RouteHints     RouteHints

    // 最终CLTV过期增量
    FinalExpiry    uint16

    // 盲化路径集
    BlindedPathSet *BlindedPaymentPathSet
}
```

#### RestrictParams (限制参数)

```go
type RestrictParams struct {
    // 成功概率函数 - 由 MissionControl 提供
    ProbabilitySource func(from, to route.Vertex,
                          amt lnwire.MilliSatoshi,
                          capacity btcutil.Amount) float64

    // 最大费用限制
    FeeLimit         lnwire.MilliSatoshi

    // 允许的起始通道列表
    OutgoingChannelIDs []uint64

    // 最后一跳限制
    LastHop          *route.Vertex

    // 最大时间锁
    CltvLimit        uint32

    // 目的地自定义记录
    DestCustomRecords record.CustomSet

    // 目的地特性向量
    DestFeatures     *lnwire.FeatureVector

    // 支付地址
    PaymentAddr      fn.Option[[32]byte]
}
```

#### Route (路由)

**位置**: `lib/lndv0.20.1/routing/route/route.go`

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

    // 第一跳金额
    FirstHopAmount tlv.RecordT[...]

    // 第一跳自定义记录
    FirstHopWireCustomRecords lnwire.CustomRecords
}
```

#### Hop (跳跃)

```go
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

    // 是否使用旧版载荷
    LegacyPayload    bool

    // 元数据
    Metadata         []byte

    // 盲化路由加密数据
    EncryptedData    []byte

    // 盲化点
    BlindingPoint    *btcec.PublicKey

    // 盲化支付总额
    TotalAmtMsat     lnwire.MilliSatoshi
}
```

---

## 4. 寻路流程

### 4.1 核心寻路算法

LND 使用**改进的 Dijkstra 算法**进行最短路径搜索。搜索是**从目标向源反向进行**，以便：

1. 正确计算沿途的手续费
2. 准确检查通道带宽
3. 避免金额下溢

**关键函数**: `findPath()`
**位置**: `lib/lndv0.20.1/routing/pathfind.go` (第 602 行)

### 4.2 寻路流程图

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
                              │
                              ▼
                          路由结果 (*route.Route, 成功概率)
```

### 4.3 边权重计算

**公式**:

```
边权重 = 手续费 + 时间锁惩罚

时间锁惩罚 = 锁定金额 × 时间锁增量 × 风险因子 / 10^9
```

**代码实现** (`pathfind.go` 第 393-409 行):

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

**设计原理**:
- 时间锁惩罚使系统偏好时间锁增量较小的通道
- 惩罚与金额成正比，大额支付锁定风险更高
- 风险因子相对较小，避免与费用偏好冲突

### 4.4 概率调整后的距离

```go
func getProbabilityBasedDist(weight int64, probability float64,
    attemptCost float64) float64 {

    // 公式: weight + attemptCost / probability
    return float64(weight) + attemptCost/probability
}
```

**设计原理**:
- 低概率路径被赋予更高的"虚拟距离"
- 概率越低，需要的尝试成本越高
- 平衡费用最小化和成功率

### 4.5 尝试成本计算

```go
// 基础尝试成本
defaultAttemptCost := float64(
    cfg.AttemptCost +
        amt * lnwire.MilliSatoshi(cfg.AttemptCostPPM) / 1000000,
)

// 根据时间偏好调整
timePref *= 0.9  // 缩放到 [-0.9, 0.9]
absoluteAttemptCost := defaultAttemptCost * (1/(0.5 - timePref/2) - 1)

// 时间偏好效果:
//   timePref = -1 (费用优先): 2x 成本 → 接受高风险低费用路径
//   timePref =  0 (平衡):     1x 成本
//   timePref = +1 (可靠优先): ∞ 成本 → 只接受高概率路径
```

---

## 5. MissionControl

### 5.1 概述

MissionControl 是支付成功率优化的核心组件，维护历史支付结果并用于估计未来支付的边成功概率。

**位置**: `lib/lndv0.20.1/routing/missioncontrol.go`

### 5.2 核心数据结构

```go
type MissionControl struct {
    cfg        *mcConfig
    state      *missionControlState    // 内部状态
    store      *missionControlStore    // 持久化存储
    estimator  Estimator               // 概率估计器
    log        btclog.Logger
    mu         sync.Mutex
}

type TimedPairResult struct {
    FailTime     time.Time           // 最后失败时间
    FailAmt      lnwire.MilliSatoshi // 最后失败金额
    SuccessTime  time.Time           // 最后成功时间
    SuccessAmt   lnwire.MilliSatoshi // 最高成功金额
}
```

### 5.3 概率估计器

LND 支持两种概率估计器:

#### 5.3.1 AprioriEstimator (先验估计器)

基于历史成功/失败记录，使用半衰期机制衰减历史数据的影响。

**默认参数**:

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultAprioriHopProbability` | 0.6 | 单跳默认概率 |
| `DefaultAprioriWeight` | 0.5 | 先验权重 |

#### 5.3.2 BimodalEstimator (双峰估计器)

更复杂的概率模型，考虑通道容量、节点特性等多种因素。

### 5.4 关键配置

```go
type MissionControlConfig struct {
    // 概率估计器
    Estimator          Estimator

    // 最大历史记录数
    MaxMcHistory       int

    // 刷新间隔
    McFlushInterval    time.Duration

    // 最小失败放松间隔
    MinFailureRelaxInterval time.Duration
}
```

**默认值**:

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultPenaltyHalfLife` | 1小时 | 半衰期，衰减历史数据影响 |
| `DefaultMaxMcHistory` | 1000 | 最大历史记录数 |
| `DefaultMcFlushInterval` | 1秒 | 状态刷新间隔 |
| `minSecondChanceInterval` | 1分钟 | 最小二次尝试间隔 |

### 5.5 工作流程

```
┌──────────────────────────────────────────────────────────────────┐
│                    MissionControl 工作流程                         │
└──────────────────────────────────────────────────────────────────┘

1. 支付请求 → 创建 PaymentSession
        │
        ▼
2. 路径查找时 → 查询 ProbabilitySource
        │
        ├── 调用 MissionControl.GetProbability()
        │
        ├── 基于历史数据计算边成功概率
        │
        │   概率计算逻辑:
        │   ┌─────────────────────────────────────────────────┐
        │   │ 1. 检查是否有历史记录                            │
        │   │ 2. 使用半衰期衰减计算:                          │
        │   │    decayFactor = e^(-λt)                       │
        │   │    λ = ln(2) / halfLife                        │
        │   │ 3. 结合成功/失败记录计算最终概率                │
        │   └─────────────────────────────────────────────────┘
        │
        ▼
3. 支付尝试结果反馈
        │
        ├── 成功: ReportPaymentSuccess(attemptID, route)
        │         └─ 更新 SuccessTime, SuccessAmt
        │
        └── 失败: ReportPaymentFail(attemptID, route, failure)
                  └─ 更新 FailTime, FailAmt
                  └─ 判断是否为最终错误
        │
        ▼
4. 更新内部状态
        │
        ▼
5. 周期性任务
        │
        ├── 半衰期衰减
        └── 状态刷新到数据库
```

### 5.6 Second Chance 机制

```go
// 如果节点返回策略相关失败，可能获得"二次尝试"机会
// 条件:
//   - 失败类型为通道策略相关 (fee insufficient, amount below minimum, etc.)
//   - 距离上次二次尝试超过 minSecondChanceInterval (1分钟)
//   - 之前没有策略更新失败记录
```

---

## 6. 支付会话

### 6.1 PaymentSession

**位置**: `lib/lndv0.20.1/routing/payment_session.go`

PaymentSession 管理单个支付的会话生命周期。

```go
type PaymentSessionSource interface {
    // 创建新的支付会话
    NewPaymentSession(p *LightningPayment,
        firstHopBlob fn.Option[tlv.Blob],
        ts fn.Option[htlcswitch.AuxTrafficShaper]) (PaymentSession, error)

    // 创建空的支付会话(用于恢复支付)
    NewPaymentSessionEmpty() PaymentSession
}
```

### 6.2 支付生命周期

**位置**: `lib/lndv0.20.1/routing/payment_lifecycle.go`

```go
type paymentLifecycle struct {
    router            *ChannelRouter
    feeLimit         lnwire.MilliSatoshi
    paymentID        lntypes.Hash
    session          PaymentSession
    shardTracker     shards.ShardTracker
    currentHeight    uint32
    // ...
}
```

**生命周期状态**:

```
┌──────────────────────────────────────────────────────────────────┐
│                    支付生命周期                                    │
└──────────────────────────────────────────────────────────────────┘

  ┌─────────┐
  │  初始化  │ → PreparePayment()
  └────┬────┘
       │
       ▼
  ┌─────────┐
  │  请求路由 │ → NewPaymentSession()
  └────┬────┘
       │
       ▼
  ┌─────────┐
  │ 发送HTLC │ → SendAttempt()
  └────┬────┘
       │
       ├── 成功 ──────────────────────────────┐
       │                                      │
       │  ┌─────────┐                        │
       │  │  完成   │ ← ReportPaymentSuccess()│
       │  └─────────┘                        │
       │                                      │
       ├── 临时错误 ──────────────────────────┐│
       │                                      ││
       │  重新路由 ──────────────────────────┐││
       │    └─→ (发送HTLC) ─────────────────┘││
       │                                      ││
       └── 最终错误 ──────────────────────────┐│
                                             ││
         报告失败 ──────────────────────────┐││
           └─→ FailPayment()                │││
                                             │││
         支付完成 ──────────────────────────┘││
           └─ 记录结果                        ││
                                             ││
                                             └┘
```

---

## 7. 通道策略

### 7.1 FeeSchema (费用模式)

```go
type FeeSchema struct {
    // 基础费用: 任何支付都收取的固定费用
    BaseFee    lnwire.MilliSatoshi

    // 费率: 每百万 sat 收取的费用比例
    // 实际费率 = FeeRate / 1,000,000
    FeeRate    uint32

    // 入站费用
    InboundFee fn.Option[models.InboundFee]
}
```

### 7.2 ChannelPolicy (通道策略)

```go
type ChannelPolicy struct {
    FeeSchema                   // 费用配置

    // 时间锁增量: 转发支付时必须满足的最小时间差
    TimeLockDelta uint32

    // 最大HTLC: 可以转发的最大金额(含手续费)
    MaxHTLC     lnwire.MilliSatoshi

    // 最小HTLC: 可以转发的最小金额
    MinHTLC     *lnwire.MilliSatoshi
}
```

### 7.3 费用计算

```go
// ComputeFee 计算转发费用
// 公式: BaseFee + amt * FeeRate / 1,000,000
func (p *ChannelPolicy) ComputeFee(amt lnwire.MilliSatoshi) lnwire.MilliSatoshi {
    return p.BaseFee + amt * lnwire.MilliSatoshi(p.FeeRate) / 1000000
}
```

**示例**:

```
假设:
  BaseFee = 1000 msat
  FeeRate = 1000 (0.1%)

转发 100,000 msat:
  费用 = 1000 + 100000 * 1000 / 1000000
       = 1000 + 100
       = 1100 msat
```

### 7.4 CLTV 时间锁

```go
// 最小 CLTV Delta
MinCLTVDelta = 18

// 最大 CLTV Delta
MaxCLTVDelta = math.MaxUint16
```

路径上的每个节点会累加其 `TimeLockDelta`，最终的时间锁值必须不超过 `CltvLimit`。

---

## 8. 路由提示

### 8.1 概述

Route Hints 用于帮助付款方通过私有通道找到通往收款方的路由。私有通道不在公开的通道图中广播。

**位置**: `lib/lndv0.20.1/zpay32/hophint.go`

### 8.2 数据结构

```go
// HopHint 是路由提示中的单个跳跃
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
// 每个路由提示代表一条通往收款方的可能路径
type RouteHints [][]HopHint
```

### 8.3 BOLT 11 编码

路由提示编码在 BOLT 11 发票的 `r` 字段中:

```
┌──────────────────────────────────────────────────────────────┐
│ BOLT 11 r 字段格式                                          │
├──────────────────────────────────────────────────────────────┤
│ 每个 HopHint 占用 51 字节                                    │
│                                                              │
│ ┌────────┬────────┬────────┬────────────────────────┬──────┐│
│ │ pubkey │chan_id │  fee   │    fee_proportional    │delta ││
│ │ 33字节 │ 8字节  │ 8字节  │       8字节           │2字节 ││
│ └────────┴────────┴────────┴────────────────────────┴──────┘│
└──────────────────────────────────────────────────────────────┘
```

### 8.4 解析流程

```go
// parseRouteHint 解析路由提示
// 位置: lib/lndv0.20.1/zpay32/decode.go (第 595 行)

func parseRouteHint(data []byte) ([]HopHint, error) {
    // 验证长度是 51 的倍数
    if len(data)%51 != 0 {
        return nil, ErrInvalidRouteHintLength
    }

    // 每 51 字节解析一个 HopHint
    numHints := len(data) / 51
    routeHint := make([]HopHint, 0, numHints)

    for i := 0; i < numHints; i++ {
        hopHint := HopHint{
            // 解析各字段...
        }
        routeHint = append(routeHint, hopHint)
    }

    return routeHint, nil
}
```

---

## 9. 盲化路由

### 9.1 概述

盲化路由 (Blinded Routing) 允许收款方指定一条加密的路由，付款方只知道路由的入口节点，不知道后续节点。这增强了隐私性。

**位置**: `lib/lndv0.20.1/routing/blindedpath/`

### 9.2 关键结构

```go
// BlindedPath 盲化路径
type BlindedPath struct {
    // 入口节点
    IntroductionPoint *btcec.PublicKey

    // 盲化点 (用于解密)
    BlindingPoint     *btcec.PublicKey

    // 盲化跳跃列表
    BlindedHops       []*BlindedHopInfo
}

// BlindedHopInfo 单个盲化跳跃
type BlindedHopInfo struct {
    // 盲化节点公钥 (NUMS key)
    BlindedNodePub *btcec.PublicKey

    // 加密数据 (包含下一跳信息)
    CipherText     []byte
}
```

### 9.3 盲化路径工作原理

```
┌──────────────────────────────────────────────────────────────────┐
│                    盲化路由示意图                                  │
└──────────────────────────────────────────────────────────────────┘

付款方视角:                    实际路由:
    ↓                            ↓
┌─────────┐                 ┌─────────┐
│ Alice   │ ──── → ───────→ │  Bob    │ ──── → ──── → (盲化部分)
│ (付款方) │   可见路由      │(入口节点)│
└─────────┘                 └────┬────┘
                                  │
                                  │ 加密数据
                                  ▼
                              ┌─────────┐
                              │  Carol  │ ← Alice 不知道此节点
                              └────┬────┘
                                   │
                                   ▼
                              ┌─────────┐
                              │  Dave   │ ← Alice 不知道此节点
                              └────┬────┘
                                   │
                                   ▼
                              ┌─────────┐
                              │  Evan   │ ← Alice 不知道此节点
                              └─────────┘
```

### 9.4 盲化路径处理

```go
// 盲化路径在路由中的处理
// 位置: lib/lndv0.20.1/routing/pathfind.go

// 盲化路径的特点:
// 1. 中间跳跃的 AmtToForward = 0
// 2. 中间跳跃的 OutgoingTimeLock = 0
// 3. EncryptedData 包含下一跳的加密信息
// 4. BlindingPoint 用于解密
```

---

## 10. 关键方法汇总

| 方法 | 文件位置 | 功能描述 |
|------|----------|----------|
| `FindRoute` | `router.go:515` | 查找最优路由入口 |
| `findPath` | `pathfind.go:602` | 核心Dijkstra寻路算法 |
| `SendPayment` | `router.go:899` | 发送支付 |
| `SendPaymentAsync` | `router.go:919` | 异步发送支付 |
| `QueryRoutes` | `router_backend.go:175` | 查询可用路由 (RPC) |
| `GetProbability` | `missioncontrol.go` | 获取边成功概率 |
| `ReportPaymentSuccess` | `missioncontrol.go` | 报告支付成功 |
| `ReportPaymentFail` | `missioncontrol.go` | 报告支付失败 |
| `newRoute` | `pathfind.go:139` | 构建路由对象 |
| `edgeWeight` | `pathfind.go:399` | 计算边权重 |
| `getProbabilityBasedDist` | `pathfind.go` | 计算概率调整后的距离 |
| `BuildRoute` | `router.go:1328` | 根据节点列表构建路由 |
| `PreparePayment` | `router.go:967` | 准备支付会话 |
| `FindBlindedPaths` | `router.go:621` | 查找盲化路径 |
| `MarshallRoute` | `router_backend.go:607` | 路由序列化 |
| `UnmarshallRoute` | `router_backend.go:795` | 路由反序列化 |

---

## 11. 关键配置参数

### 11.1 路径查找参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultAttemptCost` | 100 satoshi | 尝试固定成本 |
| `DefaultAttemptCostPPM` | 1000 (0.1%) | 尝试比例成本 |
| `DefaultMinRouteProbability` | 0.01 (1%) | 最小路由概率 |
| `RiskFactorBillionths` | 15 | 风险因子 |
| `DefaultPayAttemptTimeout` | 60秒 | 支付尝试超时 |

### 11.2 MissionControl 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `DefaultPenaltyHalfLife` | 1小时 | 惩罚半衰期 |
| `DefaultMaxMcHistory` | 1000 | 最大历史记录数 |
| `DefaultMcFlushInterval` | 1秒 | 状态刷新间隔 |
| `minSecondChanceInterval` | 1分钟 | 最小二次尝试间隔 |
| `DefaultAprioriHopProbability` | 0.6 | 单跳默认概率 |
| `DefaultAprioriWeight` | 0.5 | 先验权重 |

### 11.3 CLTV 参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `MinCLTVDelta` | 18 | 最小 CLTV Delta |
| `MaxCLTVDelta` | 65535 | 最大 CLTV Delta |
| `DefaultFinalCLTVDelta` | 40 | 默认最终 CLTV Delta |

### 11.4 路由信息大小限制

| 参数 | 值 | 说明 |
|------|-----|------|
| `sphinx.MaxPayloadSize` | 变量 | Sphinx onion 最大载荷大小 |
| `sphinx.LegacyHopDataSize` | 130 bytes | 旧版跳跃数据大小 |

---

## 12. 总结

LND 的路由系统采用了多层次的架构设计：

### 12.1 核心组件

| 组件 | 职责 |
|------|------|
| **ChannelRouter** | 作为核心路由引擎，负责路径搜索和支付执行 |
| **MissionControl** | 通过历史数据学习，提高未来支付的成功率 |
| **Pathfinding** | 使用改进的 Dijkstra 算法，结合费用、时间和概率进行优化 |
| **PaymentSession** | 管理单个支付的完整生命周期 |
| **PrivacyMapper** | 在 Lightning Terminal 层提供隐私保护 |

### 12.2 设计目标

1. **高效性**: 使用 Dijkstra 算法的改进版本快速找到最优路径
2. **可靠性**: 通过 MissionControl 学习历史数据，避免失败率高的路径
3. **隐私性**: 支持盲化路由，保护收款方信息
4. **灵活性**: 支持多路径支付 (MPP)、自定义路由提示等

### 12.3 关键创新

1. **反向搜索**: 从目标向源搜索，正确计算费用和检查带宽
2. **概率加权**: 考虑路径成功概率，避免高风险路径
3. **半衰期衰减**: 历史数据的影响随时间递减
4. **Second Chance**: 对策略相关失败给予重试机会

---

## 附录 A: 相关文件索引

```
lib/lndv0.20.1/
├── routing/
│   ├── router.go                 # ChannelRouter 主文件
│   ├── pathfind.go               # 寻路算法
│   ├── missioncontrol.go        # MissionControl
│   ├── payment_session.go       # 支付会话
│   ├── payment_lifecycle.go     # 支付生命周期
│   ├── probability_estimator.go  # 概率估计器基类
│   ├── probability_apriori.go   # Apriori 估计器
│   ├── probability_bimodal.go   # Bimodal 估计器
│   ├── route/
│   │   └── route.go             # Route, Hop 数据结构
│   ├── blindedpath/
│   │   └── blinded_path.go      # 盲化路径
│   ├── additional_edge.go        # 额外边(路由提示)
│   ├── unified_edges.go          # 统一边策略
│   └── control_tower.go          # 控制塔
├── lnrpc/routerrpc/
│   ├── router_backend.go         # RPC 后端
│   ├── router_server.go          # RPC 服务器
│   └── router.pb.go              # Protocol Buffers
└── zpay32/
    ├── invoice.go                # BOLT 11 发票解析
    ├── hophint.go                # HopHint 解析
    └── decode.go                 # 发票解码
```

---

## 附录 B: 常见错误码

| 错误类型 | 说明 | 处理建议 |
|----------|------|----------|
| `errNoPathFound` | 未找到路径 | 检查目标可达性 |
| `errInsufficientBalance` | 余额不足 | 检查通道流动性 |
| `errNoChannel` | 无可用通道 | 检查通道状态 |
| `errFeeLimitExceeded` | 费用超限 | 提高费用限制 |
| `errCltvLimitExceeded` | 时间锁超限 | 调整时间锁限制 |
| `errUnknownRequiredFeature` | 未知必需特性 | 检查目的地特性 |

---

*文档结束*
