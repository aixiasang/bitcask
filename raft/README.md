# 🔄 Raft 分布式共识模块

Bitcask 的分布式共识实现，基于 Raft 算法提供强一致性的分布式存储能力。

## ✨ 功能特性

- 🔄 完整实现 Raft 共识算法
- 🗳️ 支持领导者选举（Leader Election）
- 📝 支持日志复制（Log Replication）
- 👥 支持成员变更（Membership Changes）
- 📸 支持快照（Snapshot）
- 🧩 集成 Bitcask 存储引擎
- 🛡️ 提供高可用性和容错能力

## 🧩 核心组件

### 🔄 RaftNode

Raft 节点实现，包含：
- 状态机处理
- 选举计时器
- 心跳机制
- 日志管理
- RPC 通信

### 🌐 RaftServer

提供 RPC 服务，包括：
- `AppendEntries` - 日志复制和心跳
- `RequestVote` - 领导者选举
- `InstallSnapshot` - 快照传输

### 📔 RaftLog

日志存储和管理，功能包括：
- 日志记录的添加与应用
- 日志条目持久化
- 日志压缩

### 💾 RaftStorage

持久化存储，包括：
- 当前任期（currentTerm）
- 投票记录（votedFor）
- 日志条目（logs）
- 快照数据（snapshot）

## 🔄 状态机转换

Raft 节点在以下三种状态间转换：

1. **📡 Follower**
   - 响应领导者的请求
   - 将日志条目应用到状态机
   - 超时后转为 Candidate

2. **🙋 Candidate**
   - 增加当前任期并发起选举
   - 赢得多数投票后成为 Leader
   - 发现更高任期的 Leader 后转为 Follower
   - 选举超时后重新选举

3. **👑 Leader**
   - 处理客户端请求
   - 向所有节点复制日志
   - 向所有节点发送心跳
   - 发现更高任期后转为 Follower

## 🔄 共识流程

### 🗳️ 领导者选举流程

1. Follower 超时后转为 Candidate
2. Candidate 增加任期号并投票给自己
3. Candidate 向其他节点发送 RequestVote RPC
4. 其他节点根据日志新旧程度决定是否投票
5. Candidate 获得多数票后成为 Leader
6. Leader 开始发送心跳阻止新的选举

### 📝 日志复制流程

1. Leader 接收客户端请求
2. Leader 将请求追加到本地日志
3. Leader 向所有 Follower 发送 AppendEntries RPC
4. Leader 等待多数节点确认
5. Leader 将条目应用到自己的状态机并返回结果
6. Leader 通知 Follower 提交日志

## ⚙️ 配置选项

```go
type Config struct {
    // 节点ID
    NodeID uint64
    
    // 集群成员地址
    PeerAddrs map[uint64]string
    
    // 选举超时时间范围（毫秒）
    ElectionTimeoutMin int
    ElectionTimeoutMax int
    
    // 心跳间隔（毫秒）
    HeartbeatInterval int
    
    // 数据存储目录
    DataDir string
    
    // 快照相关配置
    SnapshotInterval int
    SnapshotThreshold int
    
    // 应用回调函数
    StateMachine StateMachine
}
```

## 📝 使用示例

```go
// 创建 Raft 节点配置
config := &raft.Config{
    NodeID: 1,
    PeerAddrs: map[uint64]string{
        1: "localhost:9001",
        2: "localhost:9002",
        3: "localhost:9003",
    },
    ElectionTimeoutMin: 150,
    ElectionTimeoutMax: 300,
    HeartbeatInterval: 50,
    DataDir: "./raft-node1",
    StateMachine: &MyStateMachine{}, // 实现 StateMachine 接口
}

// 创建并启动 Raft 节点
node, err := raft.NewRaftNode(config)
if err != nil {
    log.Fatalf("Failed to create raft node: %v", err)
}

// 启动节点
if err := node.Start(); err != nil {
    log.Fatalf("Failed to start raft node: %v", err)
}

// 提交命令到 Raft 集群
future := node.Propose([]byte("set key value"))
if err := future.Error(); err != nil {
    log.Printf("Failed to propose: %v", err)
} else {
    log.Printf("Command applied with index: %d", future.Index())
}

// 优雅关闭
node.Stop()
```

## 🤖 状态机接口

用户需要实现 `StateMachine` 接口来处理命令：

```go
type StateMachine interface {
    // 应用日志到状态机
    Apply([]byte) interface{}
    
    // 创建快照
    Snapshot() ([]byte, error)
    
    // 从快照恢复
    Restore([]byte) error
}
```

## 🛡️ 容错与恢复

Raft 集群可以容忍少于半数节点失效的情况。例如：

- 3节点集群可容忍1个节点失效
- 5节点集群可容忍2个节点失效
- 7节点集群可容忍3个节点失效

节点失效恢复后，将通过日志复制或快照恢复数据并重新加入集群。

## ⚡ 性能优化

- 📦 批量处理日志条目
- 🚿 流水线复制（Pipeline Replication）
- 🗜️ 日志压缩与快照
- 🗳️ 预投票机制（Pre-Vote）

## 💡 使用建议

- 推荐使用奇数个节点，如3、5、7个
- 考虑网络延迟对选举超时的影响
- 定期备份 Raft 日志和状态
- 监控节点状态和集群健康度 