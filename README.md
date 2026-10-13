# im —— 一个硬核的 IM 内核原型

[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm%20Noncommercial%201.0.0-blue.svg)](LICENSE)
[![GitHub Repo](https://img.shields.io/badge/repo-github.com/qi719/im--kernel-black.svg)](https://github.com/qi719/im-kernel)

> 这是一个 **内核原型**，不是生产级 IM。组件不完善（没有认证/加密/群管理/断线重连），
> 但内核设计思想偏极致性能：**io_uring 网络 IO + Chronicle Queue 消息日志 + MPSC 无锁队列解耦**。
> 适合用来学习/压测"低延迟消息系统"的底层构造方式。

---

## 核心思想

把 IM 拆成三段独立的异步流水线，每一段都选了当前 Java 生态里最硬核的实现：

```
                          Netty EventLoop (io_uring)
                          │  只做 read/write，不碰业务
                          ▼
                  ┌───────────────────────┐
                  │  MPSC 无锁队列          │   JCTools MpscUnboundedXaddArrayQueue
                  │  (多生产者/单消费者)    │   Netty IO 线程 → 业务线程，无锁投递
                  └───────────────────────┘
                          ▼
                  ┌───────────────────────┐
                  │  MessageProducerProc  │   单消费者线程，串行处理事件
                  │  (业务分发)            │   → User.sendMessage()
                  └───────────────────────┘
                          ▼
                  ┌───────────────────────┐
                  │  Chronicle Queue      │   消息落地：append-only 顺序写
                  │  (持久化日志)          │   每用户一个 ExcerptTailer 游标
                  └───────────────────────┘
                          ▼
                  ┌───────────────────────┐
                  │  MessageConsumerProc  │   轮询所有在线用户游标
                  │  (推送线程)            │   → ctx.writeAndFlush() 回网络
                  └───────────────────────┘
```

三段流水线相互解耦：
- **写入路径** 是 append-only 顺序写，配合 `doubleBuffer + ASYNC` 同步模式，写入延迟极低。
- **读取路径** 每个用户拥有独立 Tailer（类似 Kafka consumer offset），天然支持多端同步、离线消息、历史回放。
- **网络路径** 由 Netty + io_uring 承担，IO 线程只搬数据，不阻塞。

---

## 技术栈

| 层级 | 选型 | 作用 |
|------|------|------|
| 网络 IO | Netty 4.2 + `IoUringIoHandler` | Linux io_uring 异步IO，multishot accept、Buffer Ring、系统调用批处理 |
| ByteBuf 池 | `PooledByteBufAllocator` | 直接内存池化，降低 GC 压力 |
| 事件投递 | JCTools `MpscUnboundedXaddArrayQueue` | 多生产者单消费者无锁队列，Netty IO线程 → 业务线程无锁传递 |
| 消息存储 | Chronicle Queue (BINARY_LIGHT) | 日志结构持久化，每用户独立游标 |
| 并发结构 | JDK `ConcurrentHashMap` / `AtomicBoolean` | 用户表/在线连接表/状态标记 |

---

## 为什么硬核

1. **io_uring 而非 epoll/NIO**
   - 开启 `SO_REUSEPORT`，多 EventLoop 共享监听端口。
   - `IoUringBufferRingConfig`：8192 buffer ring、batch 4096、`batchAllocation=true`，配合 `IoUringAdaptiveBufferRingAllocator`，把"接收数据"做成零拷贝批处理。
   - `IoUring.isAcceptMultishotEnabled()` —— multishot accept，一次 syscall 接收多个连接。
   - Ring size 自动 ×4，进一步批处理化。

2. **Netty IO 线程零业务**
   - `MessageHandler` 收到字符串只做 `split` 并丢进 MPSC 队列，立刻返回。
   - IO 线程不做任何阻塞、不落地、不推送，吞吐不受业务拖累。

3. **MPSC 无锁队列**
   - `MpscUnboundedXaddArrayQueue`：多线程（多个 EventLoop）写入单线程（业务线程）读取，无锁基于 CAS，XADD 变体减少争用。
   - 相比 `ArrayBlockingQueue` 的锁，吞吐高一个数量级。

4. **Chronicle Queue 作为消息日志**
   - `WireType.BINARY_LIGHT` 二进制轻量级编码，省 CPU。
   - `doubleBuffer(true)` + `syncMode(ASYNC)`：写入即返回，落盘异步，延迟亚毫秒级。
   - `RollCycles.FAST_DAILY`：按日滚动，文件可清理。
   - 每个 `ExcerptTailer` 用 `userId` 作为 id 命名，**每用户独立游标**，完全等价于 Kafka 的 consumer offset。

5. **池化 + 直接内存**
   - 服务端 `PooledByteBufAllocator(true, 1, 1, 8192, 11, 0, 0, true)`：单 pool arena、默认 8K chunk、11 阶 subpage，专为小消息高频分配调优。
   - `SO_SNDBUF=16MB / SO_RCVBUF=1MB`，TCP 缓冲拉大，配合 `TCP_NODELAY` 关闭 Nagle。

---

## 快速开始

### 前置要求

- **Linux 内核 ≥ 5.1（建议 5.10+）**，io_uring 必需，Windows/macOS 无法运行服务端。
- JDK 17+（Netty 4.2 + io_uring native 需要较新 JDK）。
- Maven 3.6+。
- 修改 [MessageRoom.java](src/main/java/com/qcs/IM/MessageRoom.java) 中的 `ChronicleQueue.singleBuilder("/home/qcs/message/")` 路径，改成你本机有权限的目录。

### 构建

```bash
mvn clean package
```

会生成 `target/im-1.0-SNAPSHOT.jar`（maven-shade-plugin 已配置 mainClass）。

### 运行服务端

```bash
java -jar target/im-1.0-SNAPSHOT.jar
# 监听 0.0.0.0:7090
```

启动后控制台应输出：
```
accept multishot enabled
onAcquired
```

### 运行客户端（压测）

修改 [NettyClient.java](src/main/java/com/qcs/IM/NettyClient.java) 中 `host` 为服务端 IP，然后：

```bash
mvn exec:java -Dexec.mainClass="com.qcs.IM.NettyClient"
```

客户端会一次性发送 9999 条消息，服务端 `MessageConsumerProc` 在收满 9999 条时打印端到端耗时（纳秒级精度）。

---

## 消息协议

文本行协议，`|` 分隔，`LineBasedFrameDecoder(256)` 按行切包：

```
userId|type|roomId|content
```

| type | 含义 | 说明 |
|------|------|------|
| `0`  | 登录 | 把 `ChannelHandlerContext` 注册到 `onLineChannels`，标记用户上线 |
| `1`  | 发消息 | 写入 `userId` 对应房间的 Chronicle Queue |

示例：
```
1|0
1|1|1|hello world
```

---

## 项目结构

```
src/main/java/com/qcs/IM/
├── MessageMain.java          # 启动入口，Netty ServerBootstrap + io_uring 配置
├── MessageHandler.java       # Netty 入站 Handler，解析协议并投递到 MPSC 队列
├── MessageProducerProc.java  # 单消费者线程，处理登录/发消息事件
├── MessageConsumerProc.java  # 推送线程，轮询 Tailer 把新消息写回客户端
├── MessageRoom.java          # 房间，封装一个 ChronicleQueue 实例
├── User.java                 # 用户，维护自己每间房的 appender / tailer
├── IoUringConfig.java        # io_uring 配置：Buffer Ring / ring size / multishot
├── NettyClient.java          # 压测客户端（NioEventLoopGroup，9999 条消息）
└── Test.java                 # MPSC vs ThreadPool 的小基准对比
```

---

## 性能基准

代码内置两处计时点：
- [MessageConsumerProc.java](src/main/java/com/qcs/IM/MessageConsumerProc.java#L17-L24)：当 `counter == 9999` 时记录端到端耗时。
- [NettyClient.java](src/main/java/com/qcs/IM/NettyClient.java#L47-L56)：记录客户端发送总耗时。

具体数字依赖机器与 io_uring 内核版本，本项目不绑定具体性能结论，请自行压测。
重点关注的是**架构能否消除 IO 线程阻塞、能否让写入路径走 append-only**，而不是单次跑分。

---

## 已知局限（开源前必看）

这个项目是**内核原型**，以下能力**没有实现**，别在生产里用：

- 无用户认证、无 TLS/加密，协议是明文 `|` 分隔字符串。
- 用户与房间在 `MessageMain` 里**硬编码**，没有动态注册接口。
- 没有断线重连、心跳、连接超时回收。
- 没有群聊语义（`MessageRoom` 只是日志载体，成员管理逻辑未做）。
- 没有背压（`AUTO_READ` 被注释，慢消费者可能 OOM）。
- `MessageConsumerProc` 用 `Thread.yield()` 自旋，CPU 占用偏高，生产环境应换成 `WaitStrategy` 或者 `LongAdder` + blocking 策略。
- Chronicle Queue 路径硬编码 `/home/qcs/message/`，跨机部署需要改。
- 客户端 [NettyClient.java](src/main/java/com/qcs/IM/NettyClient.java) 只是压测脚本，不是可用客户端。

---

## 路线图（欢迎 PR）

- [ ] 把硬编码用户/房间改成动态接口
- [ ] 引入 TLS 与简单鉴权
- [ ] 群聊成员管理 + 离线消息拉取 API
- [ ] `MessageConsumerProc` 改为可阻塞/事件驱动，避免空转
- [ ] 背压：打开 `AUTO_READ=false` + isWritable 钩子
- [ ] 跨平台 Profile 模式：非 Linux 自动降级到 epoll

---

## License

本项目采用 **PolyForm Noncommercial License 1.0.0**，详见 [LICENSE](LICENSE)。

简单说：

- ✅ 个人学习、研究、教育用途、内部测试都可以
- ✅ 可以修改、可以再分发
- ❌ **禁止商用**（不允许直接售卖，也不允许作为商业产品/服务的一部分使用）
- ❌ 不能用于商业目的，除非另行获取商业授权

> 注：作者本人也清楚这个限制**实际上难以强制执行**，但出于开源动机明确性的考虑保留此条款。
> 若你有商业用途需求，可联系作者单独协商。
