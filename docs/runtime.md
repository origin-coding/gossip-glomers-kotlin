# 节点与 RPC

`Runtime` 只处理标准输入输出、`init` 和节点任务生命周期。`Node` 注册请求处理器，
关联 RPC 响应，并提供后台任务入口。Topology、邻居和传播策略保存在 Broadcast 下。

```kotlin
Runtime().run {
    on<EchoBody, EchoOkBody>("echo") { request ->
        EchoOkBody(request.body.echo)
    }
}
```

请求 DTO 只声明业务字段，响应 DTO 同样不声明 `type`、`msg_id` 或 `in_reply_to`。
`Node` 自动解码请求、填写响应头，并回复 handler 的返回值；默认响应类型为 `${type}_ok`，
可通过 `on` 的 `responseType` 参数覆盖。没有业务字段的请求和响应可使用 `EmptyBody`。
`Request.src` 和 `Request.msgId` 可用于业务去重。

## 并发与生命周期

配置函数不可挂起，只用于注册 handler 和启动后台任务。Runtime 使用 `runBlocking`
的单线程事件循环：每个请求启动一个协程，消息路由本身不等待 handler 完成。
`rpc()` 等待时仅挂起调用者，路由继续接收响应。

节点状态在同一线程访问，不需要线程安全集合，但挂起点允许其他操作交错。
跨挂起点的本地原子操作需要业务使用 `Mutex` 或专属工作队列；本地锁不能代替跨节点 CAS。
不要在 handler 中使用 `Thread.sleep`、嵌套 `runBlocking`，或切换到其他调度器修改节点状态。

```kotlin
background {
    // 节点托管的长期任务，可以使用 delay、rpc 和协程取消。
}

every(100.milliseconds) {
    flushBatches()
}
```

`every` 首次等待一个间隔，本次任务结束后再等待下一个间隔，同一任务不会重叠执行。
周期任务不依赖 topology；需要等待拓扑的实验应显式等待自己的就绪信号。

EOF 时取消未完成的 handler、RPC 和后台任务，并排空已经入队的输出。
stdin 读取线程是守护线程，因为阻塞的标准输入不保证响应 interrupt。

## RPC 与错误

```kotlin
val response = rpc<GossipBody, EmptyBody>(neighbor, "gossip", GossipBody(batch))
```

RPC 注册等待记录后发送请求，按 `in_reply_to`、来源和预期响应类型匹配结果。
默认超时 1 秒，包含等待输出队列的时间；可使用 `timeout` 参数覆盖。
重复、迟到、已取消请求的响应会被忽略。每次请求只尝试一次，不自动重试。

- `RpcException` 保留远端错误码，例如 key 不存在（20）、CAS 条件不匹配（22）。
- `RpcTimeoutException` 表示没有收到确认，远端可能已经执行。
- handler 中这两类错误会转换成对应的协议错误响应，超时使用不确定错误码 0。
- 取消正常传播；未捕获的编程错误使整个节点失败，异常输出到 stderr。

后台任务自己决定预期失败的恢复策略。Broadcast 集合合并幂等，因此发送任务可以退避后
重发同一个固定批次；等待 ACK 期间的新值保留给后续批次。KV 不自动采用这一重试策略。

## KV 客户端

```kotlin
import io.github.origincoding.gossipglomers.clients.kv.kv
import io.github.origincoding.gossipglomers.clients.kv.readAs
import io.github.origincoding.gossipglomers.clients.kv.writeAs
import io.github.origincoding.gossipglomers.clients.kv.compareAndSwapAs

val store = kv() // seq-kv；也可以显式传入 lin-kv 等服务地址
val state = store.readAs<MyState>("state")
store.writeAs("state", state)
store.compareAndSwapAs("state", state, updated, createIfNotExists = true)
```

`MyState` 需要可序列化。原始接口 `read/write/compareAndSwap` 使用 `JsonElement`，
另提供 `readLong`。泛型方法只负责编解码，key 不存在时不会自动返回 0，CAS 冲突和超时也不会
自动重试。key 设计、去重、初始化、持久化与一致性由实验负责。gcounter 仍保留未实现入口。
