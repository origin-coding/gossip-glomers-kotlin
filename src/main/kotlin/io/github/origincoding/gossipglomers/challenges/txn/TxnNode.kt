package io.github.origincoding.gossipglomers.challenges.txn

import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.ErrorCode
import io.github.origincoding.gossipglomers.runtime.Node
import io.github.origincoding.gossipglomers.runtime.RpcException
import io.github.origincoding.gossipglomers.runtime.RpcTimeoutException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val MAX_REPLICATION_BATCH_SIZE = 64

private data class VersionedValue(
    val value: Long,
    val version: Version,
)

class TxnNode(private val node: Node) {
    private val committed = mutableMapOf<Long, VersionedValue>()
    private var clock = 0L
    private val outboxes = node.nodeIds
        .filter { it != node.id }
        .associateWith {
            Channel<ReplicatedTxn>(Channel.UNLIMITED)
        }

    fun register() {
        node.on<TxnBody, TxnOkBody>("txn") { request ->
            execute(request.body.txn)
        }
        node.on<ReplicateBody, EmptyBody>("replicate") { request ->
            request.body.transactions.forEach { txn ->
                observe(txn.version)
                apply(txn)
            }
            EmptyBody
        }
        outboxes.forEach { (peer, queue) -> node.background { replicate(peer, queue) } }
    }

    // 整笔事务在同一事件循环中执行；读取到提交之间不能挂起。
    private fun execute(operations: List<Operation>): TxnOkBody {
        val writes = mutableMapOf<Long, Long>()
        val results = operations.map { operation ->
            val key = operation.key

            when (operation) {
                is Operation.Read -> OperationResult.Read(key, writes[key] ?: committed[key]?.value)
                is Operation.Write -> {
                    writes[key] = operation.value
                    OperationResult.Write(key, operation.value)
                }
            }
        }

        if (writes.isNotEmpty()) {
            val txn = ReplicatedTxn(nextVersion(), writes.toMap())
            apply(txn)

            outboxes.values.forEach { queue ->
                queue.trySend(txn).getOrThrow()
            }
        }
        return TxnOkBody(results)
    }

    private fun apply(txn: ReplicatedTxn) {
        for ((key, value) in txn.writes) {
            val current = committed[key]

            if (current == null || txn.version > current.version) {
                committed[key] = VersionedValue(value, txn.version)
            }
        }
    }

    private suspend fun replicate(peer: String, queue: Channel<ReplicatedTxn>) {
        for (first in queue) {
            // 第一笔由 for 等待；其余只取当前已有的积压，不等待凑满批次。
            val batch = ReplicateBody(buildList {
                add(first)
                while (size < MAX_REPLICATION_BATCH_SIZE) {
                    val next = queue.tryReceive().getOrNull() ?: break
                    add(next)
                }
            })
            var backoff = 100.milliseconds

            while (true) {
                try {
                    node.rpc<ReplicateBody, EmptyBody>(peer, "replicate", batch)
                    break
                } catch (e: RpcTimeoutException) {
                    // ACK 未收到；远端可能已经应用，重发同一个固定批次。
                } catch (e: RpcException) {
                    if (e.code != ErrorCode.TIMEOUT && e.code != ErrorCode.TEMPORARILY_UNAVAILABLE) {
                        throw e
                    }
                }

                delay(backoff)
                backoff = minOf(backoff * 2, 3.seconds)
            }
        }
    }

    // 本地提交时，分配新版本
    private fun nextVersion() = Version(++clock, node.id)

    // 收到远端事务时，推进本地时钟
    private fun observe(version: Version) {
        clock = maxOf(clock, version.counter) + 1
    }
}
