package io.github.origincoding.gossipglomers.challenges.kafka

import io.github.origincoding.gossipglomers.clients.kv.KvClient
import io.github.origincoding.gossipglomers.clients.kv.compareAndSwapAs
import io.github.origincoding.gossipglomers.clients.kv.kv
import io.github.origincoding.gossipglomers.clients.kv.readAsOrDefault
import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.ErrorCode
import io.github.origincoding.gossipglomers.runtime.Node
import io.github.origincoding.gossipglomers.runtime.RpcException
import io.github.origincoding.gossipglomers.runtime.RpcTimeoutException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.Serializable

@Serializable
data class SendBody(val key: String, val msg: Long)

@Serializable
data class SendOkBody(val offset: Long)

@Serializable
data class PollBody(val offsets: Map<String, Long>)

// 协议中的每条消息是 [offset, msg] 数组，不能直接使用序列化为对象的 Pair。
@Serializable
data class PollOkBody(val msgs: Map<String, List<List<Long>>>)

@Serializable
data class CommitOffsetsBody(val offsets: Map<String, Long>)

@Serializable
data class ListCommittedOffsetsBody(val keys: List<String>)

@Serializable
data class ListCommittedOffsetsOkBody(val offsets: Map<String, Long>)

// 仅用于节点内排队，不参与 RPC 序列化。
private data class PendingSend(val msg: Long, val result: CompletableDeferred<Long>)

/** 三种写入策略共用 lin-kv 日志，列表下标即消息的 offset。 */
class KafkaNode(private val node: Node, private val mode: KafkaMode = KafkaMode.BATCH) {
    private val nodes = node.nodeIds.sorted()

    private val keyMutexes = mutableMapOf<String, Mutex>()

    private val outbox = mutableMapOf<String, Channel<PendingSend>>()

    private val store = node.kv("lin-kv")
    private val String.logKey get() = "logs-${this}"
    private val String.committedOffsetKey get() = "committed-offsets-${this}"

    fun register() {
        node.on<SendBody, SendOkBody>("send") { request ->
            val owner = if (mode == KafkaMode.DIRECT) node.id else ownerOf(request.body.key)

            if (owner == node.id) {
                SendOkBody(acceptLocalMessage(request.body))
            } else {
                node.rpc<SendBody, SendOkBody>(owner, "forward", request.body)
            }
        }

        node.on<SendBody, SendOkBody>("forward") { request ->
            SendOkBody(acceptLocalMessage(request.body))
        }

        node.on<PollBody, PollOkBody>("poll") { request ->
            val messages = coroutineScope {
                request.body.offsets.map { (key, offset) -> async { key to readMessages(key, offset) } }
            }.awaitAll().toMap()
            PollOkBody(messages)
        }

        node.on<CommitOffsetsBody, EmptyBody>("commit_offsets") { request ->
            coroutineScope {
                request.body.offsets.forEach { (key, offset) -> launch { commitOffset(store, key, offset) } }
            }
            EmptyBody
        }

        node.on<ListCommittedOffsetsBody, ListCommittedOffsetsOkBody>("list_committed_offsets") { request ->
            val offsets = coroutineScope {
                request.body.keys.map { key ->
                    async {
                        store.readAsOrDefault<Long?>(key.committedOffsetKey, null)?.let { offset -> key to offset }
                    }
                }
            }.awaitAll().filterNotNull().toMap()
            ListCommittedOffsetsOkBody(offsets)
        }
    }

    private suspend fun acceptLocalMessage(body: SendBody): Long = when (mode) {
        KafkaMode.DIRECT -> appendBatch(body.key, listOf(body.msg))
        KafkaMode.HASH -> keyMutexes.getOrPut(body.key) { Mutex() }.withLock {
            appendBatch(body.key, listOf(body.msg))
        }
        KafkaMode.BATCH -> enqueueMessage(body)
    }

    private suspend fun enqueueMessage(body: SendBody): Long {
        val result = CompletableDeferred<Long>()
        queueFor(body.key).send(PendingSend(body.msg, result))
        return result.await()
    }

    private fun queueFor(key: String): Channel<PendingSend> = outbox.getOrPut(key) {
        val channel = Channel<PendingSend>(64)
        node.background { consumeMessages(key, channel) }
        channel
    }

    private suspend fun consumeMessages(key: String, channel: Channel<PendingSend>) {
        while (true) {
            // 队列为空时，在这里挂起等待。
            val first = channel.receive()
            val batch = mutableListOf(first)

            // 取走当前已有的消息，不主动等待。
            while (batch.size < 64) {
                val next = channel.tryReceive().getOrNull() ?: break
                batch.add(next)
            }

            try {
                val baseOffset = appendBatch(key, batch.map { it.msg })

                batch.forEachIndexed { index, pending ->
                    pending.result.complete(baseOffset + index.toLong())
                }
            } catch (e: RpcException) {
                batch.forEach {
                    it.result.completeExceptionally(e)
                }
            } catch (e: RpcTimeoutException) {
                batch.forEach {
                    it.result.completeExceptionally(e)
                }
            }
        }
    }

    private suspend fun appendBatch(key: String, messages: List<Long>): Long {
        val storageKey = key.logKey

        while (true) {
            val before = store.readAsOrDefault<List<Long>>(storageKey, emptyList())
            val after = before + messages

            try {
                store.compareAndSwapAs(storageKey, before, after, createIfNotExists = true)
                return before.size.toLong()
            } catch (e: RpcException) {
                if (e.code != ErrorCode.PRECONDITION_FAILED) {
                    throw e
                }
            }
        }
    }

    private suspend fun commitOffset(store: KvClient, key: String, offset: Long) {
        val storageKey = key.committedOffsetKey

        while (true) {
            val before = store.readAsOrDefault<Long?>(storageKey, null)
            if (before != null && before >= offset) {
                return
            }

            try {
                store.compareAndSwapAs<Long?>(storageKey, before, offset, createIfNotExists = true)
                return
            } catch (e: RpcException) {
                if (e.code != ErrorCode.PRECONDITION_FAILED) throw e
            }
        }
    }

    private suspend fun readMessages(key: String, offset: Long): List<List<Long>> {
        val log = store.readAsOrDefault<List<Long>>(key.logKey, emptyList())

        return log.mapIndexedNotNull { index, msg ->
            if (index.toLong() >= offset) {
                listOf(index.toLong(), msg)
            } else {
                null
            }
        }
    }

    private fun ownerOf(key: String): String {
        val index = Math.floorMod(key.hashCode(), nodes.size)
        return nodes[index]
    }
}
