package io.github.origincoding.gossipglomers.challenges.kafka

import io.github.origincoding.gossipglomers.clients.kv.KvClient
import io.github.origincoding.gossipglomers.clients.kv.compareAndSwapAs
import io.github.origincoding.gossipglomers.clients.kv.kv
import io.github.origincoding.gossipglomers.clients.kv.readAsOrDefault
import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.ErrorCode
import io.github.origincoding.gossipglomers.runtime.Node
import io.github.origincoding.gossipglomers.runtime.Request
import io.github.origincoding.gossipglomers.runtime.RpcException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
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

/** 5b：基于 lin-kv 的共享日志，列表下标即消息的 offset。 */
class KafkaNode(private val node: Node) {
    private val store = node.kv("lin-kv")

    private val String.logKey get() = "logs-${this}"
    private val String.committedOffsetKey get() = "committed-offsets-${this}"

    fun register() {
        node.on<SendBody, SendOkBody>("send") { request ->
            SendOkBody(acceptMessage(store, request))
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

    private suspend fun acceptMessage(store: KvClient, request: Request<SendBody>): Long {
        val storageKey = request.body.key.logKey

        while (true) {
            val before = store.readAsOrDefault<List<Long>>(storageKey, emptyList())
            val after = before + request.body.msg
            try {
                store.compareAndSwapAs(storageKey, before, after, createIfNotExists = true)
                return before.size.toLong()
            } catch (e: RpcException) {
                if (e.code != ErrorCode.PRECONDITION_FAILED) throw e
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
}
