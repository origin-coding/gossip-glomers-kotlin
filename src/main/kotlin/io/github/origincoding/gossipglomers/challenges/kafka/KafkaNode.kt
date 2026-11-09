package io.github.origincoding.gossipglomers.challenges.kafka

import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.Node
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

/** 5a：单节点内存日志，列表下标即消息的 offset。 */
class KafkaNode(private val node: Node) {
    private val logs = mutableMapOf<String, MutableList<Long>>()
    private val committedOffsets = mutableMapOf<String, Long>()

    fun register() {
        node.on<SendBody, SendOkBody>("send") { request ->
            val log = logs.getOrPut(request.body.key) { mutableListOf() }
            log.addLast(request.body.msg)
            SendOkBody(log.lastIndex.toLong())
        }

        node.on<PollBody, PollOkBody>("poll") { request ->
            val messages = request.body.offsets.mapValues { (key, offset) ->
                logs[key].orEmpty().mapIndexedNotNull { index, msg ->
                    if (index.toLong() >= offset) listOf(index.toLong(), msg) else null
                }
            }
            PollOkBody(messages)
        }

        node.on<CommitOffsetsBody, EmptyBody>("commit_offsets") { request ->
            committedOffsets.putAll(request.body.offsets)
            EmptyBody
        }

        node.on<ListCommittedOffsetsBody, ListCommittedOffsetsOkBody>("list_committed_offsets") { request ->
            val offsets = request.body.keys.mapNotNull { key ->
                committedOffsets[key]?.let { offset -> key to offset }
            }.toMap()
            ListCommittedOffsetsOkBody(offsets)
        }
    }
}
