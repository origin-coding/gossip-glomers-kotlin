package io.github.origincoding.gossipglomers.challenges.broadcast

import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.ErrorCode
import io.github.origincoding.gossipglomers.runtime.Node
import io.github.origincoding.gossipglomers.runtime.RpcException
import io.github.origincoding.gossipglomers.runtime.RpcTimeoutException
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.serialization.Serializable

@Serializable
data class BroadcastBody(val message: Long)

@Serializable
data class ReadOkBody(val messages: List<Long>)

@Serializable
data class GossipBody(val messages: Set<Long>)

@Serializable
data class TopologyBody(val topology: Map<String, List<String>>)

class BroadcastNode(private val node: Node, private val topology: Topology = Topology.Star) {
    private class Outbox {
        val messages = mutableSetOf<Long>()
        val wakeups = Channel<Unit>(Channel.CONFLATED)
    }

    private val messages = mutableSetOf<Long>()
    private val ready = CompletableDeferred<Map<String, Outbox>>()

    fun register() {
        node.on<TopologyBody, EmptyBody>("topology") { request ->
            val neighbors = topology.neighbors(node.id, node.nodeIds, request.body.topology)
            if (!ready.isCompleted) {
                val outboxes = neighbors.associateWith { Outbox() }
                ready.complete(outboxes)
                outboxes.forEach { (neighbor, outbox) ->
                    node.background { replicate(neighbor, outbox) }
                }
            } else {
                require(ready.await().keys == neighbors) { "Changing broadcast topology is not supported" }
            }
            EmptyBody
        }
        node.on<BroadcastBody, EmptyBody>("broadcast") { request ->
            accept(request.body.message, request.src, ready.await())
            EmptyBody
        }
        node.on<GossipBody, EmptyBody>("gossip") { request ->
            val outboxes = ready.await()
            request.body.messages.forEach { accept(it, request.src, outboxes) }
            EmptyBody
        }
        node.on<EmptyBody, ReadOkBody>("read") { ReadOkBody(messages.toList()) }
    }

    private fun accept(value: Long, source: String, outboxes: Map<String, Outbox>) {
        if (!messages.add(value)) return
        outboxes.forEach { (neighbor, outbox) ->
            if (neighbor != source) {
                outbox.messages.add(value)
                outbox.wakeups.trySend(Unit).getOrThrow()
            }
        }
    }

    private suspend fun replicate(neighbor: String, outbox: Outbox) {
        for (signal in outbox.wakeups) {
            delay(100.milliseconds)
            if (outbox.messages.isEmpty()) continue

            // 等待 ACK 期间的新消息留在 outbox，不会被旧批次的确认清除。
            val batch = GossipBody(outbox.messages.toSet())
            outbox.messages.clear()
            var backoff = 250.milliseconds
            while (true) {
                try {
                    node.rpc<GossipBody, EmptyBody>(neighbor, "gossip", batch)
                    break
                } catch (_: RpcTimeoutException) {
                    // 集合合并幂等，只有这里明确允许重发同一个批次。
                } catch (e: RpcException) {
                    if (e.code != ErrorCode.TIMEOUT && e.code != ErrorCode.TEMPORARILY_UNAVAILABLE) throw e
                }
                delay(backoff)
                backoff = minOf(backoff * 2, 3.seconds)
            }
        }
    }
}
