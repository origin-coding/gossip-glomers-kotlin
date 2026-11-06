package io.github.origincoding.gossipglomers.challenges.broadcast

import io.github.origincoding.gossipglomers.runtime.Runtime
import io.github.origincoding.gossipglomers.runtime.Message
import io.github.origincoding.gossipglomers.runtime.NodeContext
import io.github.origincoding.gossipglomers.runtime.inReplyTo
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlin.math.pow
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant

@Serializable
data class BroadcastBody(
    val message: Long,
    val type: String = "broadcast"
)

@Serializable
data class BroadcastOkBody(
    val type: String = "broadcast_ok"
)

@Serializable
data class ReadOkBody(
    val messages: List<Long>,
    val type: String = "read_ok",
)

@Serializable
data class GossipBody(
    val messages: Set<Long>,
    val type: String = "gossip"
)

@Serializable
data class GossipOkBody(
    val type: String = "gossip_ok"
)

data class OutgoingBuffer(
    val messages: MutableSet<Long> = mutableSetOf(),
    val flushAt: Instant = Clock.System.now() + 100.milliseconds
)

data class PendingBatch(
    val neighbor: String,
    val messages: Set<Long>,
    val requestIds: MutableSet<Long> = mutableSetOf(),
    var attemptCount: Long = 0,
    var nextRetryAt: Instant = Clock.System.now() + 250.milliseconds,
) {
    fun scheduleNextRetry(duration: Duration = 250.milliseconds) {
        val expDecay = 2.0.pow(attemptCount.toDouble()) * duration.inWholeMilliseconds.toDouble()
        val maxDecay = 3000.toDouble()

        this.nextRetryAt = Clock.System.now() + (minOf(expDecay, maxDecay)).milliseconds
        this.attemptCount += 1
    }
}

class BroadcastNode {
    private val messages = mutableSetOf<Long>()

    private var nextBatchId: Long = 0L
    private val outboxByNeighbor = mutableMapOf<String, OutgoingBuffer>()
    private val pendingBatches = mutableMapOf<Long, PendingBatch>()
    private val batchIdByRequestId = mutableMapOf<Long, Long>()

    fun register(runtime: Runtime) {
        runtime.on("broadcast", ::onBroadcast)
        runtime.on("read", ::onRead)
        runtime.on("broadcast_ok", ::onBroadcastOk)
        runtime.on("gossip", ::onGossip)
        runtime.on("gossip_ok", ::onGossipOk)
        runtime.onTick(::onTick)
    }

    private fun onBroadcast(
        node: NodeContext,
        request: Message<JsonObject>,
    ) {
        val message = node.decode<BroadcastBody>(request).message
        acceptValue(node, message, request.src)
        node.reply(request, BroadcastOkBody())
    }

    private fun onGossip(node: NodeContext, request: Message<JsonObject>) {
        val messages = node.decode<GossipBody>(request).messages
        messages.forEach { message -> acceptValue(node, message, request.src) }
        node.reply(request, GossipOkBody())
    }

    private fun acceptValue(node: NodeContext, message: Long, source: String) {
        if (!messages.add(message)) {
            return
        }

        node.neighbors.filter { it != source }.forEach { neighbor ->
            outboxByNeighbor.getOrPut(neighbor) { OutgoingBuffer() }.also { it.messages.add(message) }
        }
    }

    private fun onBroadcastOk(
        node: NodeContext,
        request: Message<JsonObject>
    ) {
        val body = node.decode<BroadcastOkBody>(request)
    }

    private fun onTick(node: NodeContext) {
        flushDueOutboxes(node)
        retryDueBatches(node)
    }

    private fun flushDueOutboxes(node: NodeContext) = outboxByNeighbor
        .filterValues { it.flushAt <= Clock.System.now() }
        .forEach { (neighbor, value) ->
            val batchId = this.nextBatchId++
            val pendingBatch = PendingBatch(neighbor, value.messages.toSet()).also { pendingBatches[batchId] = it }
            sendBatchAttempt(node, batchId, pendingBatch)
            outboxByNeighbor.remove(neighbor)
        }

    private fun retryDueBatches(node: NodeContext) {
        val now = Clock.System.now()
        pendingBatches.filterValues { it.nextRetryAt < now }.forEach { (batchId, pendingBatch) ->
            sendBatchAttempt(node, batchId, pendingBatch)
        }
    }

    private fun sendBatchAttempt(node: NodeContext, batchId: Long, pendingBatch: PendingBatch) {
        val requestId = node.send(pendingBatch.neighbor, GossipBody(pendingBatch.messages))

        pendingBatch.requestIds.add(requestId)
        pendingBatch.scheduleNextRetry()
        batchIdByRequestId[requestId] = batchId
    }

    private fun onGossipOk(node: NodeContext, request: Message<JsonObject>) {
        val inReplyTo = request.inReplyTo
        val batchId = batchIdByRequestId[inReplyTo] ?: return
        val pendingBatch = pendingBatches[batchId] ?: return

        if (pendingBatch.neighbor != request.src) {
            return
        }

        pendingBatch.requestIds.forEach { id -> batchIdByRequestId.remove(id) }
        pendingBatches.remove(batchId)
    }

    private fun onRead(
        node: NodeContext,
        request: Message<JsonObject>,
    ) {
        node.reply(request, ReadOkBody(messages.toList()))
    }
}

/** Stage 03: Broadcast. Implement the node here. */
fun run(): Unit {
    val runtime = Runtime(tickMillis = 25)
    val broadcastNode = BroadcastNode().apply { register(runtime) }
    runtime.run()
}
