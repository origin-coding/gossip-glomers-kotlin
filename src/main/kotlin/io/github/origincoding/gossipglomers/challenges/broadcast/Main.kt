package io.github.origincoding.gossipglomers.challenges.broadcast

import io.github.origincoding.gossipglomers.runtime.Runtime
import io.github.origincoding.gossipglomers.runtime.Message
import io.github.origincoding.gossipglomers.runtime.NodeContext
import io.github.origincoding.gossipglomers.runtime.inReplyTo
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlin.math.pow
import kotlin.system.exitProcess
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

data class DeliveryKey(val neighbor: String, val message: Long)

data class PendingDelivery(
    var nextRetryAt: Instant = Clock.System.now(),
    var attemptCount: Long = 1,
    val requestIds: MutableSet<Long> = mutableSetOf<Long>()
) {
    init {
        this.nextRetryAt += 250.milliseconds
    }

    fun decay(duration: Duration = 250.milliseconds) {
        val expDecay = 2.0.pow(attemptCount.toDouble()) * duration.inWholeMilliseconds.toDouble()
        val maxDecay = 3000.toDouble()

        this.nextRetryAt = Clock.System.now() + (minOf(expDecay, maxDecay)).milliseconds
        this.attemptCount += 1
    }
}

class BroadcastNode {
    private val messages = mutableSetOf<Long>()

    // 用来存储、查找广播信息
    private val pendingDeliveries = mutableMapOf<DeliveryKey, PendingDelivery>()
    private val requestIndex = mutableMapOf<Long, DeliveryKey>()

    fun register(runtime: Runtime) {
        runtime.on("broadcast", ::onBroadcast)
        runtime.on("read", ::onRead)
        runtime.on("broadcast_ok", ::onBroadcastOk)
        runtime.onTick(::onTick)
    }

    private fun onBroadcast(
        node: NodeContext,
        request: Message<JsonObject>,
    ) {
        val message = node.decode<BroadcastBody>(request).message
        val added = messages.add(message)
        if (added) {
            node.neighbors.forEach { neighbor ->
                val requestId = node.send(neighbor, BroadcastBody(message))

                val key = DeliveryKey(neighbor, message)
                val value = PendingDelivery(requestIds = mutableSetOf(requestId))
                pendingDeliveries[key] = value
                requestIndex[requestId] = key
            }
        }

        node.reply(request, BroadcastOkBody())
    }

    private fun onBroadcastOk(
        node: NodeContext,
        request: Message<JsonObject>
    ) {
        val inReplyTo = request.inReplyTo
        val deliveryKey = requestIndex[inReplyTo] ?: return
        val pendingDelivery = pendingDeliveries[deliveryKey] ?: return

        pendingDelivery.requestIds.forEach { requestIndex.remove(it) }
        pendingDeliveries.remove(deliveryKey)

        val body = node.decode<BroadcastOkBody>(request)
    }

    private fun onTick(node: NodeContext) = pendingDeliveries
        .filterValues { it.nextRetryAt <= Clock.System.now() }
        .forEach { (key, value) ->
            value.decay()
            // 这里先省略指数退后策略、超时判定等代码。
            val requestId = node.send(key.neighbor, BroadcastBody(key.message))
            value.requestIds.add(requestId)
            requestIndex[requestId] = key
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
    val runtime = Runtime()
    val broadcastNode = BroadcastNode()
    broadcastNode.register(runtime)
    runtime.run()
}
