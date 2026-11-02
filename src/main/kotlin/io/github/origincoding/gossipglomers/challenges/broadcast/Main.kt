package io.github.origincoding.gossipglomers.challenges.broadcast

import io.github.origincoding.gossipglomers.runtime.Runtime
import io.github.origincoding.gossipglomers.runtime.Message
import io.github.origincoding.gossipglomers.runtime.NodeContext
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlin.system.exitProcess

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

class BroadcastNode {
    private val messages = mutableSetOf<Long>()

    fun register(runtime: Runtime) {
        runtime.on("broadcast", ::onBroadcast)
        runtime.on("read", ::onRead)
        runtime.on("broadcast_ok", ::onBroadcastOk)
    }

    private fun onBroadcast(
        node: NodeContext,
        request: Message<JsonObject>,
    ) {
        val body = node.decode<BroadcastBody>(request)
        val added = messages.add(body.message)

        if (added) {
            val broadcast = BroadcastBody(body.message)
            node.sendToNeighbors(broadcast)
        }

        node.reply(request, BroadcastOkBody())
    }

    private fun onBroadcastOk(
        node: NodeContext,
        request: Message<JsonObject>
    ) {}

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
