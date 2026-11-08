package io.github.origincoding.gossipglomers.runtime

import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put

internal class NodeHarness(scope: CoroutineScope, nodeIds: List<String> = listOf("n1", "n2")) {
    val sent = Channel<Envelope>(Channel.UNLIMITED)
    val node = Node("n1", nodeIds, Json { ignoreUnknownKeys = true; encodeDefaults = true }, scope, sent::send)

    suspend fun next(): Envelope = withTimeout(5.seconds) { sent.receive() }

    fun respond(
        request: Envelope,
        body: JsonObject = JsonObject(emptyMap()),
        type: String = "${request.type}_ok",
        source: String = request.dest,
    ) {
        node.acceptResponse(Envelope(source, node.id, buildJsonObject {
            body.forEach { (key, value) -> put(key, value) }
            put("type", type)
            put("in_reply_to", requireNotNull(request.msgId))
        }))
    }
}

internal fun incoming(type: String, id: Long, body: JsonObject = JsonObject(emptyMap())) =
    Envelope("c1", "n1", buildJsonObject {
        body.forEach { (key, value) -> put(key, value) }
        put("type", type)
        put("msg_id", id)
    })
