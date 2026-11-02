package io.github.origincoding.gossipglomers.runtime

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject

class NodeContext(
    val nodeId: String,
    val nodeIds: List<String>,
    val json: Json,
    private val write: (Message<JsonObject>) -> Unit,
) {
    private var nextMessageId = 1L
    private var topologyNeighbors: Set<String>? = null

    val neighbors: Set<String>
        get() = checkNotNull(topologyNeighbors) {
            "Topology has not been initialized"
        }

    internal fun updateNeighbors(neighbors: Collection<String>) {
        topologyNeighbors = neighbors.toSet() - nodeId
    }

    inline fun <reified T> decode(message: Message<JsonObject>): T =
        json.decodeFromJsonElement(message.body)

    inline fun <reified T> send(dest: String, body: T) {
        send(dest, json.encodeToJsonElement(body).jsonObject)
    }

    fun send(dest: String, body: JsonObject) {
        val fields = body.toMutableMap()
        fields["msg_id"] = JsonPrimitive(nextMessageId++)
        write(Message(src = nodeId, dest = dest, body = JsonObject(fields)))
    }

    inline fun <reified T> reply(request: Message<JsonObject>, body: T) {
        reply(request, json.encodeToJsonElement(body).jsonObject)
    }

    fun reply(request: Message<JsonObject>, body: JsonObject) {
        val requestId = requireNotNull(request.msgId) {
            "Cannot reply to a message without msg_id"
        }
        val fields = body.toMutableMap()
        fields["in_reply_to"] = JsonPrimitive(requestId)
        send(request.src, JsonObject(fields))
    }

    inline fun <reified T> sendToNeighbors(body: T, excluding: Set<String> = emptySet()) {
        sendToNeighbors(json.encodeToJsonElement(body).jsonObject, excluding)
    }

    fun sendToNeighbors(body: JsonObject, excluding: Set<String> = emptySet()) {
        for (neighbor in neighbors) {
            if (neighbor !in excluding) {
                send(neighbor, body)
            }
        }
    }
}
