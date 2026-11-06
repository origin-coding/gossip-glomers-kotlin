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

    /** 返回本次发送的 msg_id；正常返回不代表对端已经收到。 */
    inline fun <reified T> send(dest: String, body: T): Long =
        send(dest, json.encodeToJsonElement(body).jsonObject)

    fun send(dest: String, body: JsonObject): Long {
        val requestId = nextMessageId++
        val fields = body.toMutableMap()
        fields["msg_id"] = JsonPrimitive(requestId)
        write(Message(src = nodeId, dest = dest, body = JsonObject(fields)))
        return requestId
    }

    /** 返回回复自身的 msg_id，原请求编号仍放在 in_reply_to 中。 */
    inline fun <reified T> reply(request: Message<JsonObject>, body: T): Long =
        reply(request, json.encodeToJsonElement(body).jsonObject)

    fun reply(request: Message<JsonObject>, body: JsonObject): Long {
        val requestId = requireNotNull(request.msgId) {
            "Cannot reply to a message without msg_id"
        }
        val fields = body.toMutableMap()
        fields["in_reply_to"] = JsonPrimitive(requestId)
        return send(request.src, JsonObject(fields))
    }

    /** 按实际发送顺序返回 msg_id；没有发送目标时返回空列表。 */
    inline fun <reified T> sendToNeighbors(body: T, excluding: Set<String> = emptySet()): List<Long> =
        sendToNeighbors(json.encodeToJsonElement(body).jsonObject, excluding)

    fun sendToNeighbors(body: JsonObject, excluding: Set<String> = emptySet()): List<Long> =
        buildList {
            for (neighbor in neighbors) {
                if (neighbor !in excluding) {
                    add(send(neighbor, body))
                }
            }
        }
}
