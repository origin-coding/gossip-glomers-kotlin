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

    inline fun <reified T> decode(message: Message<JsonObject>): T =
        json.decodeFromJsonElement(message.body)

    inline fun <reified T> reply(request: Message<JsonObject>, body: T) {
        reply(request, json.encodeToJsonElement(body).jsonObject)
    }

    fun reply(request: Message<JsonObject>, body: JsonObject) {
        val requestId = requireNotNull(request.msgId) {
            "Cannot reply to a message without msg_id"
        }
        val fields = body.toMutableMap()
        fields["in_reply_to"] = JsonPrimitive(requestId)
        fields["msg_id"] = JsonPrimitive(nextMessageId++)

        write(
            Message(
                src = nodeId,
                dest = request.src,
                body = JsonObject(fields),
            )
        )
    }
}
