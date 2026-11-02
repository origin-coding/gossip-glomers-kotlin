package io.github.origincoding.gossipglomers.runtime

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

@Serializable
data class Message<T>(
    val src: String,
    val dest: String,
    val body: T,
)

val Message<JsonObject>.type: String
    get() = body.getValue("type").jsonPrimitive.content

val Message<JsonObject>.msgId: Long?
    get() = body["msg_id"]?.jsonPrimitive?.long

val Message<JsonObject>.inReplyTo: Long?
    get() = body["in_reply_to"]?.jsonPrimitive?.long

@Serializable
data class InitBody(
    @SerialName("node_id") val nodeId: String,
    @SerialName("node_ids") val nodeIds: List<String>,
)

@Serializable
data class InitOkBody(
    val type: String = "init_ok",
)

@Serializable
data class TopologyBody(
    val topology: Map<String, List<String>>,
)

@Serializable
data class TopologyOkBody(
    val type: String = "topology_ok",
)
