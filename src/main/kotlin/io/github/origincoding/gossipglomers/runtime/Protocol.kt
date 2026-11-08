package io.github.origincoding.gossipglomers.runtime

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long

@Serializable
data class Envelope(val src: String, val dest: String, val body: JsonObject)

val Envelope.type: String get() = body.getValue("type").jsonPrimitive.content
val Envelope.msgId: Long? get() = body["msg_id"]?.jsonPrimitive?.long
val Envelope.inReplyTo: Long? get() = body["in_reply_to"]?.jsonPrimitive?.long

data class Request<T>(val src: String, val msgId: Long, val body: T)

@Serializable
internal data class InitBody(
    @SerialName("node_id") val nodeId: String,
    @SerialName("node_ids") val nodeIds: List<String>,
)

@Serializable
data object EmptyBody

object ErrorCode {
    const val TIMEOUT = 0
    const val NOT_SUPPORTED = 10
    const val TEMPORARILY_UNAVAILABLE = 11
    const val MALFORMED_REQUEST = 12
    const val KEY_DOES_NOT_EXIST = 20
    const val PRECONDITION_FAILED = 22
}

class RpcException(val code: Int, message: String) : Exception(message)

// 超时只表示没有收到确认，不能据此断定远端没有执行。
class RpcTimeoutException(val destination: String, val requestType: String) :
    Exception("RPC $requestType to $destination timed out")
