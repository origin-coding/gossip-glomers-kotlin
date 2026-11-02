package io.github.origincoding.gossipglomers.challenges.echo

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.serializer
import kotlin.concurrent.atomics.AtomicInt

@Serializable
data class Message<T>(
    val src: String,
    val dest: String,
    val body: T
)

@Serializable
data class InitBody(
    @SerialName("msg_id") val msgId: Long,
    @SerialName("node_id") val nodeId: String,
    @SerialName("node_ids") val nodeIds: List<String>,
    val type: String = "init",
)

@Serializable
data class InitOkBody(
    @SerialName("in_reply_to") val inReplyTo: Long,
    val type: String = "init_ok",
)

@Serializable
data class EchoBody(
    @SerialName("msg_id") val msgId: Int,
    val echo: String,
    val type: String = "echo",
)

@Serializable
data class EchoOkBody(
    @SerialName("in_reply_to") val inReplyTo: Int,
    val echo: String,
    val type: String = "echo_ok",
    @SerialName("msg_id") val msgId: Int = 1,
)

/** Read one protocol message per line and keep the node alive until stdin reaches EOF. */
@OptIn(kotlin.concurrent.atomics.ExperimentalAtomicApi::class)
fun run(): Unit {
    val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true // Maelstrom may include extra metadata, such as an outer id.
    }
    val input = System.`in`.bufferedReader(Charsets.UTF_8)
    val output = System.out.bufferedWriter(Charsets.UTF_8)

    val messageId = AtomicInt(1)

    fun sendLine(line: String) {
        output.write(line)
        output.newLine()
        output.flush() // Send immediately, even while stdin remains open.
    }

    while (true) {
        val line = input.readLine() ?: break
        val message = json.decodeFromString<Message<JsonObject>>(line)

        when (message.body["type"]?.jsonPrimitive?.content) {
            "init" -> {
                val initBody = json.decodeFromJsonElement<InitBody>(json.serializersModule.serializer(), message.body)
                val initOkBody = InitOkBody(initBody.msgId)
                val response = Message<InitOkBody>(message.dest, message.src, initOkBody)
                sendLine(json.encodeToString(response))
            }

            "echo" -> {
                val echoBody = json.decodeFromJsonElement<EchoBody>(json.serializersModule.serializer(), message.body)
                val echoOkBody = EchoOkBody(echoBody.msgId, echoBody.echo, msgId = messageId.fetchAndAdd(1))
                val response = Message<EchoOkBody>(message.dest, message.src, echoOkBody)
                sendLine(json.encodeToString(response))
            }
        }
    }
}
