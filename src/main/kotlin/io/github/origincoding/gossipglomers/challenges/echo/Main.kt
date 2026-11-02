package io.github.origincoding.gossipglomers.challenges.echo

import io.github.origincoding.gossipglomers.runtime.Runtime
import kotlinx.serialization.Serializable

@Serializable
data class EchoBody(
    val echo: String,
)

@Serializable
data class EchoOkBody(
    val echo: String,
    val type: String = "echo_ok",
)

fun run() {
    val runtime = Runtime()

    runtime.on("echo") { node, message ->
        val body = node.decode<EchoBody>(message)
        node.reply(message, EchoOkBody(body.echo))
    }

    runtime.run()
}
