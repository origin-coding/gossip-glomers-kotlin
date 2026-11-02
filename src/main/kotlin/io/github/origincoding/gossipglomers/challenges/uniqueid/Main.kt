@file:OptIn(ExperimentalUuidApi::class)

package io.github.origincoding.gossipglomers.challenges.uniqueid

import io.github.origincoding.gossipglomers.runtime.Runtime
import kotlinx.serialization.Serializable
import kotlin.system.exitProcess
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@Serializable
class GenerateBody

@Serializable
class GenerateOkBody(
    val id: String,
    val type: String = "generate_ok"
)

/** Stage 02: Unique ID Generation. Implement the node here. */
fun run(): Unit {
    val runtime = Runtime()

    runtime.on("generate") { node, message ->
        node.reply(message, GenerateOkBody(Uuid.generateV4().toHexDashString()))
    }

    runtime.run()
}
