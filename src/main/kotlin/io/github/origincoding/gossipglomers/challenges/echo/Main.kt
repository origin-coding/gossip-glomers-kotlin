package io.github.origincoding.gossipglomers.challenges.echo

import io.github.origincoding.gossipglomers.runtime.Runtime
import kotlinx.serialization.Serializable

@Serializable
data class EchoBody(val echo: String)

@Serializable
data class EchoOkBody(val echo: String)

fun run() {
    Runtime().run {
        on<EchoBody, EchoOkBody>("echo") { request -> EchoOkBody(request.body.echo) }
    }
}
