package io.github.origincoding.gossipglomers.challenges.gcounter

import io.github.origincoding.gossipglomers.clients.kv.kv
import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.Runtime
import kotlinx.serialization.Serializable

@Serializable
data class AddBody(val delta: Long)

@Serializable
data class ReadOkBody(val value: Long)

fun run() {
    Runtime().run {
        val counter = GCounter(kv())

        on<AddBody, EmptyBody>("add") { request ->
            counter.add(request.body.delta)
            EmptyBody
        }

        on<EmptyBody, ReadOkBody>("read") { ReadOkBody(counter.read()) }
    }
}
