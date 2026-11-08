@file:OptIn(ExperimentalUuidApi::class)

package io.github.origincoding.gossipglomers.challenges.uniqueid

import io.github.origincoding.gossipglomers.runtime.EmptyBody
import io.github.origincoding.gossipglomers.runtime.Runtime
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid
import kotlinx.serialization.Serializable

@Serializable
data class GenerateOkBody(val id: String)

fun run() {
    Runtime().run {
        on<EmptyBody, GenerateOkBody>("generate") { GenerateOkBody(Uuid.generateV4().toHexDashString()) }
    }
}
