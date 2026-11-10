package io.github.origincoding.gossipglomers.challenges.kafka

import io.github.origincoding.gossipglomers.runtime.Runtime

fun run() {
    val mode = KafkaMode.fromEnvironment()
    Runtime().run { KafkaNode(this, mode).register() }
}
