package io.github.origincoding.gossipglomers.challenges.kafka

import io.github.origincoding.gossipglomers.runtime.Runtime

fun run() {
    Runtime().run { KafkaNode(this).register() }
}
