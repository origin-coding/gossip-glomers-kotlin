package io.github.origincoding.gossipglomers.challenges.broadcast

import io.github.origincoding.gossipglomers.runtime.Runtime

fun run() {
    val topology = Topology.fromEnvironment()
    Runtime().run { BroadcastNode(this, topology).register() }
}
