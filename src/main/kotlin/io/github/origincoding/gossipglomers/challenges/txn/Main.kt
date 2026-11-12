package io.github.origincoding.gossipglomers.challenges.txn

import io.github.origincoding.gossipglomers.runtime.Runtime

/** Stage 06a: 单节点事务；多节点复制待接入。 */
fun run(): Unit {
    Runtime().run { TxnNode(this).register() }
}
