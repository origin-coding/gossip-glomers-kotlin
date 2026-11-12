package io.github.origincoding.gossipglomers.challenges.txn

import io.github.origincoding.gossipglomers.runtime.Runtime

/** Stage 06: 本地事务与异步批量复制，支持读已提交。 */
fun run(): Unit {
    Runtime().run { TxnNode(this).register() }
}
