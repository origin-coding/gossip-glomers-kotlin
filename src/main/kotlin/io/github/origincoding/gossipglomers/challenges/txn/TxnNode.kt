package io.github.origincoding.gossipglomers.challenges.txn

import io.github.origincoding.gossipglomers.runtime.Node

private data class VersionedValue(
    val value: Long,
    val version: Version,
)

class TxnNode(private val node: Node) {
    private val committed = mutableMapOf<Long, VersionedValue>()
    private var clock = 0L

    fun register() {
        node.on<TxnBody, TxnOkBody>("txn") { request ->
            execute(request.body.txn)
        }
    }

    // 整笔事务在同一事件循环中执行；读取到提交之间不能挂起。
    private fun execute(operations: List<Operation>): TxnOkBody {
        val writes = mutableMapOf<Long, Long>()
        val results = operations.map { operation ->
            val key = operation.key

            when (operation) {
                is Operation.Read -> OperationResult.Read(key, writes[key] ?: committed[key]?.value)
                is Operation.Write -> {
                    writes[key] = operation.value
                    OperationResult.Write(key, operation.value)
                }
            }
        }

        if (writes.isNotEmpty()) {
            val txn = ReplicatedTxn(nextVersion(), writes.toMap())
            apply(txn)
        }
        return TxnOkBody(results)
    }

    private fun apply(txn: ReplicatedTxn) {
        for ((key, value) in txn.writes) {
            val current = committed[key]

            if (current == null || txn.version > current.version) {
                committed[key] = VersionedValue(value, txn.version)
            }
        }
    }

    // 本地提交时，分配新版本
    private fun nextVersion() = Version(++clock, node.id)

    // 收到远端事务时，推进本地时钟
    private fun observe(version: Version) {
        clock = maxOf(clock, version.counter) + 1
    }
}
