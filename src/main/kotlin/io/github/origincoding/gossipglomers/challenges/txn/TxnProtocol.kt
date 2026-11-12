package io.github.origincoding.gossipglomers.challenges.txn

import kotlinx.serialization.Serializable

@Serializable(with = OperationSerializer::class)
sealed interface Operation {
    val key: Long

    data class Read(override val key: Long) : Operation
    data class Write(override val key: Long, val value: Long) : Operation
}

@Serializable(with = OperationResultSerializer::class)
sealed interface OperationResult {
    val key: Long

    data class Read(override val key: Long, val value: Long?) : OperationResult
    data class Write(override val key: Long, val value: Long) : OperationResult
}

@Serializable
data class TxnBody(val txn: List<Operation>)

@Serializable
data class TxnOkBody(val txn: List<OperationResult>)

@Serializable
data class Version(
    val counter: Long,
    val nodeId: String,
) : Comparable<Version> {
    override fun compareTo(other: Version): Int =
        compareValuesBy(this, other, Version::counter, Version::nodeId)
}

/** 一笔已提交事务的最终写集合；同一个 key 的中间写入不会用于复制。 */
@Serializable
data class ReplicatedTxn(
    val version: Version,
    val writes: Map<Long, Long>,
)
