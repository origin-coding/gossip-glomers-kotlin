package io.github.origincoding.gossipglomers.challenges.txn

import io.github.origincoding.gossipglomers.runtime.NodeHarness
import io.github.origincoding.gossipglomers.runtime.inReplyTo
import io.github.origincoding.gossipglomers.runtime.incoming
import io.github.origincoding.gossipglomers.runtime.type
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject

class TxnNodeTest {
    @Test
    fun `transactions read their own writes and publish only the final value`() = runBlocking {
        val h = NodeHarness(this, listOf("n1"))
        TxnNode(h.node).register()

        val operations = listOf(
            Operation.Read(1),
            Operation.Write(1, 6),
            Operation.Read(1),
            Operation.Write(1, 9),
            Operation.Read(1),
            Operation.Write(2, 12),
        )
        h.node.acceptRequest(incoming("txn", 1, h.node.json.encodeToJsonElement(TxnBody(operations)).jsonObject))

        val reply = h.next()
        assertEquals("txn_ok", reply.type)
        assertEquals(1L, reply.inReplyTo)
        assertEquals(
            TxnOkBody(listOf(
                OperationResult.Read(1, null),
                OperationResult.Write(1, 6),
                OperationResult.Read(1, 6),
                OperationResult.Write(1, 9),
                OperationResult.Read(1, 9),
                OperationResult.Write(2, 12),
            )),
            h.node.json.decodeFromJsonElement<TxnOkBody>(reply.body),
        )

        val reads = TxnBody(listOf(Operation.Read(1), Operation.Read(2), Operation.Read(3)))
        h.node.acceptRequest(incoming("txn", 2, h.node.json.encodeToJsonElement(reads).jsonObject))
        val readReply = h.next()
        assertEquals(2L, readReply.inReplyTo)
        assertEquals(
            TxnOkBody(listOf(
                OperationResult.Read(1, 9),
                OperationResult.Read(2, 12),
                OperationResult.Read(3, null),
            )),
            h.node.json.decodeFromJsonElement<TxnOkBody>(readReply.body),
        )
        assertTrue(h.sent.tryReceive().isFailure)
    }
}
