package io.github.origincoding.gossipglomers.challenges.txn

import io.github.origincoding.gossipglomers.runtime.ErrorCode
import io.github.origincoding.gossipglomers.runtime.NodeHarness
import io.github.origincoding.gossipglomers.runtime.inReplyTo
import io.github.origincoding.gossipglomers.runtime.incoming
import io.github.origincoding.gossipglomers.runtime.msgId
import io.github.origincoding.gossipglomers.runtime.type
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.put

class TxnNodeTest {
    @Test
    fun `replication batches queued writes and retries the fixed batch before newer writes`() = runBlocking {
        val h = NodeHarness(this)
        TxnNode(h.node).register()
        fun write(id: Long) {
            val body = TxnBody(listOf(Operation.Write(1, id)))
            h.node.acceptRequest(incoming("txn", id, h.node.json.encodeToJsonElement(body).jsonObject))
        }

        try {
            write(1)
            val firstMessages = List(2) { h.next() }
            assertEquals(1L, firstMessages.single { it.type == "txn_ok" }.inReplyTo)
            val first = firstMessages.single { it.type == "replicate" }
            assertEquals("n2", first.dest)
            assertEquals(1, h.node.json.decodeFromJsonElement<ReplicateBody>(first.body).transactions.size)

            // 第一个 RPC 等待 ACK 时继续提交，确保队列里形成积压。
            for (id in 2L..66L) write(id)
            repeat(65) { assertEquals("txn_ok", h.next().type) }
            h.respond(first)

            val batchRequest = h.next()
            assertEquals("replicate", batchRequest.type)
            val batch = h.node.json.decodeFromJsonElement<ReplicateBody>(batchRequest.body)
            assertEquals((2L..65L).toList(), batch.transactions.map { it.writes.getValue(1L) })

            write(67)
            assertEquals("txn_ok", h.next().type)
            h.respond(batchRequest, buildJsonObject {
                put("code", ErrorCode.TIMEOUT)
                put("text", "retry this batch")
            }, type = "error")

            val retry = h.next()
            assertEquals("replicate", retry.type)
            assertNotEquals(batchRequest.msgId, retry.msgId)
            assertEquals(batch, h.node.json.decodeFromJsonElement<ReplicateBody>(retry.body))
            h.respond(retry)

            val remainder = h.next()
            assertEquals("replicate", remainder.type)
            assertEquals(
                listOf(66L, 67L),
                h.node.json.decodeFromJsonElement<ReplicateBody>(remainder.body).transactions.map { it.writes.getValue(1L) },
            )
            h.respond(remainder)
            yield()
            assertTrue(h.sent.tryReceive().isFailure)
        } finally {
            coroutineContext.cancelChildren()
        }
    }

    @Test
    fun `batch receiver applies versions idempotently and advances the local clock`() = runBlocking {
        val h = NodeHarness(this, listOf("n1"))
        TxnNode(h.node).register()
        val batch = ReplicateBody(listOf(
            ReplicatedTxn(Version(100, "n2"), mapOf(1L to 6L, 2L to 10L)),
            ReplicatedTxn(Version(99, "n2"), mapOf(1L to 4L)),
            ReplicatedTxn(Version(101, "n2"), mapOf(2L to 20L)),
            ReplicatedTxn(Version(100, "n2"), mapOf(1L to 6L, 2L to 10L)),
        ))
        h.node.acceptRequest(incoming("replicate", 1, h.node.json.encodeToJsonElement(batch).jsonObject))
        val ack = h.next()
        assertEquals("replicate_ok", ack.type)
        assertEquals(1L, ack.inReplyTo)

        val local = TxnBody(listOf(Operation.Read(1), Operation.Write(1, 7), Operation.Read(2)))
        h.node.acceptRequest(incoming("txn", 2, h.node.json.encodeToJsonElement(local).jsonObject))
        assertEquals(
            TxnOkBody(listOf(OperationResult.Read(1, 6), OperationResult.Write(1, 7), OperationResult.Read(2, 20))),
            h.node.json.decodeFromJsonElement<TxnOkBody>(h.next().body),
        )

        val read = TxnBody(listOf(Operation.Read(1)))
        h.node.acceptRequest(incoming("txn", 3, h.node.json.encodeToJsonElement(read).jsonObject))
        assertEquals(
            TxnOkBody(listOf(OperationResult.Read(1, 7))),
            h.node.json.decodeFromJsonElement<TxnOkBody>(h.next().body),
        )
    }

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
