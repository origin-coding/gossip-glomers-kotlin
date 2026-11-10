package io.github.origincoding.gossipglomers.challenges.kafka

import io.github.origincoding.gossipglomers.runtime.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import kotlinx.serialization.json.*

class KafkaNodeTest {
    @Test
    fun `direct writes locally while routed modes forward to the same sorted owner`() = runBlocking {
        for (mode in KafkaMode.entries) {
            // "a" belongs to n2 after sorting, although n2 was supplied first.
            val h = NodeHarness(this, listOf("n2", "n1"))
            KafkaNode(h.node, mode).register()
            h.node.acceptRequest(send(1, "a", 10))

            val request = h.next()
            val expectedOffset = if (mode == KafkaMode.DIRECT) {
                assertRead(request, "a")
                h.respond(request, log())
                val cas = h.next()
                assertCas(cas, "a", emptyList(), listOf(10))
                h.respond(cas)
                0L
            } else {
                assertEquals("n2", request.dest)
                assertEquals("forward", request.type)
                assertEquals("a", request.body.getValue("key").jsonPrimitive.content)
                assertEquals(10L, request.body.getValue("msg").jsonPrimitive.long)
                h.respond(request, buildJsonObject { put("offset", 17) })
                17L
            }

            assertReply(h.next(), "send_ok", 1, expectedOffset)
            assertTrue(h.sent.tryReceive().isFailure)
        }
    }

    @Test
    fun `hash serializes local and forwarded writes for a key without blocking other keys`() = runBlocking {
        val h = NodeHarness(this)
        KafkaNode(h.node, KafkaMode.HASH).register()
        h.node.acceptRequest(send(1, "b", 10))
        val firstRead = h.next()
        assertRead(firstRead, "b")

        h.node.acceptRequest(send(2, "b", 20, "forward"))
        h.node.acceptRequest(send(3, "d", 30))
        val otherRead = h.next()
        assertRead(otherRead, "d")
        h.respond(otherRead, log())
        val otherCas = h.next()
        assertCas(otherCas, "d", emptyList(), listOf(30))
        h.respond(otherCas)
        assertReply(h.next(), "send_ok", 3, 0)
        assertTrue(h.sent.tryReceive().isFailure)

        h.respond(firstRead, log())
        val firstCas = h.next()
        assertCas(firstCas, "b", emptyList(), listOf(10))
        h.respond(firstCas)
        val completed = listOf(h.next(), h.next())
        assertReply(completed.single { it.type == "send_ok" }, "send_ok", 1, 0)
        val secondRead = completed.single { it.type == "read" }
        assertRead(secondRead, "b")
        h.respond(secondRead, log(10))
        val secondCas = h.next()
        assertCas(secondCas, "b", listOf(10), listOf(10, 20))
        h.respond(secondCas)
        assertReply(h.next(), "forward_ok", 2, 1)
    }

    @Test
    fun `default batch mode collects writes during RPC and recomputes offsets after conflict`() = runBlocking {
        withTimeout(5000) {
            val h = NodeHarness(this)
            KafkaNode(h.node).register()
            try {
                h.node.acceptRequest(send(1, "b", 10))
                val firstRead = h.next()
                assertRead(firstRead, "b")
                h.node.acceptRequest(send(2, "b", 20))
                h.node.acceptRequest(send(3, "b", 30, "forward"))
                yield()
                assertTrue(h.sent.tryReceive().isFailure)

                h.respond(firstRead, log())
                val firstCas = h.next()
                assertCas(firstCas, "b", emptyList(), listOf(10))
                h.respond(firstCas)
                val completed = listOf(h.next(), h.next())
                assertReply(completed.single { it.type == "send_ok" }, "send_ok", 1, 0)
                val batchRead = completed.single { it.type == "read" }
                h.respond(batchRead, log(10))
                val batchCas = h.next()
                assertCas(batchCas, "b", listOf(10), listOf(10, 20, 30))
                h.respond(batchCas, buildJsonObject { put("code", ErrorCode.PRECONDITION_FAILED) }, type = "error")

                val retryRead = h.next()
                assertRead(retryRead, "b")
                h.respond(retryRead, log(10, 99))
                val retryCas = h.next()
                assertCas(retryCas, "b", listOf(10, 99), listOf(10, 99, 20, 30))
                assertTrue(h.sent.tryReceive().isFailure)
                h.respond(retryCas)

                val replies = listOf(h.next(), h.next()).associateBy { it.inReplyTo }
                assertReply(replies.getValue(2L), "send_ok", 2, 2)
                assertReply(replies.getValue(3L), "forward_ok", 3, 3)
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }

    @Test
    fun `failed batch notifies waiting request and the key worker can process another batch`() = runBlocking {
        withTimeout(5000) {
            val h = NodeHarness(this)
            KafkaNode(h.node, KafkaMode.BATCH).register()
            try {
                h.node.acceptRequest(send(1, "b", 10))
                h.respond(h.next(), log())
                h.respond(h.next(), buildJsonObject { put("code", ErrorCode.TEMPORARILY_UNAVAILABLE) }, type = "error")
                val failure = h.next()
                assertEquals("error", failure.type)
                assertEquals(1L, failure.inReplyTo)
                assertEquals(ErrorCode.TEMPORARILY_UNAVAILABLE, failure.body.getValue("code").jsonPrimitive.int)

                h.node.acceptRequest(send(2, "b", 20))
                h.respond(h.next(), log())
                val cas = h.next()
                assertCas(cas, "b", emptyList(), listOf(20))
                h.respond(cas)
                assertReply(h.next(), "send_ok", 2, 0)
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }

    private fun send(id: Long, key: String, msg: Long, type: String = "send") =
        incoming(type, id, buildJsonObject {
            put("key", key)
            put("msg", msg)
        })

    private fun log(vararg messages: Long) = buildJsonObject {
        put("value", JsonArray(messages.map { JsonPrimitive(it) }))
    }

    private fun assertRead(message: Envelope, key: String) {
        assertEquals("lin-kv", message.dest)
        assertEquals("read", message.type)
        assertEquals("logs-$key", message.body.getValue("key").jsonPrimitive.content)
    }

    private fun assertCas(message: Envelope, key: String, before: List<Long>, after: List<Long>) {
        assertEquals("lin-kv", message.dest)
        assertEquals("cas", message.type)
        assertEquals("logs-$key", message.body.getValue("key").jsonPrimitive.content)
        assertEquals(JsonArray(before.map { JsonPrimitive(it) }), message.body["from"])
        assertEquals(JsonArray(after.map { JsonPrimitive(it) }), message.body["to"])
        assertEquals(JsonPrimitive(true), message.body["create_if_not_exists"])
    }

    private fun assertReply(message: Envelope, type: String, requestId: Long, offset: Long) {
        assertEquals(type, message.type)
        assertEquals("c1", message.dest)
        assertEquals(requestId, message.inReplyTo)
        assertEquals(offset, message.body.getValue("offset").jsonPrimitive.long)
    }
}
