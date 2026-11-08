package io.github.origincoding.gossipglomers.runtime

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

class NodeTest {
    @Test
    fun `RPCs match reordered responses and reject wrong sources`() = runBlocking {
        val h = NodeHarness(this)
        val first = async { h.node.rpc("seq-kv", "read") }
        val second = async { h.node.rpc("seq-kv", "read") }
        val a = h.next()
        val b = h.next()

        h.respond(a, source = "n2")
        yield()
        assertFalse(first.isCompleted)
        h.respond(b, buildJsonObject { put("value", 2) })
        h.respond(a, buildJsonObject { put("value", 1) })
        h.respond(a) // 重复响应不完成其他请求。

        assertEquals(1, first.await().getValue("value").jsonPrimitive.int)
        assertEquals(2, second.await().getValue("value").jsonPrimitive.int)
        assertEquals(0, h.node.pendingRpcCount)
    }

    @Test
    fun `suspended handler does not prevent another request completing`() = runBlocking {
        val h = NodeHarness(this)
        h.node.on<EmptyBody, EmptyBody>("slow") {
            h.node.rpc("seq-kv", "read")
            EmptyBody
        }
        h.node.on<EmptyBody, EmptyBody>("fast") { EmptyBody }
        h.node.acceptRequest(incoming("slow", 10))
        val rpc = h.next()
        h.node.acceptRequest(incoming("fast", 11))
        assertEquals(11L, h.next().inReplyTo)

        h.respond(rpc)
        assertEquals(10L, h.next().inReplyTo)
    }

    @Test
    fun `timeout and caller cancellation clean up pending requests`() = runBlocking {
        val h = NodeHarness(this)
        val timeout = async {
            assertFailsWith<RpcTimeoutException> {
                h.node.rpc("seq-kv", "read", timeout = 30.milliseconds)
            }
        }
        val expired = h.next()
        timeout.await()
        assertEquals(0, h.node.pendingRpcCount)
        h.respond(expired)

        val cancelled = launch { h.node.rpc("seq-kv", "read") }
        val abandoned = h.next()
        cancelled.cancelAndJoin()
        assertEquals(0, h.node.pendingRpcCount)
        h.respond(abandoned)

        val next = async { h.node.rpc("seq-kv", "read") }
        h.respond(h.next())
        assertEquals("read_ok", next.await().getValue("type").jsonPrimitive.content)
    }

    @Test
    fun `remote errors keep their codes and do not poison later calls`() = runBlocking<Unit> {
        val h = NodeHarness(this)
        val failure = async {
            assertFailsWith<RpcException> { h.node.rpc("seq-kv", "cas") }
        }
        h.respond(h.next(), buildJsonObject {
            put("code", ErrorCode.PRECONDITION_FAILED)
            put("text", "conflict")
        }, type = "error")
        assertEquals(ErrorCode.PRECONDITION_FAILED, failure.await().code)
        assertEquals(0, h.node.pendingRpcCount)

        val success = async { h.node.rpc("seq-kv", "write") }
        h.respond(h.next())
        success.await()
    }

    @Test
    fun `request timeout produces an indefinite protocol error`() = runBlocking {
        val h = NodeHarness(this)
        h.node.on<EmptyBody, EmptyBody>("probe") {
            h.node.rpc("seq-kv", "read", timeout = 30.milliseconds)
            EmptyBody
        }
        h.node.acceptRequest(incoming("probe", 20))
        h.next() // 发出的 KV 请求没有响应。
        val error = h.next()
        assertEquals("error", error.type)
        assertEquals(20L, error.inReplyTo)
        assertEquals(ErrorCode.TIMEOUT, error.body.getValue("code").jsonPrimitive.int)
    }

    @Test
    fun `unexpected success type is a visible protocol failure`() = runBlocking {
        val h = NodeHarness(this)
        val result = async {
            assertFailsWith<IllegalStateException> { h.node.rpc("seq-kv", "read") }
        }
        h.respond(h.next(), type = "write_ok")
        result.await()
        assertEquals(0, h.node.pendingRpcCount)
    }

    @Test
    fun `unknown programming errors fail the node scope`() = runBlocking<Unit> {
        assertFailsWith<IllegalStateException> {
            coroutineScope {
                val h = NodeHarness(this)
                h.node.on<EmptyBody, EmptyBody>("broken") { error("bug") }
                h.node.acceptRequest(incoming("broken", 1))
                yield()
            }
        }
    }
}
