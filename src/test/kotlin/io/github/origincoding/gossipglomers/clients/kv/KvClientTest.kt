package io.github.origincoding.gossipglomers.clients.kv

import io.github.origincoding.gossipglomers.runtime.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.*

class KvClientTest {
    @Serializable
    data class State(val count: Long, val tags: List<String>)

    @Test
    fun `generic values round trip and CAS keeps structured fields`() = runBlocking {
        val h = NodeHarness(this)
        val kv = h.node.kv()
        val initial = State(7, listOf("a", "b"))
        val updated = initial.copy(count = 8)
        val encoded = h.node.json.encodeToJsonElement(initial)

        val write = async { kv.writeAs("state", initial) }
        val writeRequest = h.next()
        assertEquals("seq-kv", writeRequest.dest)
        assertEquals(encoded, writeRequest.body["value"])
        h.respond(writeRequest)
        write.await()

        val read = async { kv.readAs<State>("state") }
        h.respond(h.next(), buildJsonObject { put("value", encoded) })
        assertEquals(initial, read.await())

        val cas = async { kv.compareAndSwapAs("state", initial, updated, createIfNotExists = true) }
        val casRequest = h.next()
        assertEquals("cas", casRequest.type)
        assertEquals(encoded, casRequest.body["from"])
        assertEquals(h.node.json.encodeToJsonElement(updated), casRequest.body["to"])
        assertEquals(JsonPrimitive(true), casRequest.body["create_if_not_exists"])
        h.respond(casRequest)
        cas.await()
    }

    @Test
    fun `missing key is not silently converted to zero`() = runBlocking {
        val h = NodeHarness(this)
        val read = async { assertFailsWith<RpcException> { h.node.kv().readAs<Long>("missing") } }
        h.respond(h.next(), buildJsonObject { put("code", 20) }, type = "error")
        assertEquals(ErrorCode.KEY_DOES_NOT_EXIST, read.await().code)
    }

    @Test
    fun `default applies only to a missing key and does not write it`() = runBlocking {
        val h = NodeHarness(this)
        val fallback = State(0, emptyList())
        val read = async { h.node.kv().readAsOrDefault("missing", fallback) }
        val request = h.next()
        assertEquals("read", request.type)
        h.respond(request, buildJsonObject { put("code", ErrorCode.KEY_DOES_NOT_EXIST) }, type = "error")
        assertEquals(fallback, read.await())
        assertTrue(h.sent.tryReceive().isFailure)
    }

    @Test
    fun `existing value and explicit null are preserved instead of default`() = runBlocking {
        val h = NodeHarness(this)
        val stored = async { h.node.kv().readAsOrDefault("count", 99L) }
        h.respond(h.next(), buildJsonObject { put("value", 0) })
        assertEquals(0L, stored.await())

        val nullable = async { h.node.kv().readAsOrDefault<Long?>("optional", 99L) }
        h.respond(h.next(), buildJsonObject { put("value", JsonNull) })
        assertNull(nullable.await())
    }

    @Test
    fun `default does not conceal other remote errors or invalid data`() = runBlocking<Unit> {
        val h = NodeHarness(this)
        val unavailable = async {
            assertFailsWith<RpcException> { h.node.kv().readAsOrDefault("count", 0L) }
        }
        h.respond(h.next(), buildJsonObject { put("code", ErrorCode.TEMPORARILY_UNAVAILABLE) }, type = "error")
        assertEquals(ErrorCode.TEMPORARILY_UNAVAILABLE, unavailable.await().code)

        val malformed = async {
            assertFailsWith<SerializationException> { h.node.kv().readAsOrDefault("count", 0L) }
        }
        h.respond(h.next(), buildJsonObject { put("value", "not a number") })
        malformed.await()
    }

    @Test
    fun `default does not conceal timeout`() = runBlocking<Unit> {
        val h = NodeHarness(this)
        assertFailsWith<RpcTimeoutException> {
            h.node.kv(timeout = 30.milliseconds).readAsOrDefault("count", 0L)
        }
    }
}
