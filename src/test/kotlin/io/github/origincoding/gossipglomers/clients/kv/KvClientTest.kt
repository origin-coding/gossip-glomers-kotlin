package io.github.origincoding.gossipglomers.clients.kv

import io.github.origincoding.gossipglomers.runtime.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.Serializable
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
}
