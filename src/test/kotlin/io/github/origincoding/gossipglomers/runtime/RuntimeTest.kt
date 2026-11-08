package io.github.origincoding.gossipglomers.runtime

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json

class RuntimeTest {
    private val init = """{"src":"c1","dest":"n1","body":{"type":"init","msg_id":1,"node_id":"n1","node_ids":["n1"]}}"""

    @Test(timeout = 5000)
    fun `init replies first and completed output is drained at EOF`() {
        val input = listOf(init, """{"src":"c1","dest":"n1","body":{"type":"ping","msg_id":2}}""").joinToString("\n")
        val output = ByteArrayOutputStream()
        Runtime(ByteArrayInputStream(input.toByteArray()), output).run {
            on<EmptyBody, EmptyBody>("ping") { EmptyBody }
        }
        val messages = output.toString(Charsets.UTF_8).lineSequence().filter { it.isNotBlank() }
            .map { Json.decodeFromString<Envelope>(it) }.toList()
        assertEquals(listOf("init_ok", "ping_ok"), messages.map { it.type })
        assertEquals(listOf(1L, 2L), messages.map { it.inReplyTo })
        assertEquals(2, messages.map { it.msgId }.toSet().size)
    }

    @Test(timeout = 5000)
    fun `EOF cancels a suspended RPC instead of waiting for timeout`() {
        val input = listOf(init, """{"src":"c1","dest":"n1","body":{"type":"probe","msg_id":2}}""").joinToString("\n")
        val output = ByteArrayOutputStream()
        lateinit var observed: Node
        Runtime(ByteArrayInputStream(input.toByteArray()), output).run {
            observed = this
            on<EmptyBody, EmptyBody>("probe") {
                rpc("seq-kv", "read", timeout = 60.seconds)
                EmptyBody
            }
        }
        assertEquals(0, observed.pendingRpcCount)
        assertTrue(output.toString(Charsets.UTF_8).contains("init_ok"))
    }
}
