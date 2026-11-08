package io.github.origincoding.gossipglomers.challenges.broadcast

import io.github.origincoding.gossipglomers.runtime.*
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.*

class BroadcastNodeTest {
    @Test
    fun `lost ACK retries fixed batch and preserves messages received while waiting`() = runBlocking {
        withTimeout(5000) {
            val h = NodeHarness(this)
            BroadcastNode(h.node).register()
            try {
                h.node.acceptRequest(incoming("topology", 1, buildJsonObject { put("topology", buildJsonObject {}) }))
                assertEquals("topology_ok", h.next().type)
                h.node.acceptRequest(incoming("broadcast", 2, buildJsonObject { put("message", 1) }))
                assertEquals("broadcast_ok", h.next().type)
                val first = h.next()
                assertEquals(setOf(1L), values(first))

                h.node.acceptRequest(incoming("broadcast", 3, buildJsonObject { put("message", 2) }))
                assertEquals("broadcast_ok", h.next().type)
                val retry = h.next() // first 的 ACK 丢失。
                assertEquals(setOf(1L), values(retry))
                h.respond(first) // 旧尝试的迟到响应不能确认新的请求。
                h.respond(retry)

                val second = h.next()
                assertEquals(setOf(2L), values(second))
                h.respond(second)
                h.node.acceptRequest(incoming("read", 4))
                assertEquals(setOf(1L, 2L), values(h.next()))
            } finally {
                coroutineContext.cancelChildren()
            }
        }
    }

    private fun values(message: Envelope): Set<Long> =
        message.body.getValue("messages").jsonArray.map { it.jsonPrimitive.long }.toSet()
}
