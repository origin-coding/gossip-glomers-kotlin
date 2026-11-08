package io.github.origincoding.gossipglomers.challenges.broadcast

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TopologyTest {
    private val nodes = (0 until 25).map { "n$it" }

    @Test
    fun `default follows supplied neighbors`() {
        val supplied = mapOf("n0" to listOf("n1", "n0", "n1"))
        assertEquals(setOf("n1"), Topology.Default.neighbors("n0", nodes, supplied))
        assertFailsWith<IllegalArgumentException> { Topology.Default.neighbors("n1", nodes, supplied) }
    }

    @Test
    fun `star and fanout form connected undirected trees`() {
        for (topology in listOf(Topology.Star, Topology.Fanout(1), Topology.Fanout(3), Topology.Fanout(Int.MAX_VALUE))) {
            val graph = nodes.associateWith { topology.neighbors(it, nodes, emptyMap()) }
            graph.forEach { (node, neighbors) ->
                assertFalse(node in neighbors)
                neighbors.forEach { assertTrue(node in graph.getValue(it)) }
            }
            val visited = mutableSetOf<String>()
            val pending = ArrayDeque(listOf(nodes.first()))
            while (pending.isNotEmpty()) {
                val next = pending.removeFirst()
                if (visited.add(next)) pending.addAll(graph.getValue(next))
            }
            assertEquals(nodes.toSet(), visited)
            assertEquals(2 * (nodes.size - 1), graph.values.sumOf { it.size })
            assertEquals(emptySet(), topology.neighbors("n0", listOf("n0"), emptyMap()))
        }
    }

    @Test
    fun `fanout bounds children and environment selects strategy`() {
        val topology = Topology.Fanout(3)
        assertEquals(setOf("n1", "n2", "n3"), topology.neighbors("n0", nodes, emptyMap()))
        nodes.drop(1).forEach { assertTrue(topology.neighbors(it, nodes, emptyMap()).size <= 4) }
        assertEquals(Topology.Star, Topology.fromEnvironment(emptyMap()))
        assertEquals(Topology.Default, Topology.fromEnvironment(mapOf("BROADCAST_TOPOLOGY" to "default")))
        assertEquals(Topology.Fanout(2), Topology.fromEnvironment(mapOf("BROADCAST_TOPOLOGY" to "fanout", "BROADCAST_FANOUT" to "2")))
        assertFailsWith<IllegalArgumentException> { Topology.Fanout(0) }
        assertFailsWith<IllegalStateException> { Topology.fromEnvironment(mapOf("BROADCAST_TOPOLOGY" to "typo")) }
    }
}
