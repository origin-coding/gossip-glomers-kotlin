package io.github.origincoding.gossipglomers.challenges.broadcast

/** 只计算邻居，不持有节点状态，也不发送消息。树的边是双向的。 */
sealed interface Topology {
    fun neighbors(nodeId: String, nodeIds: List<String>, supplied: Map<String, List<String>>): Set<String>

    data object Default : Topology {
        override fun neighbors(nodeId: String, nodeIds: List<String>, supplied: Map<String, List<String>>): Set<String> {
            val neighbors = requireNotNull(supplied[nodeId]) { "Topology does not contain $nodeId" }.toSet() - nodeId
            require(neighbors.all { it in nodeIds }) { "Topology contains unknown neighbors for $nodeId" }
            return neighbors
        }
    }

    data object Star : Topology {
        override fun neighbors(nodeId: String, nodeIds: List<String>, supplied: Map<String, List<String>>): Set<String> {
            require(nodeId in nodeIds) { "Unknown node $nodeId" }
            val center = nodeIds.first()
            return if (nodeId == center) nodeIds.toSet() - center else setOf(center)
        }
    }

    data class Fanout(val childrenPerNode: Int = 3) : Topology {
        init {
            require(childrenPerNode > 0) { "Fanout must be positive" }
        }

        override fun neighbors(nodeId: String, nodeIds: List<String>, supplied: Map<String, List<String>>): Set<String> {
            val index = nodeIds.indexOf(nodeId)
            require(index >= 0) { "Unknown node $nodeId" }
            return buildSet {
                if (index > 0) add(nodeIds[(index - 1) / childrenPerNode])
                val firstChild = index.toLong() * childrenPerNode + 1
                val end = minOf(firstChild + childrenPerNode, nodeIds.size.toLong())
                for (child in firstChild until end) add(nodeIds[child.toInt()])
            }
        }
    }

    companion object {
        // 沿用已有广播的星形选择；default 表示使用 Maelstrom 提供的拓扑。
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): Topology =
            when (val mode = environment["BROADCAST_TOPOLOGY"] ?: "star") {
                "default" -> Default
                "star" -> Star
                "fanout" -> Fanout(
                    environment["BROADCAST_FANOUT"]?.let {
                        requireNotNull(it.toIntOrNull()) { "BROADCAST_FANOUT must be an integer" }
                    } ?: 3
                )
                else -> error("Unknown BROADCAST_TOPOLOGY '$mode'; use default, star, or fanout")
            }
    }
}
