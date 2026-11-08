package io.github.origincoding.gossipglomers.clients.kv

import io.github.origincoding.gossipglomers.runtime.Node
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import kotlinx.serialization.json.put

/** 只封装单次 KV 操作；不存在、冲突、超时的处理策略由业务决定。 */
class KvClient(
    private val node: Node,
    private val service: String,
    private val timeout: Duration = 1.seconds,
) {
    @PublishedApi
    internal val json = node.json

    suspend fun read(key: String): JsonElement = node.rpc(
        destination = service,
        type = "read",
        body = buildJsonObject { put("key", key) },
        timeout = timeout,
    ).getValue("value")

    suspend fun readLong(key: String): Long = read(key).jsonPrimitive.long

    suspend fun write(key: String, value: JsonElement) {
        node.rpc(service, "write", buildJsonObject {
            put("key", key)
            put("value", value)
        }, timeout = timeout)
    }

    suspend fun compareAndSwap(
        key: String,
        from: JsonElement,
        to: JsonElement,
        createIfNotExists: Boolean = false,
    ) {
        node.rpc(service, "cas", buildJsonObject {
            put("key", key)
            put("from", from)
            put("to", to)
            put("create_if_not_exists", createIfNotExists)
        }, timeout = timeout)
    }
}

fun Node.kv(service: String = "seq-kv", timeout: Duration = 1.seconds): KvClient =
    KvClient(this, service, timeout)
