package io.github.origincoding.gossipglomers.clients.kv

import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement

suspend inline fun <reified T> KvClient.readAs(key: String): T =
    json.decodeFromJsonElement(read(key))

suspend inline fun <reified T> KvClient.writeAs(key: String, value: T) {
    write(key, json.encodeToJsonElement(value))
}

suspend inline fun <reified T> KvClient.compareAndSwapAs(
    key: String,
    from: T,
    to: T,
    createIfNotExists: Boolean = false,
) {
    compareAndSwap(key, json.encodeToJsonElement(from), json.encodeToJsonElement(to), createIfNotExists)
}
