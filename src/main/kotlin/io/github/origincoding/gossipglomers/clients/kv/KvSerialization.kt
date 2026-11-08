package io.github.origincoding.gossipglomers.clients.kv

import io.github.origincoding.gossipglomers.runtime.ErrorCode
import io.github.origincoding.gossipglomers.runtime.RpcException
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement

suspend inline fun <reified T> KvClient.readAs(key: String): T =
    json.decodeFromJsonElement(read(key))

/** 仅在 key 不存在时返回默认值，不创建 key，也不吞掉其他错误或已有的 null 值。 */
suspend inline fun <reified T> KvClient.readAsOrDefault(key: String, defaultValue: T): T =
    try {
        readAs<T>(key)
    } catch (e: RpcException) {
        if (e.code == ErrorCode.KEY_DOES_NOT_EXIST) defaultValue else throw e
    }

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
