package io.github.origincoding.gossipglomers.challenges.gcounter

import io.github.origincoding.gossipglomers.clients.kv.KvClient
import io.github.origincoding.gossipglomers.clients.kv.compareAndSwapAs
import io.github.origincoding.gossipglomers.clients.kv.readAsOrDefault

private const val COUNTER_KEY = "counter"

class GCounter(private val store: KvClient) {
    suspend fun read(): Long = store.readAsOrDefault(COUNTER_KEY, 0L)

    suspend fun add(delta: Long) {
        val current = read()
        store.compareAndSwapAs(
            key = COUNTER_KEY,
            from = current,
            to = current + delta,
            createIfNotExists = true,
        )
    }
}
