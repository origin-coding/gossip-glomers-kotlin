package io.github.origincoding.gossipglomers.runtime

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement

typealias Handler = (NodeContext, Message<JsonObject>) -> Unit

class Runtime {
    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true
    }
    private val handlers = mutableMapOf<String, Handler>()
    private var started = false

    fun on(type: String, handler: Handler) {
        check(!started) { "Register handlers before run()" }
        require(type != "init") { "init is handled by Runtime" }
        require(type !in handlers) { "Handler already registered: $type" }
        handlers[type] = handler
    }

    fun run() {
        check(!started) { "Runtime has already started" }
        started = true

        val input = System.`in`.bufferedReader(Charsets.UTF_8)
        val output = System.out.bufferedWriter(Charsets.UTF_8)

        fun write(message: Message<JsonObject>) {
            output.write(json.encodeToString(message))
            output.newLine()
            output.flush()
        }

        var node: NodeContext? = null
        while (true) {
            val line = input.readLine() ?: break
            val message = json.decodeFromString<Message<JsonObject>>(line)

            if (message.type == "init") {
                check(node == null) { "Node has already been initialized" }
                val init = json.decodeFromJsonElement<InitBody>(message.body)
                val initializedNode = NodeContext(
                    nodeId = init.nodeId,
                    nodeIds = init.nodeIds,
                    json = json,
                    write = ::write,
                )
                node = initializedNode
                initializedNode.reply(message, InitOkBody())
            } else {
                val initializedNode = checkNotNull(node) {
                    "Received ${message.type} before init"
                }
                dispatch(initializedNode, message)
            }
        }
    }

    private fun dispatch(node: NodeContext, message: Message<JsonObject>) {
        val handler = handlers[message.type]
            ?: error("No handler registered for: ${message.type}")
        handler(node, message)
    }
}
