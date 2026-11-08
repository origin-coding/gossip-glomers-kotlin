package io.github.origincoding.gossipglomers.runtime

import java.io.InputStream
import java.io.OutputStream
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement

class Runtime(
    private val input: InputStream = System.`in`,
    private val output: OutputStream = System.out,
) {
    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true
    }
    private var started = false

    fun run(configure: Node.() -> Unit): Unit = runBlocking {
        check(!started) { "Runtime has already started" }
        started = true
        val transport = StdioTransport(json, input, output)
        val writer = transport.start(this)
        try {
            // 继承 runBlocking 的单线程事件循环，只有阻塞 IO 在别处运行。
            coroutineScope {
                val first = transport.receive() ?: return@coroutineScope
                require(first.type == "init" && first.inReplyTo == null) { "First message must be init" }
                val init = json.decodeFromJsonElement<InitBody>(first.body)
                require(first.dest == init.nodeId)
                val node = Node(init.nodeId, init.nodeIds, json, this, transport::send)
                try {
                    node.configure()
                    node.finishRegistration()
                    node.reply(first, "init_ok", JsonObject(emptyMap()))
                    while (true) {
                        val message = transport.receive() ?: break
                        require(message.dest == node.id) { "Message addressed to ${message.dest}" }
                        if (message.inReplyTo != null) {
                            node.acceptResponse(message)
                        } else {
                            require(message.type != "init") { "Node has already been initialized" }
                            node.acceptRequest(message)
                        }
                        yield()
                    }
                } finally {
                    // EOF 后不会再收到响应，取消等待者和后台任务。
                    coroutineContext.cancelChildren()
                }
            }
        } finally {
            transport.stopReading()
            transport.finishWriting()
            // 正常 EOF 时排空已入队的输出；不等待未完成的网络操作。
            withContext(NonCancellable) { writer.join() }
        }
    }
}
