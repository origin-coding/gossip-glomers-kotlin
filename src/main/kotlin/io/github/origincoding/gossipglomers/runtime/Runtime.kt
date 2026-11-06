package io.github.origincoding.gossipglomers.runtime

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlin.concurrent.thread
import kotlin.time.Duration.Companion.milliseconds

// Handler 只处理当前事件并返回；等待 ACK 用节点状态记录，由后续消息或 tick 推进。
typealias Handler = (NodeContext, Message<JsonObject>) -> Unit

private sealed interface Event {
    data class Incoming(val line: String) : Event
    data object Tick : Event
    data object EndOfInput : Event
}

class Runtime(private val tickMillis: Long = 250) {
    init {
        require(tickMillis >= 0) { "tickMillis must not be negative" }
    }

    private val json = Json {
        encodeDefaults = true
        ignoreUnknownKeys = true
    }
    private val handlers = mutableMapOf<String, Handler>()
    private var tickHandler: ((NodeContext) -> Unit)? = null
    private var started = false

    fun on(type: String, handler: Handler) {
        check(!started) { "Register handlers before run()" }
        require(type != "init") { "init is handled by Runtime" }
        require(type != "topology") { "topology is handled by Runtime" }
        require(type !in handlers) { "Handler already registered: $type" }
        handlers[type] = handler
    }

    /**
     * topology 处理成功后才调用，和消息 handler 在同一线程串行执行。
     * tickMillis 是事件产生的间隔；回调可能因排队而延迟，不是精确定时。
     */
    fun onTick(handler: (NodeContext) -> Unit) {
        check(!started) { "Register handlers before run()" }
        check(tickHandler == null) { "Tick handler already registered" }
        require(tickMillis > 0) { "Ticks are disabled when tickMillis is 0" }
        tickHandler = handler
    }

    // runBlocking 把同步入口接到协程世界，事件循环仍在调用线程执行。
    fun run(): Unit = runBlocking {
        check(!started) { "Runtime has already started" }
        started = true

        // 队列满时生产者等待，避免输入或 tick 无限制堆积。
        val events = Channel<Event>(capacity = 64)

        // launch 继承当前上下文；定时器只投递事件，不访问节点状态。
        val ticker = if (tickHandler != null) {
            launch {
                while (true) {
                    delay(tickMillis.milliseconds)
                    events.send(Event.Tick)
                }
            }
        } else null
        val reader = startReader(events)

        try {
            EventLoop().consume(events)
        } finally {
            // handler 抛异常时也清理，异常继续向上传递。
            ticker?.cancel()
            events.cancel()
            reader.interrupt()
        }
    }

    // 本次运行的状态只由消费者持有，输入线程和定时器只接触事件队列。
    private inner class EventLoop {
        private val output = System.out.bufferedWriter(Charsets.UTF_8)
        private var node: NodeContext? = null
        private var topologyReady = false

        suspend fun consume(events: Channel<Event>) {
            for (event in events) {
                when (event) {
                    is Event.Incoming -> handleIncoming(event.line)
                    Event.Tick -> if (topologyReady) {
                        tickHandler?.invoke(checkNotNull(node))
                    }
                    // EOF 也排队，因此退出前会处理完它之前的所有输入。
                    Event.EndOfInput -> return
                }

                // 队列持续有消息时 receive 不一定挂起，给定时器运行机会。
                yield()
            }
        }

        private fun handleIncoming(line: String) {
            val message = json.decodeFromString<Message<JsonObject>>(line)
            if (message.type == "init") {
                check(node == null) { "Node has already been initialized" }
                node = handleInit(message, ::write)
                return
            }

            val initialized = checkNotNull(node) {
                "Received ${message.type} before init"
            }
            if (message.type == "topology") {
                handleTopology(initialized, message)
                topologyReady = true
                return
            }

            dispatch(initialized, message)
        }

        private fun write(message: Message<JsonObject>) {
            output.write(json.encodeToString(message))
            output.newLine()
            output.flush()
        }
    }

    private fun startReader(events: Channel<Event>): Thread = thread(name = "runtime-stdin", isDaemon = true) {
        // readLine 是阻塞 IO，协程 cancel 无法保证打断它。
        // Maelstrom 每个节点是独立进程：用守护线程隔离读取，退出时不等待它。
        val input = System.`in`.bufferedReader(Charsets.UTF_8)
        try {
            while (true) {
                val line = input.readLine() ?: break
                // 从普通线程向协程队列投递；队列满时阻塞读取线程，不丢消息。
                events.trySendBlocking(Event.Incoming(line)).getOrThrow()
            }
            events.trySendBlocking(Event.EndOfInput).getOrThrow()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (_: CancellationException) {
            // 主循环退出时会取消队列，唤醒正在等待投递的读取线程。
        } catch (e: Exception) {
            // 读取失败也交给消费者感知，不让主循环永久等待下一条输入。
            events.close(e)
        }
    }

    private fun handleInit(
        message: Message<JsonObject>,
        write: (Message<JsonObject>) -> Unit,
    ): NodeContext {
        val init = json.decodeFromJsonElement<InitBody>(message.body)
        val node = NodeContext(
            nodeId = init.nodeId,
            nodeIds = init.nodeIds,
            json = json,
            write = write,
        )
        node.reply(message, InitOkBody())
        return node
    }

    private fun handleTopology(node: NodeContext, message: Message<JsonObject>) {
        val topology = node.decode<TopologyBody>(message)
        initStarTopology(node)
        node.reply(message, TopologyOkBody())
    }

    private fun initDefaultTopology(node: NodeContext, topology: TopologyBody) {
        val neighbors = requireNotNull(topology.topology[node.nodeId]) {
            "Topology does not contain node ${node.nodeId}"
        }
        node.updateNeighbors(neighbors)
    }

    private fun initStarTopology(node: NodeContext) {
        val centerNodeId = node.nodeIds.first()
        val neighbors = if (node.nodeId == centerNodeId) {
            node.nodeIds - centerNodeId
        } else {
            listOf(centerNodeId)
        }
        node.updateNeighbors(neighbors)
    }

    private fun dispatch(node: NodeContext, message: Message<JsonObject>) {
        val handler = handlers[message.type]
            ?: error("No handler registered for: ${message.type}")
        handler(node, message)
    }
}
