package io.github.origincoding.gossipglomers.runtime

import java.io.InputStream
import java.io.OutputStream
import kotlin.concurrent.thread
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.launch
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json

internal class StdioTransport(
    private val json: Json,
    private val input: InputStream,
    private val output: OutputStream,
) {
    private val incoming = Channel<String>(64)
    private val outgoing = Channel<Envelope>(64)
    private var reader: Thread? = null

    fun start(scope: CoroutineScope): Job {
        val writer = scope.launch(Dispatchers.IO) {
            val buffer = output.bufferedWriter(Charsets.UTF_8)
            for (message in outgoing) {
                buffer.write(json.encodeToString(message))
                buffer.newLine()
                buffer.flush()
            }
        }
        reader = thread(name = "maelstrom-stdin", isDaemon = true) {
            try {
                val buffer = input.bufferedReader(Charsets.UTF_8)
                while (true) {
                    val line = buffer.readLine() ?: break
                    incoming.trySendBlocking(line).getOrThrow()
                }
                incoming.close()
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
            } catch (_: CancellationException) {
                // Runtime 关闭时取消队列，唤醒等待投递的读取线程。
            } catch (e: Exception) {
                incoming.close(e)
            }
        }
        return writer
    }

    suspend fun receive(): Envelope? {
        val result = incoming.receiveCatching()
        if (result.isClosed) {
            result.exceptionOrNull()?.let { throw it }
            return null
        }
        return json.decodeFromString<Envelope>(result.getOrThrow())
    }

    suspend fun send(message: Envelope) = outgoing.send(message)

    fun stopReading() {
        incoming.cancel()
        // readLine 不保证响应 interrupt，因此不等待这个守护线程。
        reader?.interrupt()
    }

    fun finishWriting() {
        outgoing.close()
    }
}
