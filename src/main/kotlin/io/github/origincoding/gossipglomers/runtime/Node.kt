package io.github.origincoding.gossipglomers.runtime

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.decodeFromJsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put

/** 节点状态、handler 和后台任务都在 Runtime 的同一线程访问；挂起点允许操作交错。 */
class Node internal constructor(
    val id: String,
    val nodeIds: List<String>,
    val json: Json,
    private val scope: CoroutineScope,
    private val write: suspend (Envelope) -> Unit,
) {
    private data class Handler(
        val responseType: String,
        val handle: suspend (Envelope) -> JsonObject,
    )

    private data class PendingRpc(
        val destination: String,
        val expectedType: String,
        val response: CompletableDeferred<Envelope>,
    )

    private val handlers = mutableMapOf<String, Handler>()
    private val pending = mutableMapOf<Long, PendingRpc>()
    private var nextMessageId = 1L
    private var registrationClosed = false

    internal val pendingRpcCount: Int get() = pending.size

    inline fun <reified I, reified O> on(
        type: String,
        responseType: String = "${type}_ok",
        noinline handler: suspend (Request<I>) -> O,
    ) {
        register(type, responseType) { message ->
            val decoded = try {
                json.decodeFromJsonElement<I>(message.body)
            } catch (e: SerializationException) {
                throw RpcException(ErrorCode.MALFORMED_REQUEST, e.message ?: "Cannot decode request")
            }
            val request = Request(message.src, requireNotNull(message.msgId), decoded)
            json.encodeToJsonElement(handler(request)).jsonObject
        }
    }

    @PublishedApi
    internal fun register(
        type: String,
        responseType: String,
        handler: suspend (Envelope) -> JsonObject,
    ) {
        check(!registrationClosed) { "Register handlers during node configuration" }
        require(type != "init") { "init is handled by Runtime" }
        require(type !in handlers) { "Handler already registered: $type" }
        handlers[type] = Handler(responseType, handler)
    }

    internal fun finishRegistration() {
        registrationClosed = true
    }

    /** 生命周期属于节点；预期失败由任务自己处理，未知错误使节点失败。 */
    fun background(block: suspend () -> Unit): Job = scope.launch { block() }

    suspend inline fun <reified I, reified O> rpc(
        destination: String,
        type: String,
        body: I,
        expectedType: String = "${type}_ok",
        timeout: Duration = 1.seconds,
    ): O = json.decodeFromJsonElement(
        rpc(destination, type, json.encodeToJsonElement(body).jsonObject, expectedType, timeout)
    )

    /** 单次调用，不自动重试；包括等待输出队列在内，整个调用受 timeout 约束。 */
    suspend fun rpc(
        destination: String,
        type: String,
        body: JsonObject = JsonObject(emptyMap()),
        expectedType: String = "${type}_ok",
        timeout: Duration = 1.seconds,
    ): JsonObject {
        require(timeout.isPositive() && timeout.isFinite()) { "RPC timeout must be positive and finite" }
        val requestId = nextMessageId++
        val waiter = CompletableDeferred<Envelope>()
        pending[requestId] = PendingRpc(destination, expectedType, waiter)
        try {
            val response = withTimeoutOrNull(timeout) {
                emit(destination, type, body, messageId = requestId)
                waiter.await()
            } ?: throw RpcTimeoutException(destination, type)

            if (response.type == "error") {
                throw RpcException(
                    response.body.getValue("code").jsonPrimitive.int,
                    response.body["text"]?.jsonPrimitive?.content ?: "Remote RPC error",
                )
            }
            return response.body
        } finally {
            pending.remove(requestId)
            waiter.cancel()
        }
    }

    internal fun acceptResponse(message: Envelope) {
        val requestId = requireNotNull(message.inReplyTo)
        val call = pending[requestId] ?: return // 重复、迟到或已经取消。
        if (message.src != call.destination) return
        pending.remove(requestId)
        if (message.type != call.expectedType && message.type != "error") {
            call.response.completeExceptionally(
                IllegalStateException("Expected ${call.expectedType} from ${call.destination}, got ${message.type}")
            )
        } else {
            call.response.complete(message)
        }
    }

    internal fun acceptRequest(message: Envelope) {
        requireNotNull(message.msgId) { "Request ${message.type} has no msg_id" }
        scope.launch {
            val handler = handlers[message.type]
            if (handler == null) {
                replyError(message, ErrorCode.NOT_SUPPORTED, "Unsupported request: ${message.type}")
                return@launch
            }
            val response = try {
                handler.handle(message)
            } catch (e: RpcException) {
                replyError(message, e.code, e.message ?: "RPC error")
                return@launch
            } catch (e: RpcTimeoutException) {
                replyError(message, ErrorCode.TIMEOUT, e.message ?: "RPC timed out")
                return@launch
            }
            // 取消继续向上传播，编程错误也不伪装成 RPC 失败。
            reply(message, handler.responseType, response)
        }
    }

    internal suspend fun reply(request: Envelope, type: String, body: JsonObject) {
        emit(request.src, type, body, replyTo = requireNotNull(request.msgId))
    }

    private suspend fun replyError(request: Envelope, code: Int, text: String) {
        reply(request, "error", buildJsonObject {
            put("code", code)
            put("text", text)
        })
    }

    private suspend fun emit(
        destination: String,
        type: String,
        body: JsonObject,
        messageId: Long = nextMessageId++,
        replyTo: Long? = null,
    ) {
        require(body.keys.none { it == "type" || it == "msg_id" || it == "in_reply_to" }) {
            "Message headers are managed by Node"
        }
        val fields = body.toMutableMap()
        fields["type"] = JsonPrimitive(type)
        fields["msg_id"] = JsonPrimitive(messageId)
        if (replyTo != null) fields["in_reply_to"] = JsonPrimitive(replyTo)
        write(Envelope(id, destination, JsonObject(fields)))
    }
}
