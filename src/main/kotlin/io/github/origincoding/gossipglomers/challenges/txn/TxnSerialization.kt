package io.github.origincoding.gossipglomers.challenges.txn

import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.longOrNull

object OperationSerializer : KSerializer<Operation> {
    override val descriptor: SerialDescriptor = JsonArray.serializer().descriptor

    override fun serialize(encoder: Encoder, value: Operation) = when (value) {
        is Operation.Read -> encodeTriple(encoder, "r", value.key, null)
        is Operation.Write -> encodeTriple(encoder, "w", value.key, value.value)
    }

    override fun deserialize(decoder: Decoder): Operation {
        val (kind, key, value) = decodeTriple(decoder)

        return when (kind) {
            "r" -> {
                if (value != null) {
                    throw SerializationException("Read request must have a null third element")
                }
                Operation.Read(key)
            }

            "w" -> Operation.Write(key, requireWriteValue(value))
            else -> throw SerializationException("Unknown operation: $kind")
        }
    }
}

object OperationResultSerializer : KSerializer<OperationResult> {
    override val descriptor: SerialDescriptor = JsonArray.serializer().descriptor

    override fun serialize(encoder: Encoder, value: OperationResult) = when (value) {
        is OperationResult.Read -> encodeTriple(encoder, "r", value.key, value.value)
        is OperationResult.Write -> encodeTriple(encoder, "w", value.key, value.value)
    }

    override fun deserialize(decoder: Decoder): OperationResult {
        val (kind, key, value) = decodeTriple(decoder)

        return when (kind) {
            "r" -> OperationResult.Read(key, value)
            "w" -> OperationResult.Write(key, requireWriteValue(value))
            else -> throw SerializationException("Unknown operation: $kind")
        }
    }
}

private fun encodeTriple(encoder: Encoder, kind: String, key: Long, value: Long?) {
    val jsonEncoder = encoder as? JsonEncoder ?: throw SerializationException("Only JSON is supported")

    jsonEncoder.encodeJsonElement(buildJsonArray {
        add(kind)
        add(key)
        add(value)
    })
}

private fun decodeTriple(decoder: Decoder): Triple<String, Long, Long?> {
    val jsonDecoder = decoder as? JsonDecoder ?: throw SerializationException("Only JSON is supported")

    val array =
        jsonDecoder.decodeJsonElement() as? JsonArray ?: throw SerializationException("Expected an operation array")

    if (array.size != 3) {
        throw SerializationException("Expected exactly 3 elements")
    }

    val kind = array[0] as? JsonPrimitive
    if (kind == null || !kind.isString) {
        throw SerializationException("Operation kind must be a string")
    }

    val key = decodeLong(array[1])
    val value = if (array[2] == JsonNull) null else decodeLong(array[2])

    return Triple(kind.content, key, value)
}

private fun decodeLong(element: JsonElement): Long {
    val primitive = element as? JsonPrimitive ?: throw SerializationException("Expected a Long")

    if (primitive.isString) {
        throw SerializationException("Expected a number, not a string")
    }

    return primitive.longOrNull ?: throw SerializationException("Expected a Long")
}

private fun requireWriteValue(value: Long?): Long =
    value ?: throw SerializationException("Write value must not be null")
