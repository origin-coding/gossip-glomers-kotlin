package io.github.origincoding.gossipglomers.challenges.kafka

enum class KafkaMode {
    DIRECT,
    HASH,
    BATCH;

    companion object {
        fun fromEnvironment(environment: Map<String, String> = System.getenv()): KafkaMode =
            when (val mode = environment["KAFKA_MODE"] ?: "batch") {
                "direct" -> DIRECT
                "hash" -> HASH
                "batch" -> BATCH
                else -> error("Unknown KAFKA_MODE '$mode'; use direct, hash, or batch")
            }
    }
}
