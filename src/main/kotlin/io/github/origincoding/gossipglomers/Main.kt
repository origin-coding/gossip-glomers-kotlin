package io.github.origincoding.gossipglomers

import kotlin.system.exitProcess

fun main(args: Array<String>) {
    val challenge = if (args.isEmpty()) System.getenv("GOSSIP_GLOMERS_CHALLENGE") else args.singleOrNull()
    when (challenge) {
        "echo" -> io.github.origincoding.gossipglomers.challenges.echo.run()
        "unique-id" -> io.github.origincoding.gossipglomers.challenges.uniqueid.run()
        "broadcast" -> io.github.origincoding.gossipglomers.challenges.broadcast.run()
        "g-counter" -> io.github.origincoding.gossipglomers.challenges.gcounter.run()
        "kafka" -> io.github.origincoding.gossipglomers.challenges.kafka.run()
        "txn" -> io.github.origincoding.gossipglomers.challenges.txn.run()
        null, "--help", "-h" -> System.err.println("Choose a challenge: echo, unique-id, broadcast, g-counter, kafka, txn")
        else -> {
            System.err.println("Unknown challenge. Use --help to list available challenges.")
            exitProcess(2)
        }
    }
}
