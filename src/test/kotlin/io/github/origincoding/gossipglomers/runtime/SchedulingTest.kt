package io.github.origincoding.gossipglomers.runtime

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout

class SchedulingTest {
    @Test
    fun `periodic task does not overlap and is cancellable`() = runBlocking<Unit> {
        val h = NodeHarness(this)
        val completions = Channel<Unit>(Channel.UNLIMITED)
        var active = 0
        var maximum = 0
        val job = h.node.every(5.milliseconds) {
            active++
            maximum = maxOf(maximum, active)
            delay(20.milliseconds)
            active--
            completions.send(Unit)
        }
        withTimeout(5000) { repeat(3) { completions.receive() } }
        job.cancelAndJoin()
        assertEquals(1, maximum)
        assertEquals(0, active)
        assertFailsWith<IllegalArgumentException> { h.node.every(Duration.ZERO) {} }
    }
}
