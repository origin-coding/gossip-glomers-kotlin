package io.github.origincoding.gossipglomers.runtime

import kotlin.time.Duration
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive

/** 首次等待 interval；任务完成后再次等待，不重叠执行，不保证固定频率。 */
fun Node.every(interval: Duration, task: suspend () -> Unit): Job {
    require(interval.isPositive() && interval.isFinite()) { "Interval must be positive and finite" }
    return background {
        while (currentCoroutineContext().isActive) {
            delay(interval)
            task()
        }
    }
}
