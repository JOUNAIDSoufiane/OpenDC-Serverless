package com.atlarge.opendc.serverless.resource.management

import com.atlarge.opendc.serverless.monitor.UsageMonitor
import java.util.*

/**
 * Resource management policy that does not terminate any idle instances,
 *
 * Sets both the pre-warming and keep-alive times to 0 to disable pre-warming and termination
 */
class NoTerminationPolicy(val usageMonitor: UsageMonitor): ResourceManagementPolicy{
    override fun invoke(): ResourceManagementPolicy.Logic = object : ResourceManagementPolicy.Logic{
        override fun update(uid: UUID, timestamp: Long, idleTime: Long) {}

        override fun getTimes(uid: UUID): Pair<Long?, Long?> = usageMonitor.functionProfiles[uid]!!.timeWindows
    }
}