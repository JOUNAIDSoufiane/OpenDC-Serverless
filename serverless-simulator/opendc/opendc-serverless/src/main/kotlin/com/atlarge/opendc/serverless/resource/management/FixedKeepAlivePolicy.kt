package com.atlarge.opendc.serverless.resource.management

import com.atlarge.opendc.serverless.monitor.UsageMonitor
import java.util.*
/**
 * Resource management policy that specifies a fixed keep-alive time for idle instances.
 *
 * keep-alive is set to [timeout].
 *
 * Sets both the pre-warming time to 0 to disable pre-warming.
 */
class FixedKeepAlivePolicy(val funcUids: Set<UUID>, val timeout: Long?, val usageMonitor: UsageMonitor): ResourceManagementPolicy{
    override fun invoke(): ResourceManagementPolicy.Logic = object : ResourceManagementPolicy.Logic{

        init {
            if (timeout == null)
                throw IllegalArgumentException("Bad FixedKeepAlive timeout argument")
            for (uid in funcUids)
                usageMonitor.functionProfiles[uid]!!.timeWindows = Pair(0.toLong(), timeout)
        }

        override fun update(uid: UUID, timestamp: Long, idleTime: Long) {}

        override fun getTimes(uid: UUID): Pair<Long?, Long?> = usageMonitor.functionProfiles[uid]!!.timeWindows
    }
}