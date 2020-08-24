package com.atlarge.opendc.serverless.resource.management

import java.util.*

/**
 * A policy for selecting the pre warm and keep alive times
 */
public interface ResourceManagementPolicy {
    /**
     * The logic of the resource management policy.
     */
    public interface Logic{
        /**
         * update the policy
         */
        public fun update(funcUid: UUID, timestamp: Long, idleTime:Long)

        /**
         * Returns a pair of the function's pre warm and keep alive time
         */
        public fun getTimes(funcUid: UUID): Pair<Long?,Long?>
    }

    /**
     * Builds the logic of the policy.
     */
    operator fun invoke(): ResourceManagementPolicy.Logic
}