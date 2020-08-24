package com.atlarge.opendc.serverless.compute.routing

import com.atlarge.opendc.serverless.compute.InvocationRequest
import com.atlarge.opendc.serverless.compute.FunctionInstance

/**
 * A policy for selecting the [FunctionInstance] that contains an idle instance to route the request to
 */
public interface RoutingPolicy {
    /**
     * The logic of the allocation policy.
     */
    public interface Logic {
        /**
         * Select the instance to which the request should be routed to
         */
        public fun select(instances: MutableSet<FunctionInstance>, request: InvocationRequest): FunctionInstance?
    }
    /**
     * Builds the logic of the policy.
     */
    operator fun invoke(): Logic
}

