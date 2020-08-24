package com.atlarge.opendc.serverless.compute.routing

import com.atlarge.opendc.serverless.compute.FunctionInstance
import com.atlarge.opendc.serverless.compute.InvocationRequest
import kotlin.Comparator

/**
 * The logic for a [RoutingPolicy] that uses a [Comparator] to select the appropriate vm.
 */
interface ComparableRoutingPolicyLogic : RoutingPolicy.Logic {
    /**
     * The comparator to use.
     */
    public val comparator: Comparator<FunctionInstance>

    override fun select(instances: MutableSet<FunctionInstance>, request: InvocationRequest): FunctionInstance? {
        return instances.asSequence()
            .filter { instance ->
                val isIdle = instance.isIdle()
                val fitsRequest = instance.uid == request.image.uid
                isIdle && fitsRequest
            }
            .minWith(comparator.thenBy { it.uid })
    }
}