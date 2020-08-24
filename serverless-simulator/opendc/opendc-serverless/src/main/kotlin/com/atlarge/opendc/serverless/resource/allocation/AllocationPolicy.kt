package com.atlarge.opendc.serverless.resource.allocation

import com.atlarge.opendc.serverless.resource.VirtualMachine

/**
 * A policy for selecting the [VirtualMachine] an instance should be deployed to,
 */
public interface AllocationPolicy {
    /**
     * The logic of the allocation policy.
     */
    public interface Logic {
        /**
         * Select the node on which the instance should be deployed
         */
        public fun select(virtMachines: Set<VirtualMachine>, requiredCpu: Double, requiredMemory: Double): VirtualMachine?
    }

    /**
     * Builds the logic of the policy.
     */
    operator fun invoke(): Logic
}
