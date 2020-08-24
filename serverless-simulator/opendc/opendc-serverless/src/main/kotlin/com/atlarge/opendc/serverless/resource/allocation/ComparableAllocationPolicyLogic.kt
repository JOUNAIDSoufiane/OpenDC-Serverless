package com.atlarge.opendc.serverless.resource.allocation

import com.atlarge.opendc.serverless.resource.VirtualMachine

/**
 * The logic for an [AllocationPolicy] that uses a [Comparator] to select the appropriate vm.
 */
interface ComparableAllocationPolicyLogic : AllocationPolicy.Logic {
    /**
     * The comparator to use.
     */
    public val comparator: Comparator<VirtualMachine>

    override fun select(virtMachines: Set<VirtualMachine>, requiredCpu: Double, requiredMemory: Double): VirtualMachine? {
        return virtMachines.asSequence()
                .filter { vm ->
                    val fitsMemory = vm.hypervisor.totalMemory - vm.hypervisor.currentMemory >= requiredMemory
                    val fitsCpu = vm.hypervisor.totalCpu - vm.hypervisor.currentCpu >= requiredCpu
                    fitsMemory && fitsCpu
                }
                .minWith(comparator.thenBy { it.hypervisor.uid })
    }
}