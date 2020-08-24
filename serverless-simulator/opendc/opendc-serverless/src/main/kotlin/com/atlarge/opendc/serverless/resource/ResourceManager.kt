package com.atlarge.opendc.serverless.resource

import com.atlarge.opendc.serverless.compute.FunctionInstance
import com.atlarge.opendc.serverless.compute.InvocationRequest
import com.atlarge.opendc.serverless.monitor.UsageMonitor
import java.util.*

/**
 * Resource management component, handles the management of resoures, provides time updates to all VMs and Instance Hypervisor,
 * Initialized with a set of [virtMachines].
 */
class ResourceManager(private var name:String,
                      private var virtMachines:SortedSet<VirtualMachine>,
                      private val resourceMonitor: UsageMonitor,
                      private val verbose: Boolean)
{
    private val functionInstances:MutableSet<FunctionInstance> = mutableSetOf()

    fun functionInstances(): MutableSet<FunctionInstance> = functionInstances

    fun addInstance(instance: FunctionInstance) = functionInstances.add(instance)

    fun virtMachines():SortedSet<VirtualMachine> = virtMachines

    fun vmCount():Int = virtMachines.size

    /**
     * Compute a set of [FunctionInstance] objects
     */
    fun computeInstanceViews(){
        functionInstances.clear()
        for (vm in virtMachines)
            functionInstances.addAll(vm.hypervisor.instances())
    }

    /**
     *  Returns a filtered set of instanceViews that are capable of handling the invocation [request]
     */
    fun filterInstances(request: InvocationRequest): MutableSet<FunctionInstance> {
        return functionInstances.asSequence().filter { instance ->
            val fitsMemory = instance.hypervisor.totalMemory - instance.hypervisor.currentMemory >= request.requiredMemory
            val fitsCpu = instance.hypervisor.totalCpu - instance.hypervisor.currentCpu >= request.requiredCpu
            fitsMemory && fitsCpu
        }.toMutableSet()
    }

    /**
     * Initializes the timeout, idle instance memory penalty and cycleInterval of each [VirtualMachine]'s [InstanceHypervisor]
     */
    fun initHypervisors(idleMemoryPenalty:Double, cycleInterval:Long, scheduler: ResourceScheduler){
        for (vm in virtMachines) {
            vm.hypervisor.setInterval(cycleInterval)
            vm.hypervisor.setIdleMemoryPenalty(idleMemoryPenalty)
            vm.hypervisor.setUsageMonitor(resourceMonitor)
            vm.hypervisor.setResourceScheduler(scheduler)
        }
    }

    /**
     * Calls updateInstances on every [VirtualMachine]'s [InstanceHypervisor] and returns the total number of currently running instances
     */
    fun monitoringCycle(timestamp: Long):Long{
        var totalRunningInstances:Long = 0

        if (verbose){
            println()
            println("MONITORING CYCLE $timestamp")
        }

        for (vm in virtMachines){
            vm.hypervisor.updateInstances(timestamp)
            totalRunningInstances += vm.hypervisor.getNrRunningInstances()
            if (verbose)
                println(vm.hypervisor)
        }

        return totalRunningInstances
    }

    /**
     * Calls profileInstances on every [VirtualMachine]'s [InstanceHypervisor]
     */
    fun profilingCycle(timestamp: Long) {
        for (vm in virtMachines)
            vm.hypervisor.profileInstances(timestamp)

        resourceMonitor.failedExecutions += resourceMonitor.functionProfiles.values.sumBy {it.failedExecutions.toInt()}
    }

    /*
     * terminates the resource management service
     */
    fun terminate(){
        virtMachines.clear()
        if (verbose)
            println("Resource manager has been terminated")
    }
}