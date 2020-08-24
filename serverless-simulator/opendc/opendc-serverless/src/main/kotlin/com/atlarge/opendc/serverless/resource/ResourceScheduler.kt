package com.atlarge.opendc.serverless.resource

import com.atlarge.opendc.serverless.compute.FunctionDeployer
import com.atlarge.opendc.serverless.compute.FunctionInstance
import com.atlarge.opendc.serverless.monitor.UsageMonitor
import com.atlarge.opendc.serverless.resource.allocation.AllocationPolicy
import com.atlarge.opendc.serverless.resource.management.ResourceManagementPolicy
import java.util.*

/**
 * Component responsible for managing the selection of Instance Hypervisors and the management of Instance lifetimes.
 */
class ResourceScheduler(private val resourceManager: ResourceManager,
                        private val usageMonitor: UsageMonitor,
                        private val allocationPolicy: AllocationPolicy,
                        private val managementPolicy: ResourceManagementPolicy,
                        private val cycleInterval: Long)
{
    lateinit var functionDeployer: FunctionDeployer
    private val allocationLogic:AllocationPolicy.Logic = allocationPolicy()
    private val managementLogic:ResourceManagementPolicy.Logic = managementPolicy()

    fun updateManagementPolicy(uid: UUID, timestamp: Long, idleTime: Long) = managementLogic.update(uid, timestamp, idleTime)

    /**
     * selects a Virtual machine's [InstanceHypervisor] to deploy the request on according to an [AllocationPolicy]
     */
    fun selectHypervisor(requiredCpu: Double, requiredMemory: Double):InstanceHypervisor?{
        return allocationLogic.select(resourceManager.virtMachines(), requiredCpu, requiredMemory)?.hypervisor
    }

    /**
     * Decides whether to keep the given [FunctionInstance] alive according to the [ResourceManagementPolicy]
     */
    fun keepAlive(instance: FunctionInstance): Boolean{
        val timeWindow = managementLogic.getTimes(instance.uid)

        return if  (timeWindow.second == null || instance.idleTime <= timeWindow.second!!)
            true
        else{
            resourceManager.functionInstances().remove(instance)
            false
        }
    }

    /**
     * Spawns new warm [FunctionInstance] objects based on functions pre warming times
     * Pre warms one instance for every finished execution
     */
    fun preWarm(timestamp:Long){
        for (uid in usageMonitor.functionProfiles.keys){
            if (usageMonitor.functionProfiles[uid]!!.preWarmingWindows.isNotEmpty()) {
                for (warmUpTime in usageMonitor.functionProfiles[uid]!!.preWarmingWindows) {
                    if (warmUpTime >= timestamp && timestamp + cycleInterval >= warmUpTime) {
                        functionDeployer.deploy(uid, warmUpTime)
                    }
                }
                usageMonitor.functionProfiles[uid]!!.preWarmingWindows.removeIf { it <= timestamp + cycleInterval }
            }
        }
    }

    /**
     * Sets one pre warming window
     */
    fun setPreWarmWindow(timestamp:Long, uid: UUID){
        val preWarmingTime = managementLogic.getTimes(uid).first!!
        if (preWarmingTime != 0.toLong())
            usageMonitor.functionProfiles[uid]!!.preWarmingWindows.add(timestamp + preWarmingTime)
    }

}



