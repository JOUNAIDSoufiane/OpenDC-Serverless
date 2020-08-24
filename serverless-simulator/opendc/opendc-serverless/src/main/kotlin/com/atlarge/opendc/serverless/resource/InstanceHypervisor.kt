package com.atlarge.opendc.serverless.resource

import java.util.*
import com.atlarge.opendc.serverless.monitor.FunctionProfile
import com.atlarge.opendc.serverless.compute.FunctionInstance
import com.atlarge.opendc.serverless.monitor.UsageMonitor
import kotlin.collections.ArrayList

/*
 * Hypervisor running on top of a VM, handles the actual deployment of function instances and manages them by providing
 * time updates.
 */
public class InstanceHypervisor(
        /**
         * The unique identifier of the server.
         */
        val uid: UUID,
        /**
         * The optional name of the server.
         */
        val name: String,
        /**
         * Name of the host virtual machine
         */
        private val hostMachine: String,
        var totalCpu: Double,
        var totalMemory: Double)
{
    private var currentTime:Long = 0
    var currentCpu:Double = 0.0
    var currentMemory:Double = 0.0
    private var runningInstances:Long = 0
    private var cycleInterval:Long = 0
    private var idleMemoryPenalty:Double = 0.0
    private lateinit var usageMonitor: UsageMonitor
    private lateinit var resourceScheduler: ResourceScheduler

    fun getIdleMemoryPenalty(instance: FunctionInstance):Double = instance.memoryUsage * idleMemoryPenalty

    fun setIdleMemoryPenalty(penalty:Double){
        idleMemoryPenalty = penalty
    }
    fun setInterval(duration: Long){
        cycleInterval = duration
    }
    fun setUsageMonitor(monitor: UsageMonitor){
        usageMonitor = monitor
    }
    fun setResourceScheduler(scheduler: ResourceScheduler){
        resourceScheduler = scheduler
    }

    /*
    * List of function instances deployed on this hypervisor
    */
    private val instances = ArrayList<FunctionInstance>()

    fun instances():ArrayList<FunctionInstance> = instances

    fun addInstance(instance: FunctionInstance) = instances.add(instance)

    fun getNrRunningInstances():Long = runningInstances

    /**
     * Provisions cpu and memory for the given [FunctionInstance], returns true if successful
     */
    fun provisionResources(instance: FunctionInstance, cpu:Double, mem:Double, prevPenalty:Double?):Boolean{
        return if (!instance.isIdle() && currentCpu + cpu <= totalCpu && currentMemory + mem <= totalMemory) {
            if (prevPenalty != null) // removing the idle memory usage penalty
                currentMemory -= prevPenalty
            currentCpu += cpu
            currentMemory += mem
            true
        }
        else
            false
    }

    /**
     * Provides time updates to the [FunctionInstance]s, terminates [FunctionInstance]s that have
     * been idle for longer than the idle timeout limit and updates the displayed resource usage
     */
    fun updateInstances(timestamp:Long){
        currentTime = timestamp
        currentCpu = 0.0
        currentMemory = 0.0
        runningInstances = 0

        val toTerminate = arrayListOf<FunctionInstance>()

        for (instance in instances){
            val funcProfile = usageMonitor.functionProfiles[instance.uid]!!
            instance.update(timestamp)
            if (!instance.isIdle()) {
                runningInstances++
                currentCpu += instance.cpuUsage
                currentMemory += instance.memoryUsage
            } else if (resourceScheduler.keepAlive(instance)){
                // TODO : consider making the idlePenalty based on provisioned memory and not last execution memory usage
                val idlePenalty = instance.memoryUsage * idleMemoryPenalty
                currentMemory +=  idlePenalty
                if (instance.wasRunning())
                    resourceScheduler.setPreWarmWindow(timestamp, instance.uid)
            }
            else
                toTerminate.add(instance)
        }

        for (instance in toTerminate) {
            usageMonitor.functionProfiles[instance.uid]!!.terminatedInstances++
            usageMonitor.terminatedInstances++
            instances.remove(instance)
        }
    }

    /**
     * updates [FunctionProfile] objects according to the state, resource usage and execution time of every instance
     */
    fun profileInstances(timestamp: Long){
        for (instance in instances){
            val funcProfile = usageMonitor.functionProfiles[instance.uid]!!
            if (!instance.isIdle()){
                funcProfile.runningInstances++
                funcProfile.cpuUsage += instance.cpuUsage
                funcProfile.memoryUsage += instance.memoryUsage

                // Logic to determine instance execution time during the cycle
                if (instance.isRunning() || instance.wasRunning()) {
                    val execTime = if (instance.endTime - timestamp >= cycleInterval){
                        if (instance.startTime >= timestamp  && instance.startTime <= (timestamp+cycleInterval))
                            (timestamp+cycleInterval) - instance.startTime
                        else
                            cycleInterval
                    } else{
                        if (instance.startTime >= timestamp  && instance.startTime <= (timestamp+cycleInterval))
                            instance.endTime - instance.startTime
                        else
                            instance.endTime - timestamp
                    }
                    funcProfile.cycleExecutionTime += execTime.toDouble()
                    usageMonitor.sumExecutionTime += execTime
                    funcProfile.totalExecutionTime += execTime
                }
            }
            else{
                funcProfile.idleInstances++
                funcProfile.memoryUsage += instance.memoryUsage * idleMemoryPenalty
                funcProfile.wastedMemoryTime += cycleInterval
            }
        }
    }

    override fun hashCode(): Int = uid.hashCode()
    override fun toString(): String = "$name hosted on $hostMachine using ($currentCpu / $totalCpu Mhz, $currentMemory / $totalMemory MB)"
    override fun equals(other: Any?): Boolean = other is InstanceHypervisor && uid == other.uid
}

