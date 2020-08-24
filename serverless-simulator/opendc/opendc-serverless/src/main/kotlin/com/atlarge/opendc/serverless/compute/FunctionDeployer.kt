package com.atlarge.opendc.serverless.compute

import com.atlarge.opendc.serverless.monitor.UsageMonitor
import com.atlarge.opendc.serverless.resource.ResourceManager
import com.atlarge.opendc.serverless.resource.ResourceScheduler
import java.util.*

/**
 * This component is responsible for deployment of new [FunctionInstance] objects
 */
class FunctionDeployer(private val delayInjector: DelayInjector,
                       private val cycleInterval:Long,
                       private val verbose: Boolean,
                       private val usageMonitor: UsageMonitor,
                       private val resourceScheduler: ResourceScheduler,
                       private val resourceManager: ResourceManager)
{
    /**
     * Deploys a fresh [FunctionInstance] to execute the [InvocationRequest]
     */
    fun deploy(request: InvocationRequest, timestamp:Long):Boolean {
        val hypervisor = resourceScheduler.selectHypervisor(request.requiredCpu, request.requiredMemory)

        if (hypervisor == null){
            if (request.originalTick == timestamp && verbose)
                println("All virtual machines busy, request is queued back")
            else {
                if (verbose)
                    println("Deployment failed, attempting next cycle")
            }
            return false
        }

        val image = request.image
        val funcProfile = usageMonitor.functionProfiles[image.uid]!!
        val instance = FunctionInstance(image.uid, image.name, request.requiredCpu, request.requiredMemory, cycleInterval, hypervisor)
        val delay = delayInjector.getColdStartDelay(request.provisionedMemory)
        val endTime = timestamp + request.execDuration + delay

        if (delay > cycleInterval)
            instance.sleepUntil(timestamp + delay)

        instance.invoke(timestamp + delay, endTime, request.requiredCpu, request.requiredMemory)

        hypervisor.provisionResources(instance, request.requiredCpu, request.requiredMemory, prevPenalty = null)
        hypervisor.addInstance(instance)
        resourceManager.addInstance(instance)

        usageMonitor.coldStarts++
        funcProfile.cycleColdStarts++
        funcProfile.totalColdStarts++
        funcProfile.coldStartDurations.add(delay)

        if (verbose)
            println("[$timestamp] Function instance ${image.name} deployed and running on ${hypervisor.name} until ${instance.endTime} (delay= $delay ms) using (cpu= ${request.requiredCpu} Mhz, memory= ${request.requiredMemory} mb)")

        return true
    }

    /**
     * Deploys a warm [FunctionInstance] awaiting execution
     */
    fun deploy(uid: UUID, timestamp:Long):Boolean {
        val hypervisor = resourceScheduler.selectHypervisor(10.0, 10.0)
        val funcName = usageMonitor.functionProfiles[uid]!!.name

        if (hypervisor == null){
            if (verbose)
                println("[$timestamp] Function instance $funcName pre warming aborted! Not enough resources")
            return false
        }

        val instance = FunctionInstance(uid, usageMonitor.functionProfiles[uid]!!.name, 0.0, 0.0, cycleInterval, hypervisor)
        instance.endTime = timestamp // hack to avoid instant timeout

        hypervisor.addInstance(instance)
        resourceManager.addInstance(instance)

        if (verbose)
            println("[$timestamp] Function instance $funcName pre warmed on ${hypervisor.name}")

        return true
    }
}