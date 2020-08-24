package com.atlarge.opendc.serverless.compute

import java.util.*
import com.atlarge.opendc.compute.core.image.FuncImage
import com.atlarge.opendc.serverless.monitor.UsageMonitor
import com.atlarge.opendc.serverless.compute.routing.RoutingPolicy
import com.atlarge.opendc.serverless.resource.ResourceManager

/*
 * Component responsible for routing incoming events to the correct function Instance,
 * if unavailable, it queues the request while awaiting a deployment of new instances
 */
class FunctionRouter(private val queue: Queue<InvocationRequest>,
                     private val verbose: Boolean,
                     private val deployer: FunctionDeployer,
                     private val usageMonitor: UsageMonitor,
                     private val resourceManager: ResourceManager,
                     routingPolicy: RoutingPolicy)
{
    private val routingLogic:RoutingPolicy.Logic = routingPolicy()

    fun enqueueRequest(request: InvocationRequest):Boolean {
        if (queue.add(request))
            return true
        else
            throw EnqueueFailException("max queue limit reached")
    }

    fun getNrQueuedRequests(): Int {
        return queue.size
    }

    /**
     * Routes the incoming [InvocationRequest] to a fitting idle instance chosen via a [RoutingPolicy]
     */
    private fun routeRequest(request: InvocationRequest, timestamp: Long): Boolean {
        val instance = routingLogic.select(resourceManager.filterInstances(request), request) ?: return false

        val prevIdlePenalty = instance.hypervisor.getIdleMemoryPenalty(instance)
        val endTime = timestamp + request.execDuration

        instance.invoke(timestamp ,endTime, request.requiredCpu, request.requiredMemory)
        instance.hypervisor.provisionResources(instance, request.requiredCpu, request.requiredMemory, prevIdlePenalty)

        if (this.verbose)
            println("[$timestamp] Function instance ${request.image.name} running on ${instance.hypervisor.name} until $endTime using (cpu= ${request.requiredCpu} Mhz, memory= ${request.requiredMemory} MB)")

        return true
    }

    /**
     * Handles queued [InvocationRequest]s by trying in a first step to route them and if necessary asks the [FunctionDeployer]
     * to deploy an instance to execute the [InvocationRequest]
     */
    fun handleRequests(timestamp: Long){
        while (!queue.isEmpty()) {
            val request = queue.peek()
            val delayed = request.originalTick != timestamp
            val funcProfile = usageMonitor.functionProfiles[request.image.uid]!!

            funcProfile.provisionedCPU = request.provisionedCpu
            funcProfile.provisionedMemory = request.provisionedMemory

            while (request.nr_invocations != 0.toLong()) {
                if (routeRequest(request, timestamp)) {
                    request.nr_invocations -= 1
                    funcProfile.cycleInvocations++
                    funcProfile.totalInvocations++
                    usageMonitor.invocations++
                    if (delayed) {
                        usageMonitor.delayedInvocations++
                        funcProfile.delayedInvocations++
                    } else {
                        usageMonitor.timelyInvocations++
                        funcProfile.timelyInvocations++
                    }
                } else if (deployer.deploy(request, timestamp)) {
                    request.nr_invocations -= 1
                    funcProfile.cycleInvocations++
                    funcProfile.totalInvocations++
                    usageMonitor.invocations++
                    if (delayed) {
                        usageMonitor.delayedInvocations++
                        funcProfile.delayedInvocations++
                    }
                } else
                    return
            }
            queue.remove()
        }
    }

}

data class InvocationRequest(val image: FuncImage,
                             var nr_invocations:Long,
                             val originalTick: Long,
                             val execDuration: Long,
                             val provisionedCpu: Int,
                             val provisionedMemory: Int,
                             val requiredCpu: Double,
                             val requiredMemory: Double)

class EnqueueFailException(e:String): Exception(e)