package com.atlarge.opendc.serverless.experiment

import java.util.*
import java.util.regex.Pattern
import java.util.stream.IntStream.range
import kotlin.IllegalArgumentException
import kotlin.system.exitProcess
import com.atlarge.opendc.format.trace.TraceEntry
import com.atlarge.opendc.serverless.compute.*
import com.atlarge.opendc.serverless.monitor.*
import com.atlarge.opendc.serverless.resource.*
import com.atlarge.opendc.compute.core.workload.FuncWorkload
import com.atlarge.opendc.serverless.compute.routing.*
import com.atlarge.opendc.serverless.resource.allocation.*
import com.atlarge.opendc.serverless.resource.management.FixedKeepAlivePolicy
import com.atlarge.opendc.serverless.resource.management.HybridHistogramPolicy
import com.atlarge.opendc.serverless.resource.management.NoTerminationPolicy
import me.tongfei.progressbar.ProgressBar
import kotlin.random.Random

/**
 * The simulation core initializes all the different FaaS services, feeds invocations to the routing component and
 * time updates to the resource management component.
 */
class SimulationCore(
        private val id:String,
        private val startingTime:String,
        private val experimentName:String,
        private val trace:Set<TraceEntry<FuncWorkload>>,
        private val vmSetup:MutableSet<VirtualMachine>,
        private val pricingModel:String,
        private val failureModel:String,
        private val requestQueue: Queue<InvocationRequest>,
        private val allocationPolicy:String,
        private val routingPolicy:String,
        private val managementPolicy: String,
        private val vmCount:Int,
        private val idleInstanceTimeout:Long?,
        private val histogramLimit:Long?,
        private val histogramClassWidth:Int?,
        private val histogramOutOfBoundsThreshold: Double,
        private val histogramErrorMargin: Double,
        private val forecastErrorMargin: Double,
        private val idleMemoryPenalty:Double,
        private val rThreadPort: Int,
        private val seed:Long,
        private val verbose:Boolean)
{
    private var nrEntries = 0
    private var cycleInterval:Long = 0
    private val funcProfiles:MutableSet<Pair<String,UUID>> = mutableSetOf()
    private val funcUids:MutableSet<UUID> = mutableSetOf()

    private var costMonitor:CostMonitor
    private var usageMonitor:UsageMonitor
    private var resourceManager:ResourceManager
    private var resourceScheduler:ResourceScheduler
    private var funcDeployer:FunctionDeployer
    private var funcRouter:FunctionRouter

    init {
        if (verbose) {
            println("Experiment : $experimentName")
            println("Pricing-model: $pricingModel pricing")
            println("Failure model: ${failureModel})")
            println("VM Count: $vmCount")
            println("Idle instance timeout: $idleInstanceTimeout ms")
            println("Seed: $seed")
        }

        for (entry in trace){
            funcProfiles.add(Pair(entry.workload.image.name, entry.workload.image.uid))
            funcUids.add( entry.workload.image.uid)
            nrEntries = entry.workload.image.compHistory.toSet().size
            cycleInterval = entry.workload.image.compHistory.toSet().elementAt(2).tick - entry.workload.image.compHistory.toSet().elementAt(1).tick
        }

        /*
         * Initializing Delay injector
         */
        val delayInjector: DelayInjector
        try {
            if (idleMemoryPenalty > 1.0)
                throw IllegalArgumentException("Idle memory percentage superior to 100%")

            delayInjector = if (failureModel.startsWith("CUSTOM")) {
                val parameters = failureModel.substring(6)
                val pattern = Pattern.compile("([0-9]+\\.[0-9]+|[0-9]+)|([0-9]+\\.[0-9]+|[0-9]+)\\)")
                val matcher = pattern.matcher(parameters)

                val meanCold = if (matcher.find()) matcher.group(0).toDouble() else throw IllegalArgumentException("Invalid faultInjector parameters")
                val sdCold = if (matcher.find()) matcher.group(0).toDouble() else throw IllegalArgumentException("Invalid faultInjector parameters")
                val meanLookup = if (matcher.find()) matcher.group(0).toDouble() else throw IllegalArgumentException("Invalid faultInjector parameters")
                val sdLookup = if (matcher.find()) matcher.group(0).toDouble() else throw IllegalArgumentException("Invalid faultInjector parameters")

                DelayInjector(seed, meanCold, sdCold, meanLookup, sdLookup)
            } else {
                DelayInjector(failureModel, seed)
            }
        } catch (e:IllegalArgumentException){
            println("Invalid input : ${e.message}")
            exitProcess(1)
        }
        /*
         * Initializing FaaS monitoring services
         */
        val destination:String = if (managementPolicy == "fixed-keep-alive"){
            "data/$experimentName/${startingTime}/${allocationPolicy}Alloc-${routingPolicy}Routing-${managementPolicy}ResourceManagement-${idleInstanceTimeout}msTimeout-${vmCount}VM's.parquet"
        }
        else{
            "data/$experimentName/${startingTime}/${allocationPolicy}Alloc-${routingPolicy}Routing-${managementPolicy}ResourceManagement-${histogramLimit}msLimit-${vmCount}VM's.parquet"
        }
        usageMonitor = UsageMonitor(destination, funcProfiles)
        costMonitor = CostMonitor(pricingModel, usageMonitor)
        usageMonitor.costMonitor = costMonitor

        /**
         * Initializing policies
         */

        val allocationPolicies = mapOf(
                "sequential" to SequentialAllocationPolicy(),
                "random" to RandomAllocationPolicy(Random(seed))
        )

        val routingPolicies = mapOf(
                "sequential" to SequentialRoutingPolicy(),
                "least-idletime" to LeastIdleTimeRoutingPolicy(),
                "highest-idletime" to HighestIdleTimeRoutingPolicy(),
                "random" to RandomRoutingPolicy(Random(seed))
        )

        val managementPolicies = mapOf(
                "fixed-keep-alive" to FixedKeepAlivePolicy(funcUids,
                        idleInstanceTimeout,usageMonitor),
                "no-termination" to NoTerminationPolicy(usageMonitor),
                "hybrid-histogram" to HybridHistogramPolicy(
                        funcUids,
                        histogramLimit,
                        histogramClassWidth,
                        histogramErrorMargin,
                        forecastErrorMargin,
                        histogramOutOfBoundsThreshold,
                        rThreadPort,
                        usageMonitor)
        )

        /*
         * Initializing resource manager and scheduler
         */
        resourceManager = ResourceManager("setup", vmSetup.toSortedSet(compareBy{it.id}), usageMonitor, verbose)
        try {
            resourceScheduler = ResourceScheduler(resourceManager, usageMonitor, allocationPolicies.getValue(allocationPolicy), managementPolicies.getValue(managementPolicy), cycleInterval)
        }catch (e:IllegalArgumentException){
            println("Invalid input : ${e.message}")
            exitProcess(1)
        }
        resourceManager.initHypervisors(idleMemoryPenalty, cycleInterval, resourceScheduler)
        /*
         * Initializing deployment and routing services
         */
        funcDeployer = FunctionDeployer(delayInjector, cycleInterval, verbose, usageMonitor, resourceScheduler, resourceManager)
        funcRouter = FunctionRouter(requestQueue, verbose, funcDeployer, usageMonitor, resourceManager, routingPolicies.getValue(routingPolicy))
        resourceScheduler.functionDeployer = funcDeployer
    }

    /**
     * Starts the simulation and returns a [Report] object containing runtime statistics
     */
    fun run():Report{
        var currentTime:Long = 0
        var runningInstances:Long = 0

        val pb = ProgressBar(id, nrEntries.toLong())
        pb.extraMessage = "Running..."

        for (line in range(0,nrEntries)){
            resourceManager.monitoringCycle(currentTime + cycleInterval)
            resourceManager.computeInstanceViews()
            pb.step()

            if (verbose)
                println()

            for (entry in trace) {
                val historyFragment = entry.workload.image.compHistory.elementAt(line.toInt()) // TODO : unsafe long to int conversion
                val profile = usageMonitor.functionProfiles[entry.workload.image.uid]!!
                currentTime = historyFragment.tick

                // Updating arrival rates and times requests spend in the system
                profile.nRates++
                profile.nTimes++
                profile.avgRequestArrivalRate = profile.avgRequestArrivalRate + ((historyFragment.invocations - profile.avgRequestArrivalRate) / profile.nRates)
                profile.avgTimeInSystem = profile.avgTimeInSystem + ((historyFragment.duration - profile.avgTimeInSystem) / profile.nTimes)

                // Time between invocations tracking
                if (historyFragment.invocations > 0) {
                    resourceScheduler.updateManagementPolicy(entry.workload.image.uid, currentTime, usageMonitor.functionProfiles[entry.workload.image.uid]!!.timeSinceLastInvocation)
                    profile.timeSinceLastInvocation = cycleInterval
                } else {
                    profile.timeSinceLastInvocation += cycleInterval
                    continue
                }

                try {
                    funcRouter.enqueueRequest(
                            InvocationRequest(entry.workload.image,
                                    historyFragment.invocations.toLong(),
                                    currentTime,
                                    historyFragment.duration,
                                    historyFragment.provisionedCpu,
                                    historyFragment.provisionedMem,
                                    historyFragment.cpu,
                                    historyFragment.mem)
                    )
                }
                catch (e:EnqueueFailException) {
                    println("SimulationError: ${e.message}")
                    exitProcess(1)
                }
            }

            if (funcRouter.getNrQueuedRequests() != 0)
                funcRouter.handleRequests(currentTime)

            resourceScheduler.preWarm(currentTime)
            runningInstances = resourceManager.monitoringCycle(currentTime)
            resourceManager.profilingCycle(currentTime)
            usageMonitor.onCycleFinish(currentTime)

            if (verbose) {
                val utilization:Int = (resourceManager.functionInstances().filter { it.isRunning() }.size.toDouble() / resourceManager.functionInstances().size.toDouble() * 100).toInt()
                println("Instance resource utilization: $utilization%")
            }
        }

        while (runningInstances != 0.toLong()){
            currentTime += cycleInterval
            runningInstances = resourceManager.monitoringCycle(currentTime)
            resourceManager.computeInstanceViews()
            nrEntries += 1
            pb.maxHint(nrEntries.toLong())
            pb.step()

            if (funcRouter.getNrQueuedRequests() != 0)
                funcRouter.handleRequests(currentTime)

            resourceScheduler.preWarm(currentTime)

            resourceManager.profilingCycle(currentTime)
            usageMonitor.onCycleFinish(currentTime)
        }
        pb.stop()

        val experimentParameters:String = if (managementPolicy == "hybrid-histogram") {
            "${resourceManager.vmCount()} VM's, pricing model: $pricingModel, delayInjector: $failureModel, " +
                    "ResourceManagement: $managementPolicy (Limit: $histogramLimit, bin width:$histogramClassWidth OOBThreshold: $histogramOutOfBoundsThreshold PredictionErrMargin: $histogramErrorMargin ForecastErrMargin: $forecastErrorMargin)" +
                    ", Alloc: ${allocationPolicy}, Routing: ${routingPolicy}, seed = $seed"
        }
        else
            "${resourceManager.vmCount()} VM's, pricing model: $pricingModel, delayInjector: $failureModel, ResourceManagement: $managementPolicy ${idleInstanceTimeout}ms, Alloc: ${allocationPolicy}, Routing: ${routingPolicy}, seed = $seed"

        val report = Report(experimentName,
                experimentParameters,
                currentTime,
                usageMonitor.sumExecutionTime,
                usageMonitor.invocations,
                usageMonitor.delayedInvocations,
                usageMonitor.coldStarts,
                usageMonitor.timelyInvocations,
                usageMonitor.failedExecutions,
                usageMonitor.terminatedInstances,
                usageMonitor.functionProfiles.values.sumByDouble { it.totalCost }
        )

        usageMonitor.terminate()
        resourceManager.terminate()

        return report
    }
}

data class Report(
        val experimentName: String,
        val experimentParameters: String,
        val simDuration: Long,
        val sumExecutionTime:Long,
        val invocations:Long,
        val delayedInvocations:Long,
        val coldStarts:Long,
        val onTimeExecutions:Long,
        val failedExecutions:Long,
        val terminations:Long,
        val totalCost:Double
){
    fun print(){
        println("Experiment name : $experimentName")
        println("Experiment parameters : $experimentParameters")
        println("Total execution duration sum = $sumExecutionTime ms")
        println("Simulation duration = $simDuration ms")
        println("Total invocations = $invocations")
        println("Delayed invocations = $delayedInvocations")
        println("Cold starts = $coldStarts")
        println("OnTime instances = $onTimeExecutions")
        println("Failed executions = $failedExecutions")
        println("Instance terminations = $terminations")
        println("Total cost = $totalCost USD")
    }
}