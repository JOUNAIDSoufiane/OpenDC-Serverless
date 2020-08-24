package com.atlarge.opendc.serverless.experiment

import com.atlarge.opendc.compute.core.workload.FuncWorkload
import com.atlarge.opendc.format.trace.TraceEntry
import com.atlarge.opendc.format.trace.serverless.FuncTraceReader
import com.atlarge.opendc.serverless.compute.InvocationRequest
import com.atlarge.opendc.serverless.compute.queue.FiniteRequestQueue
import com.atlarge.opendc.serverless.resource.VirtualMachine
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import java.io.File
import java.io.FileReader
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Experiment framework, allows for parallel execution of experiments.
 *
 * To use this framework, supply the 3 following elements in order the arguments
 *  -Path Directory of trace files
 *  -Path Directory of experiment configurations
 *  -Seed
 */
data class ExperimentConfig(
        val vmSetup:MutableSet<VirtualMachine>,
        val pricingModel:String,
        val failureModel:String,
        val requestQueueSize:Long,
        val allocationPolicy:String,
        val routingPolicy:String,
        val managementPolicy: String,
        val instanceIdleTimeout:Long?,
        val histogramLimit:Long?,
        val histogramClassWidth:Int?,
        val histogramOOBThreshold: Double,
        val histogramErrorMargin: Double,
        val forecastErrorMargin: Double,
        val idleMemPenalty:Double,
        val verbose:Boolean)

data class Scenario(
        val id:String,
        val experimentName:String,
        val trace:Set<TraceEntry<FuncWorkload>>,
        val vmSetup:MutableSet<VirtualMachine>,
        val pricingModel:String,
        val failureModel:String,
        val requestQueue: Queue<InvocationRequest>,
        val allocationPolicy:String,
        val routingPolicy:String,
        val managementPolicy: String,
        val vmCount:Int,
        val instanceIdleTimeout:Long?,
        val histogramLimit: Long?,
        val histogramClassWidth: Int?,
        val histogramOOBThreshold: Double,
        val histogramErrorMargin: Double,
        val forecastErrorMargin: Double,
        val idleMemPenalty:Double,
        val rServeThreadPort: Int,
        val verbose:Boolean)

fun main(args : Array<String>){
    val startingTime:String = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
    val tracePath = args[0]
    val experimentPath = args[1]
    val rServePort = 1200
    val traceReader = FuncTraceReader(File(tracePath))
    val experimentDirectory = File(experimentPath)
    val traceSet = traceReader.returnSet()
    val seed:Long = args[2].toLong()

    val experiments:ArrayList<Scenario> = arrayListOf()

    experimentDirectory.walk().
    filterNot { it.isDirectory }.
    forEach { experimentConfig ->
        val config:ExperimentConfig = jacksonObjectMapper().readValue(FileReader(experimentConfig).readText())
        experiments.add(
            Scenario(experimentConfig.nameWithoutExtension,
                    "${experimentDirectory.nameWithoutExtension}-$tracePath",
                    traceSet,
                    config.vmSetup,
                    config.pricingModel,
                    config.failureModel,
                    FiniteRequestQueue(config.requestQueueSize),
                    config.allocationPolicy,
                    config.routingPolicy,
                    config.managementPolicy,
                    config.vmSetup.size,
                    config.instanceIdleTimeout,
                    config.histogramLimit,
                    config.histogramClassWidth,
                    config.histogramOOBThreshold,
                    config.histogramErrorMargin,
                    config.forecastErrorMargin,
                    config.idleMemPenalty,
                    rServePort,
                    config.verbose
            )
        )
    }
    experiments.sortBy { it.id }

    println("Test name: $experimentPath, ${experiments.size} concurrent experiments")
    println("Test start time: $startingTime")
    println("seed: $seed")

    val executor = Executors.newFixedThreadPool(experiments.size)
    val reports:ArrayList<Future<Report>> = arrayListOf()

    for (exp in experiments) {
        val worker = Callable { SimulationCore(
                exp.id,
                startingTime,
                exp.experimentName,
                exp.trace,
                exp.vmSetup,
                exp.pricingModel,
                exp.failureModel,
                exp.requestQueue,
                exp.allocationPolicy,
                exp.routingPolicy,
                exp.managementPolicy,
                exp.vmCount,
                exp.instanceIdleTimeout,
                exp.histogramLimit,
                exp.histogramClassWidth,
                exp.histogramOOBThreshold,
                exp.histogramErrorMargin,
                exp.forecastErrorMargin,
                exp.idleMemPenalty,
                exp.rServeThreadPort,
                seed,
                exp.verbose).run()
        }
        reports.add(executor.submit(worker))
    }
    executor.shutdown()
    while(!executor.isTerminated){}

    println("Finished running all experiments, Displaying results")
    println()

    for (report in reports) {
        report.get().print()
        println()
    }
}