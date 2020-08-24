package com.atlarge.opendc.serverless.compute

import com.atlarge.opendc.format.trace.serverless.FuncTraceReader
import com.atlarge.opendc.serverless.compute.queue.FiniteRequestQueue
import com.atlarge.opendc.serverless.experiment.SimulationCore
import com.atlarge.opendc.serverless.experiment.ExperimentConfig
import com.atlarge.opendc.serverless.experiment.Report
import com.atlarge.opendc.serverless.experiment.Scenario
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileReader
import java.time.ZoneId
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.ArrayList
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.Future

/**
 * Integration test suite for the [FunctionRouter].
 */
@DisplayName("StageWorkflowService")
internal class ExecutionTimeSystemTest {

    @Test
    fun `should execute all invocations for their respective durations`(){
        val startingTime:String = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        val rServePort = 1200
        val traceReader = FuncTraceReader(File("/home/soufianej/Documents/Bachelors_project/serverless-simulator/opendc/opendc-serverless/src/test/resources/trace"))
        val experimentDirectory = File("/home/soufianej/Documents/Bachelors_project/serverless-simulator/opendc/opendc-serverless/src/test/resources/experiments")
        val traceSet = traceReader.returnSet()
        val seed = 21240081243423

        var sumExecTime: Long = 0
        for (entry in traceSet){
            for (fragment in entry.workload.image.compHistory)
                for (i in 1..fragment.invocations)
                    sumExecTime += fragment.duration
        }

        val experiments: ArrayList<Scenario> = arrayListOf()

        experimentDirectory.walk().
        filterNot { it.isDirectory }.
        forEach { experimentConfig ->
            val config: ExperimentConfig = jacksonObjectMapper().readValue(FileReader(experimentConfig).readText())
            experiments.add(
                    Scenario(experimentConfig.nameWithoutExtension,
                            "execution-time-system-test",
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

        val executor = Executors.newFixedThreadPool(experiments.size)
        val reports: ArrayList<Future<Report>> = arrayListOf()

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

        for (report in reports) {
            assertEquals(sumExecTime, report.get().sumExecutionTime, "Not all executions lasted according to the trace")
        }
    }
}
