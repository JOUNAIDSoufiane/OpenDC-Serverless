package com.atlarge.opendc.serverless.monitor

import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import java.util.*
import java.util.concurrent.ArrayBlockingQueue
import kotlin.collections.ArrayList
import kotlin.collections.HashMap
import kotlin.concurrent.thread

/**
 * Component responsible for monitoring individual function usage
 */
class UsageMonitor(destination: String,
                   functions:MutableSet<Pair<String,UUID>>)
{
    val functionProfiles:HashMap<UUID, FunctionProfile> = HashMap()
    lateinit var costMonitor: CostMonitor

    var failedExecutions:Long = 0
    var terminatedInstances:Long = 0
    var delayedInvocations:Long = 0
    var coldStarts:Long = 0
    var invocations:Long = 0
    var timelyInvocations:Long = 0
    var sumExecutionTime: Long = 0

    init {
        for (pair in functions)
            functionProfiles[pair.second] = FunctionProfile(pair.first, pair.second)
    }

    private val schema = SchemaBuilder
            .record("slice")
            .namespace("com.atlarge.opendc.serverless")
            .fields()
            .name("Time").type().longType().noDefault()
            .name("Function").type().stringType().noDefault()
            .name("Invocations").type().longType().noDefault()
            /*
             * TODO: Fix this attribute's ambiguity
             */
            .name("DelayedInvocations").type().longType().noDefault()
            .name("TimelyInvocations").type().longType().noDefault()
            .name("TotalInvocations").type().longType().noDefault()
            .name("ColdStarts").type().longType().noDefault()
            .name("ColdStartsPct").type().doubleType().noDefault()
            .name("TotalColdStarts").type().longType().noDefault()
            .name("MedianColdStartDelay").type().longType().noDefault()
            .name("RunningInstances").type().longType().noDefault()
            .name("FailedExecutions").type().longType().noDefault()
            .name("IdleInstances").type().longType().noDefault()
            .name("TerminatedInstances").type().longType().noDefault()
            .name("ProvisionedCPU").type().intType().noDefault()
            .name("ProvisionedMemory").type().intType().noDefault()
            .name("CpuUsage").type().doubleType().noDefault()
            .name("MemoryUsage").type().doubleType().noDefault()
            .name("WastedMemoryTime").type().doubleType().noDefault()
            .name("TotalCost").type().doubleType().noDefault()
            .endRecord()

    private val queue = ArrayBlockingQueue<GenericData.Record>(2048)
    private val writer = AvroParquetWriter.builder<GenericData.Record>(Path(destination))
            .withSchema(schema)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withPageSize(4 * 1024 * 1024)
            .withRowGroupSize(16 * 1024 * 1024)
            .build()
    private val writerThread = thread(start = true, name = "serverless-writer") {
        try {
            while (true) {
                val record = queue.take()
                writer.write(record)
            }
        } catch (e: InterruptedException) {
            // Do not rethrow this
        } finally {
            writer.close()
        }
    }

    /**
     * Records every function's [FunctionProfile] onto a parquet file and then clears them for the next usage
     */
    fun onCycleFinish(time:Long) {
        for (profile in functionProfiles.values.toMutableSet().toSortedSet(compareBy{it.uid})) {
            val record = GenericData.Record(schema)

            costMonitor.updateCost(profile)

            record.put("Time", time)
            record.put("Function", profile.name)
            record.put("Invocations", profile.cycleInvocations)
            record.put("ColdStarts", profile.cycleColdStarts)
            record.put("DelayedInvocations", profile.delayedInvocations)
            record.put("TimelyInvocations", profile.timelyInvocations)
            record.put("RunningInstances", profile.runningInstances)
            record.put("FailedExecutions", profile.failedExecutions)
            record.put("IdleInstances", profile.idleInstances)
            record.put("TerminatedInstances", profile.terminatedInstances)
            record.put("ProvisionedCPU", profile.provisionedCPU)
            record.put("ProvisionedMemory", profile.provisionedMemory)
            record.put("CpuUsage", profile.cpuUsage)
            record.put("MemoryUsage", profile.memoryUsage)
            record.put("TotalCost", profile.cycleCost)
            record.put("MedianColdStartDelay", median(profile.coldStartDurations))
            record.put("TotalColdStarts", profile.totalColdStarts)
            record.put("TotalInvocations", profile.totalInvocations)

            if (profile.totalInvocations != 0.toLong())
                record.put("ColdStartsPct", profile.totalColdStarts * 100 / profile.totalInvocations)
            else
                record.put("ColdStartsPct", 0)


            record.put("WastedMemoryTime", profile.wastedMemoryTime)

            queue.put(record)
            profile.clear()
        }
    }

     fun terminate() {
         failedExecutions = 0
         terminatedInstances = 0
         delayedInvocations = 0
         coldStarts = 0
         invocations = 0
         timelyInvocations = 0
        while (queue.isNotEmpty()) {
            Thread.sleep(500)
        }
        writerThread.interrupt()
    }
}

fun median(m: ArrayList<Long>): Long {
    if (m.isEmpty())
        return 0
    val middle = m.size / 2
    return if (m.size % 2 == 1) {
        m[middle]
    } else {
        (m[middle - 1] + m[middle]) / 2
    }
}