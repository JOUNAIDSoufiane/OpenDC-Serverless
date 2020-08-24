package com.atlarge.opendc.serverless.monitor

import com.atlarge.opendc.serverless.core.IdleTimeDataFrame
import com.atlarge.opendc.serverless.core.RangeLimitedHistogram
import java.util.*
import kotlin.collections.ArrayList

/*
 * Represents the usage statistics and global variables of a function.
 */
data class FunctionProfile(
        val name: String,
        val uid: UUID
){
    /**
     * R Dataframe object for ARIMA modelling
     */
    lateinit var dataframe: IdleTimeDataFrame
    /**
     *  Histogram data structure for the Hybrid Histogram policy
     */
    lateinit var histogram:RangeLimitedHistogram
    /**
     * Pre warming and keep alive times
     */
    var timeWindows: Pair<Long?, Long?> = Pair(0.toLong(), null)
    /**
     * Amount of time since the invocation was last invoked
     */
    var timeSinceLastInvocation:Long = 0
    /**
     * Cost of computation for the most recent function
     */
    var cycleCost:Double =  0.0
    var totalCost:Double = 0.0
    /**
     * Amount of memory provisioned by the user for this function in MB
     */
    var provisionedMemory:Int = 0
    /**
    * Amount of CPU provisioned by the user for this function in Mhz
    */
    var provisionedCPU:Int = 0
    /**
     * Total cpu usage of all the specific function's instances running during the cycle
     */
    var cpuUsage:Double = 0.0
    /**
     * Total memory usage of all the specific function's instances running during the cycle
     */
    var memoryUsage:Double = 0.0
    /**
     * Total percentage of wasted memory time, i.e time warm instances spent idle
     */
    var wastedMemoryTime: Long = 0
    /**
     * Number of terminated instances
     */
    var terminatedInstances:Long = 0
    /**
     * Number of instances that executed according to their invocation timestamp
     */
    var timelyInvocations = 0
    /**
     * Number of idle instances
     */
    var idleInstances:Long = 0
    /**
     * Number of running instances
     */
    var runningInstances:Long = 0
    /**
     * Number of failed executions
     */
    var failedExecutions:Long = 0
    /**
     * number of invocation requests
     */
    var cycleInvocations:Long = 0
    var totalInvocations:Long = 0
    /**
     * Number of instance deployments
     */
    var cycleColdStarts:Long = 0
    var totalColdStarts: Long = 0
    /**
     * Number of delayed invocations
     */
    var delayedInvocations:Long = 0
    /**
     * sum of execution times of all running instances of this function
     */
    var cycleExecutionTime:Double = 0.0
    var totalExecutionTime:Long = 0
    /*
     * Set of all cold start durations
     */
    val coldStartDurations:ArrayList<Long> = arrayListOf()
    /**
     * Active pre warming windows
     */
    val preWarmingWindows: ArrayList<Long> = arrayListOf()
    /**
     * Travelling average time spent in system (on the fly)
     */
    var avgTimeInSystem: Double = 0.0
    var nTimes: Long = 0
    /**
     * Travelling average Arrival rate (on the fly)
     */
    var avgRequestArrivalRate: Double = 0.0
    var nRates: Long = 0

    fun clear(){
        coldStartDurations.clear()
        cpuUsage = 0.0
        cycleCost = 0.0
        memoryUsage = 0.0
        idleInstances = 0
        terminatedInstances = 0
        runningInstances = 0
        timelyInvocations = 0
        failedExecutions = 0
        cycleInvocations = 0
        cycleExecutionTime = 0.0
        delayedInvocations = 0
        cycleColdStarts = 0
    }
}