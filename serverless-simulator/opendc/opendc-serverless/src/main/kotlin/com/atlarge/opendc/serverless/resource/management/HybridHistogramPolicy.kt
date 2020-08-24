package com.atlarge.opendc.serverless.resource.management

import com.atlarge.opendc.serverless.core.ArimaForecastModel
import com.atlarge.opendc.serverless.core.RServeThread
import org.rosuda.REngine.Rserve.RserveException
import com.atlarge.opendc.serverless.core.RangeLimitedHistogram
import com.atlarge.opendc.serverless.monitor.UsageMonitor
import java.util.*
import kotlin.IllegalArgumentException
import kotlin.system.exitProcess

/**
 * Resource management policy from the ATC'20 Serverless in the Wild paper [1]
 *
 * Uses a custom lightweight histogram data structure to keep track of function
 * idle times (i.e time between invocations). predicts keep-alive time as the 99th
 * percentile of the histogram frequency distribution and pre-warming time as the 5th
 * percentile.
 *
 * The [limit] parameter sets a threshold for idle times, any idle time larger than
 * this value is counted as out of bounds (OOB). the [outOfBoundsThreshold] is then
 * used to determine whether there are too many OOB values, if above the threshold
 * the policy reverts to using an ARIMA forecast model to predict pre-warming and
 * keep-alive time.
 *
 * The [classWidth] represents the width of a bin in the histogram
 *
 * The [predictionErrorMargin] is used to reduce the pre warming time and increase the keep-alive time predicted by the histogram
 *
 * The [forecastErrorMargin] is used to compute the pre-warming and keep-alive time. When ARIMA predicts an idle time
 * the forecast margin is then substracted from the prediction and multiplied by 2 to obtain the keep-alive time.
 *
 * For each new function invocation, the update method is used to add the function's new idle time onto the distribution.
 *
 * [1] M. Shahrad, R. Fonseca, Í. Goiri, G. Chaudhry, P. Batum, J. Cooke, E. Laureano,
 * C. Tresness, M. Russinovich, and R. Bianchini. Serverless in the wild:
 * Characterizing and optimizing the serverless workload at a large cloud provider.
 * https://www.microsoft.com/en-us/research/uploads/prod/2020/05/serverless-ATC20.pdf
 */
class HybridHistogramPolicy(val funcUids: MutableSet<UUID>,
                            val limit:Long?,
                            val classWidth:Int?,
                            val predictionErrorMargin: Double,
                            val forecastErrorMargin: Double,
                            val outOfBoundsThreshold: Double,
                            val rThreadPort:Int,
                            val usageMonitor: UsageMonitor): ResourceManagementPolicy
{
    override fun invoke(): ResourceManagementPolicy.Logic = object : ResourceManagementPolicy.Logic{
        var forecastEngine: ArimaForecastModel

        init {
            if (limit == null || classWidth == null)
                throw IllegalArgumentException("bad histogram arguments")

            try{
                forecastEngine = ArimaForecastModel(RServeThread(rThreadPort), funcUids, usageMonitor)
            }
            catch(e:RserveException){
                println("R engine error: ${e.message}, please start the Rengine on port $rThreadPort")
                exitProcess(1)
            }

            for (uid in funcUids) {
                usageMonitor.functionProfiles[uid]!!.histogram = RangeLimitedHistogram(limit, classWidth)
            }
        }

        override fun update(funcUid: UUID, timestamp: Long, idleTime: Long) {
            val distribution = usageMonitor.functionProfiles[funcUid]!!.histogram
            val profile = usageMonitor.functionProfiles[funcUid]!!

            distribution.add(idleTime.toDouble())
            forecastEngine.addIdleTime(funcUid, timestamp, idleTime)

            if (distribution.outOfBoundsCount > (distribution.nrObservations * outOfBoundsThreshold) && distribution.nrObservations > 1){
                forecastEngine.buildModel(funcUid)

                val forecast = forecastEngine.forecast(funcUid)
                val keepAliveWindow = (forecast * forecastErrorMargin) * 2
                val preWarmingWindow = forecast - (forecast * forecastErrorMargin)

                profile.timeWindows = Pair(preWarmingWindow.toLong(), keepAliveWindow.toLong())
//                println("[$timestamp] ARIMA forecast for function ${funcUid.leastSignificantBits}: $preWarmingWindow, $keepAliveWindow")
            }
            else {
                val keepAliveWindow = distribution.tail + (distribution.head * predictionErrorMargin)
                val preWarmingWindow = distribution.head - (distribution.head * predictionErrorMargin)

                if (distribution.isRepresentative(2.0))
                    profile.timeWindows= Pair(preWarmingWindow.toLong(), keepAliveWindow.toLong())
                else
                    profile.timeWindows = Pair(preWarmingWindow.toLong(), distribution.limit)
//                println(distribution)
            }
        }

        override fun getTimes(funcUid: UUID): Pair<Long?, Long?> = usageMonitor.functionProfiles[funcUid]!!.timeWindows
    }
}