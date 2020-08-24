package com.atlarge.opendc.serverless.core

import com.atlarge.opendc.serverless.monitor.UsageMonitor
import org.rosuda.REngine.REXP
import java.lang.StringBuilder
import java.util.*

/**
 * ARIMA Time series forecast model.
 */
class ArimaForecastModel(private val rThread: RServeThread, funcUids:MutableSet<UUID>, val usageMonitor: UsageMonitor){

    init {
        rThread.engine().eval("library(forecast)")
        rThread.engine().eval("library(xts)")
        for (uid in funcUids) {
            usageMonitor.functionProfiles[uid]!!.dataframe = IdleTimeDataFrame(uid,rThread)
        }
    }

    fun addIdleTime(funcUid:UUID, timestamp:Long, idleTime:Long) = usageMonitor.functionProfiles[funcUid]!!.dataframe.addIdleTime(timestamp, idleTime)

    fun buildModel(funcUid: UUID){
        val dataFrame = usageMonitor.functionProfiles[funcUid]!!.dataframe
        dataFrame.build()
        rThread.engine().eval("model${dataFrame.dfName} = auto.arima(${dataFrame.toTimeSeries()}, D=1)")
    }

    fun forecast(funcUid: UUID):Long{
        val dataFrame = usageMonitor.functionProfiles[funcUid]!!.dataframe
        val forecastValue = rThread.engine().eval("as.numeric(forecast(model${dataFrame.dfName}, 1)\$mean)")
        return forecastValue.asDouble().toLong()
    }
}

class IdleTimeDataFrame(private val uid:UUID, private val rThread: RServeThread){
    val dfName = "dataframe${uid.leastSignificantBits}"

    private val timestampCol: StringBuilder = StringBuilder()
    private val idleTimeCol: StringBuilder = StringBuilder()
    private var firstInsert: Boolean = true

    init {
        rThread.engine().eval("$dfName <- data.frame(timestamp=c(), idleTime=c())")
        timestampCol.append("c()")
        idleTimeCol.append("c()")
    }
    
    fun addIdleTime(timestamp: Long, idleTime: Long){
        if (firstInsert) {
            timestampCol.insert(timestampCol.indexOf(')'), "$timestamp")
            idleTimeCol.insert(idleTimeCol.indexOf(')'), "$idleTime")
            firstInsert = false
        }
        else{
            timestampCol.insert(timestampCol.indexOf(')'), ",$timestamp")
            idleTimeCol.insert(idleTimeCol.indexOf(')'), ",$idleTime")
        }
    }

    fun build(): REXP = rThread.engine().eval("$dfName <- data.frame(timestamp=c(as.POSIXct($timestampCol/1000, origin=\"1970-01-01\", tz=\"UTC\")), idleTime=c($idleTimeCol))")

    fun toTimeSeries():String{
        rThread.engine().eval("ts_$dfName <- as.xts(x = $dfName[, -1], order.by = $dfName\$timestamp)")
        return "ts_$dfName"
    }
}


