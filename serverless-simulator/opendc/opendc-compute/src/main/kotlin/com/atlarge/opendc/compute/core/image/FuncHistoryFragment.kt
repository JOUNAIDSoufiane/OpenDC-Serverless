package com.atlarge.opendc.compute.core.image

data class FuncHistoryFragment(val tick: Long, val invocations:Int, val provisionedCpu:Int, val provisionedMem:Int, val cpu: Double, val mem: Double, val duration: Long){
    override fun toString(): String = "Timestamp=$tick (CPU usage=$cpu, Memory usage=$mem, Execution time=$duration)"
}
