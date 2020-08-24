package com.atlarge.opendc.serverless.compute

import com.atlarge.opendc.serverless.resource.InstanceHypervisor
import java.util.UUID

/**
 * Represents a Low overhead small container to run invocations on (1 invocation max as per the current design )
 *
 * @param uid unique id of the instance
 * @param name name of the deployed function
 * @param cpuUsage average cpu usage while executing
 * @param memoryUsage average memory usage while executing
 */
class FunctionInstance(
        val uid: UUID,
        val name:String,
        var cpuUsage: Double,
        var memoryUsage: Double,
        private var cycleInterval:Long,
        val hypervisor: InstanceHypervisor
) {
    private var wasRunning:Boolean = true
    private var running:Boolean = false
    private var sleeping:Boolean = false
    private var wakeUpTime:Long = 0
    var endTime:Long = 0
    var startTime:Long = 0
    var idleTime:Long = 0

    fun isIdle():Boolean{
        return !running && !sleeping
    }

    fun isRunning():Boolean{
        return running
    }

    /**
     * Returns if a [FunctionInstance] stopped running during the current cycle
     */
    fun wasRunning():Boolean{
        return wasRunning
    }

    fun sleepUntil(delay: Long){
        sleeping = true
        wakeUpTime = delay
    }

    fun halt(currentTime: Long){
        running = false
        idleTime = currentTime - endTime
    }

    /**
     * Sets the [FunctionInstance] to run until [endTick] using ([cpu] Mhz,[mem] mb)
     */
    fun invoke(startTick:Long ,endTick:Long, cpu: Double, mem: Double) {
        cpuUsage = cpu
        memoryUsage = mem
        startTime = startTick
        endTime =  endTick
        running = sleeping == false
    }

    /**
     * Updates the [FunctionInstance]'s state based on the currentTime, also tracks idle state duration
     */
    fun update(currentTime:Long){
        wasRunning = false

        if (running && (currentTime >= endTime)) {
            running = false
            wasRunning = true
            idleTime = currentTime - endTime
        }
        else if (sleeping && (currentTime <= wakeUpTime && (currentTime + cycleInterval) >= wakeUpTime)){
            running = true
            sleeping = false
            wakeUpTime = 0
        }
        else
            idleTime = currentTime - endTime
    }


    override fun toString():String = "Function Instance(uid=$uid, name=$name, cpuUsage=$cpuUsage Mhz, memoryUsage=$memoryUsage MB), running = $running, wasrunning=$wasRunning"
}