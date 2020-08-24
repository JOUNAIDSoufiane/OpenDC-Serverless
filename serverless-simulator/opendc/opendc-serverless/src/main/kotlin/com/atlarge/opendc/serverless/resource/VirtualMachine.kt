package com.atlarge.opendc.serverless.resource

import com.atlarge.opendc.compute.core.Flavor
import java.util.*

/**
 * Basic representation of a virtual machine.
 */
data class VirtualMachine(val id:Int, private val resources: Resources){
    val hypervisor = InstanceHypervisor(UUID.randomUUID(), "Instance Hypervisor $id", "VM$id", resources.cpuCapacity, resources.memoryCapacity)
}

data class Resources(private val hardware: Flavor, private val coreClockSpeed:Int){
    val cpuCapacity:Double = hardware.cpuCount * coreClockSpeed.toDouble()
    val memoryCapacity:Double = hardware.memorySize.toDouble()
}