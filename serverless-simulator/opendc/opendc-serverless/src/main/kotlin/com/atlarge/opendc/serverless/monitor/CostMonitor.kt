package com.atlarge.opendc.serverless.monitor

/*
 * Component that calculates the cost of the simulated function computations
 */
class CostMonitor(private val pricingModel: String,
                  private val usageMonitor: UsageMonitor){

    /*
     * Additions the cost of the provided computation to the function total cost and returns it
     */
    fun updateCost(slice: FunctionProfile){
        slice.cycleCost = enumValueOf<PricingModel>(pricingModel).calculateCost(slice.cycleExecutionTime, slice.provisionedCPU, slice.provisionedMemory, slice.cycleInvocations)
        slice.totalCost += slice.cycleCost
    }
}

enum class PricingModel{
    /*
     * Source: https://aws.amazon.com/lambda/pricing June 2020
     */
    LAMBDA{
        override fun calculateCost(executionTime: Double, provisionedCPU: Int, provisionedMemory: Int, requests:Long):Double{
            var computeSeconds:Double = 0.0
            computeSeconds += (executionTime / 1000.0)
            val computeGbSeconds = computeSeconds * (provisionedMemory.toDouble() / 1024.0)
            val computeCost = computeGbSeconds * 0.00001667
            val requestCost = requests.toDouble() * 0.00000002 // TODO : THIS IS NOT THE ACTUAL COST CALCULATION

            return computeCost + requestCost
        }
    },
    /*
     * Source: https://azure.microsoft.com/en-us/pricing/details/functions/ June 2020
     */
    AZURE{

        override fun calculateCost(executionTime: Double, provisionedCPU: Int, provisionedMemory: Int, requests:Long):Double{
            var computeSeconds:Double = 0.0
            computeSeconds += (executionTime / 1000.0)
            val computeGbSeconds = computeSeconds * (provisionedMemory.toDouble() / 1024.0)
            val computeCost = computeGbSeconds * 0.000016
            val requestCost = requests.toDouble() * 0.00000002 // TODO : THIS IS NOT THE ACTUAL COST CALCULATION

            return computeCost + requestCost
        }
    },
    /*
     * Source: https://cloud.google.com/functions/pricing June 2020
     */
    GOOGLE{
        override fun calculateCost(executionTime: Double, provisionedCPU: Int, provisionedMemory: Int, requests:Long):Double{
            var computeSeconds:Double = 0.0
            computeSeconds += (executionTime / 1000.0)
            val computeGbSeconds:Double = computeSeconds * (provisionedMemory.toDouble() / 1024.0)
            val computeGhzSeconds:Double = computeSeconds * (provisionedCPU.toDouble() / 1000.0)
            val computeGbCost:Double = computeGbSeconds * 0.0000025 // TIER 1 PRICING
            val computeGhzCost:Double = computeGhzSeconds * 0.0000100 // TIER 1 PRICING
            val computeCost:Double = computeGbCost + computeGhzCost
            val requestCost:Double = requests.toDouble() * 0.00000002 // TODO : THIS IS NOT THE ACTUAL COST CALCULATION

            return computeCost + requestCost
        }
    };

    abstract fun calculateCost(executionTime:Double, provisionedCPU: Int, provisionedMemory: Int, requests:Long):Double
}