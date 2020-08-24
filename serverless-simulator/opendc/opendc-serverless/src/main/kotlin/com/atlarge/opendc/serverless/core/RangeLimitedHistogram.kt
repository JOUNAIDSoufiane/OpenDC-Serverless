package com.atlarge.opendc.serverless.core

import com.atlarge.opendc.serverless.resource.management.HybridHistogramPolicy
import org.apache.commons.math3.stat.Frequency
import kotlin.math.abs
import kotlin.math.ceil
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * Efficient implementation of a compact Histogram structure specifically for the [HybridHistogramPolicy]
 * resource management policy
 */
class RangeLimitedHistogram(val limit: Long,
                            val classWidth: Int) {
    private val distributionFrequency: Frequency = Frequency()

    var head: Long = 0
    var tail: Long = 0
    var mean: Double = 0.0
    var stdev: Double = 0.0
    var variance: Double = 0.0
    var outOfBoundsCount = 0
    var nrObservations: Long = 0
    var variationCoefficient: Double = 0.0


    /**
     * Adds the [observation] to the frequency distribution
     */
    fun add(observation: Double){
        if (observation > limit) {
            outOfBoundsCount++
            return
        }
        nrObservations++
        val upperBoundary:Long = if (observation > classWidth) Math.multiplyExact(ceil(observation / classWidth).toLong(), classWidth.toLong()) else classWidth.toLong()
        distributionFrequency.addValue(upperBoundary)
        updateAttributes(upperBoundary)
    }

    /**
     * Computes the [mean] and [stdev] of the frequency distribution on the Fly and
     * sets the [head] and [tail] to the 5th and 99th percentiles respectively
     */
    private fun updateAttributes(observation: Long){
        val midValue = observation - (classWidth / 2)

        variance += ((nrObservations - 1) / nrObservations) * (midValue - mean).pow(2)
        mean += (midValue - mean) / nrObservations
        stdev = sqrt(variance)
        variationCoefficient = stdev / mean

        head = closestPct(0.05)
        tail = closestPct(0.99)
    }

    private fun closestPct(of: Double): Long {
        var min = Double.MAX_VALUE
        var closest:Long = 0

        for (v in distributionFrequency.valuesIterator()) {
            val diff = abs(distributionFrequency.getCumPct(v) - of)
            if (diff < min) {
                min = diff
                closest = v.toString().toLong()
            }
        }

        return closest
    }

    fun isRepresentative(threshold:Double): Boolean = variationCoefficient <= threshold

    override fun toString(): String = "${distributionFrequency}\n head = $head tail = $tail mean = $mean stdev = $stdev variationCoefficient = $variationCoefficient OOB = $outOfBoundsCount"
}