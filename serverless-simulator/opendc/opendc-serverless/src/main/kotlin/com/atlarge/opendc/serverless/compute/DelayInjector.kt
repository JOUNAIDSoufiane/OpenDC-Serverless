package com.atlarge.opendc.serverless.compute

import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.random.RandomGenerator
import kotlin.math.sqrt

/*
 * Interface for instance deployment delay estimation
 */
class DelayInjector {
    private var platform:String = ""
    private var seeder:RandomGenerator = JDKRandomGenerator()
    private var custom:Boolean = false
    /*
     * Delay generators (Gaussian distributions)
     */
    private var coldStartDelayGen = NormalDistribution()
    private var lookupDelayGen = NormalDistribution()

    /*
     * Default primary constructor to assign the failure model of an existing FaaS platform
     */
    constructor(platform:String, seed:Long) {
        this.seeder.setSeed(seed)

        if (platform == "CUSTOM") {
            this.custom = true
            return
        }
        else if (platform != "LAMBDA" &&
                 platform != "AZURE" &&
                 platform != "GOOGLE" )
            throw IllegalArgumentException("Invalid Failure model : $platform")

        this.platform = platform
    }

    /*
     * Secondary constructor to allow specification of custom distribution parameters
     */
    constructor(seed:Long,
                meanColdStart:Double,
                sdColdStart:Double,
                meanLookup:Double,
                sdLookup:Double): this("CUSTOM", seed) {
        coldStartDelayGen = NormalDistribution(seeder, meanColdStart, sdColdStart)
        lookupDelayGen = NormalDistribution(seeder, meanLookup, sdColdStart)
    }

    private fun positiveDouble(number:Double):Double{
        return sqrt(number*number)
    }


    /**
     * Returns the cold start delay duration sampled from a normal distribution, the distribution is
     * initialized using custom mean and standard deviation based on provisioned memory, language and
     * failure model
     * TODO : support more languages
     */
    fun getColdStartDelay(provisionedMemory: Int, language: String = "Nodejs6"):Long{
        return if (custom)
            positiveDouble(coldStartDelayGen.sample()).toLong()
        else{
            val parameters = enumValueOf<FailureModel>(platform).coldStartParam(provisionedMemory, language)
            positiveDouble(NormalDistribution(seeder, parameters.first, parameters.second).sample()).toLong()
        }
    }

    /*
     * TODO : Update this, see getColdStartDelay()
     */
    fun getLookupDelay():Long{
        return positiveDouble(lookupDelayGen.sample()).toLong()
    }
}

enum class FailureModel(){
    // min and max memory values from [Peeking Behind The Curtains of Serverless Platforms][2018], other values deduced from linear curve
    LAMBDA{
        override fun coldStartParam(provisionedMemory: Int, language:String): Pair<Double, Double> {
            return when (provisionedMemory){
                128 -> Pair(265.21,354.43)
                256 -> Pair(261.46,334.23)
                512 -> Pair(257.71,314.03)
                1024 -> Pair(253.96,293.83)
                1536 -> Pair(250.07,273.63)
                2048 -> Pair(246.11,253.43)
                else -> Pair(0.0,1.0)
            }
        }
    },
    AZURE{
        // Azure by default uses 1.5gb memory to instantiate functions
        override fun coldStartParam(provisionedMemory: Int, language:String): Pair<Double, Double> {
            return Pair(242.66,340.67)
        }
    },
    GOOGLE{
        override fun coldStartParam(provisionedMemory: Int, language:String): Pair<Double, Double> {
            return when (provisionedMemory){
                128 -> Pair(493.04,345.8)
                256 -> Pair(416.59,301.5)
                512 -> Pair(340.14,257.2)
                1024 -> Pair(263.69,212.9)
                1536 -> Pair(187.24,168.6)
                2048 -> Pair(110.77,124.3)
                else -> Pair(0.0,1.0)
            }
        }
    };

    abstract fun coldStartParam(provisionedMemory:Int, language:String):Pair<Double,Double>
}