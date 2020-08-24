package com.atlarge.opendc.serverless.core

import org.rosuda.REngine.Rserve.RConnection

/**
 * Initializes an RServe thread that can execute R code remotely.
 */
class RServeThread(private val port: Int){
    private val connection = RConnection("localhost", port)

    fun engine(): RConnection = connection

    override fun toString(): String = "R thread on port $port"
}