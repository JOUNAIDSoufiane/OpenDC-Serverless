/*
 * MIT License
 *
 * Copyright (c) 2020 atlarge-research
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.atlarge.opendc.serverless.compute.routing


import com.atlarge.opendc.serverless.compute.FunctionInstance
import com.atlarge.opendc.serverless.compute.InvocationRequest
import kotlin.random.Random

/**
 * A [RoutingPolicy] that selects a random idle instance to which the request can be routed
 */
public class RandomRoutingPolicy(val random: Random = Random(0)) : RoutingPolicy {
    @OptIn(ExperimentalStdlibApi::class)
    override fun invoke(): RoutingPolicy.Logic = object : RoutingPolicy.Logic {
        override fun select(instances: MutableSet<FunctionInstance>, request: InvocationRequest): FunctionInstance? {
            return instances.asIterable()
                .filter { instance ->
                    val isIdle = instance.isIdle()
                    val fitsRequest = instance.uid == request.image.uid
                    isIdle && fitsRequest
                }.randomOrNull(random)
        }
    }
}
