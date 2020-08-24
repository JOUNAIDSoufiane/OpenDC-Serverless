package com.atlarge.opendc.serverless.compute.queue

import com.atlarge.opendc.serverless.compute.InvocationRequest
import java.util.*

/**
 * queue of [InvocationRequest] objects with a specified max size
 */
class FiniteRequestQueue(private val maxsize: Long) :Queue<InvocationRequest>{
    private val queue:Queue<InvocationRequest> = LinkedList()

    override var size:Int = queue.size

    override fun contains(element: InvocationRequest?): Boolean {
        return queue.contains(element)
    }

    override fun containsAll(elements: Collection<InvocationRequest>): Boolean {
        return queue.containsAll(elements)
    }

    override fun isEmpty(): Boolean {
        return queue.isEmpty()
    }

    override fun addAll(elements: Collection<InvocationRequest>): Boolean {
        return if (this.size+elements.size < maxsize && queue.addAll(elements)){
            this.size = queue.size
            true
        }
        else
            false
    }

    override fun clear() {
        queue.clear()
        this.size = queue.size
    }

    override fun element(): InvocationRequest {
        val toReturn = queue.element()
        this.size = queue.size
        return toReturn
    }

    override fun remove(): InvocationRequest {
        val toReturn = queue.remove()
        this.size = queue.size
        return toReturn
    }

    override fun iterator(): MutableIterator<InvocationRequest> {
        return queue.iterator()
    }

    override fun remove(element: InvocationRequest?): Boolean {
        return if (queue.remove(element)){
            this.size = queue.size
            true
        }
        else
            false
    }

    override fun removeAll(elements: Collection<InvocationRequest>): Boolean {
        return if (queue.removeAll(elements)){
            this.size = queue.size
            true
        }
        else
            false
    }

    override fun add(element: InvocationRequest?): Boolean {
        return if (this.size < maxsize && queue.add(element)){
            this.size = queue.size
            true
        }
        else
            false
    }

    override fun offer(p0: InvocationRequest?): Boolean {
        return if (queue.offer(p0)){
            this.size = queue.size
            true
        }
        else
            false
    }

    override fun retainAll(elements: Collection<InvocationRequest>): Boolean {
        return if (queue.retainAll(elements)){
            this.size = queue.size
            true
        }
        else
            false
    }

    override fun peek(): InvocationRequest {
        return queue.peek()
    }

    override fun poll(): InvocationRequest {
        val toReturn = queue.poll()
        this.size = queue.size
        return toReturn
    }
}