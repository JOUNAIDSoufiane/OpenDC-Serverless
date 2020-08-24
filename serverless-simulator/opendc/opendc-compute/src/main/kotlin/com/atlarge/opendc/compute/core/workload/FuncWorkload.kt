package com.atlarge.opendc.compute.core.workload

import com.atlarge.opendc.compute.core.image.FuncImage
import com.atlarge.opendc.core.User
import com.atlarge.opendc.core.workload.Workload
import java.util.UUID

/**
 * A workload that represents a lambda function.
 *
 * @property uid A unique identified of this function.
 * @property name The name of this function.
 * @property owner The owner of the function.
 * @property image The image of the function.
 */
data class FuncWorkload(
        override val uid: UUID,
        override val name: String,
        override val owner: User,
        val image: FuncImage
) : Workload {
    override fun equals(other: Any?): Boolean = other is FuncWorkload && uid == other.uid

    override fun hashCode(): Int = uid.hashCode()

}
