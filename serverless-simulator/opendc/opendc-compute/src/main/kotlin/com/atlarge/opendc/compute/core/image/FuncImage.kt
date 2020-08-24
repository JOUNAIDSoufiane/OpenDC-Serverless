package com.atlarge.opendc.compute.core.image

import com.atlarge.opendc.compute.core.execution.ServerContext
import com.atlarge.opendc.core.resource.TagContainer
import java.util.UUID

class FuncImage(
        public override val uid: UUID,
        public override val name: String,
        public override val tags: TagContainer,
        public var compHistory: Sequence<FuncHistoryFragment>
) : Image {

    override suspend fun invoke(ctx: ServerContext) {}

    override fun toString(): String = "FuncImage(uid=$uid, name=$name)"
}
