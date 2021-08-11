package com.exactpro.th2.check1.entities

import com.datastax.driver.core.utils.UUIDs.timeBased
import com.exactpro.th2.check1.SessionKey

data class Checkpoint(
    val id: String = timeBased().toString(), val sessionKeyToCheckpointData: Map<SessionKey, CheckpointData>
) {
    constructor(sessionKeyToCheckpointData: Map<SessionKey, CheckpointData>) : this(
        timeBased().toString(), sessionKeyToCheckpointData
    )
}