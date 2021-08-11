package com.exactpro.th2.check1.entities

import com.google.protobuf.Timestamp

data class CheckpointData(val sequence: Long, val timestamp: Timestamp? = null)