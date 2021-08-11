/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.check1.utils

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.entities.CheckpointData
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.Checkpoint.DirectionCheckpoint
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter

fun ProtoToIMessageConverter.fromProtoPreFilter(protoPreMessageFilter: RootMessageFilter): IMessage =
    fromProtoFilter(protoPreMessageFilter.messageFilter, SequenceCheckRuleTask.PRE_FILTER_MESSAGE_NAME)

fun PreFilter.toRootMessageFilter(): RootMessageFilter = RootMessageFilter.newBuilder()
    .setMessageType(SequenceCheckRuleTask.PRE_FILTER_MESSAGE_NAME)
    .setMessageFilter(toMessageFilter())
    .also {
        if (hasMetadataFilter()) {
            it.metadataFilter = metadataFilter
        }
    }
    .build()

fun PreFilter.toMessageFilter(): MessageFilter = MessageFilter.newBuilder()
    .putAllFields(fieldsMap)
    .build()

fun Checkpoint.CheckpointData.convert(): CheckpointData = CheckpointData(sequence, timestamp)

fun CheckpointData.convert(): Checkpoint.CheckpointData {
    val builder = Checkpoint.CheckpointData.newBuilder().setSequence(sequence)
    if (timestamp != null)
        builder.timestamp = timestamp
    return builder.build()
}

fun com.exactpro.th2.check1.entities.Checkpoint.convert(): Checkpoint {
    val intermediateMap: MutableMap<String, DirectionCheckpoint.Builder> = HashMap()
    sessionKeyToCheckpointData.forEach { (sessionKey, checkpointData) ->
        intermediateMap.computeIfAbsent(sessionKey.sessionAlias) {
            DirectionCheckpoint.newBuilder()
        }.putDirectionToCheckpointData(sessionKey.direction.number, checkpointData.convert())
    }

    val checkpointBuilder = Checkpoint.newBuilder().setId(id)
    intermediateMap.forEach { (sessionAlias, directionCheckpoint) -> 
        checkpointBuilder.putSessionAliasToDirectionCheckpoint(sessionAlias, directionCheckpoint.build())
    }

    return checkpointBuilder.build()
}

fun Checkpoint.convert(): com.exactpro.th2.check1.entities.Checkpoint {
    val sessionKeyToSequence: MutableMap<SessionKey, CheckpointData> = HashMap()
    sessionAliasToDirectionCheckpointMap.forEach { (sessionAlias, directionCheckpoint) ->
        check(!(directionCheckpoint.directionToCheckpointDataCount != 0 && directionCheckpoint.directionToSequenceCount != 0)) {
            "Session alias '${sessionAlias}' cannot contain both of these fields: 'direction to checkpoint data' and 'direction to sequence'. Please use 'direction to checkpoint data' instead"
        }
        if (directionCheckpoint.directionToCheckpointDataCount == 0) {
            directionCheckpoint.directionToSequenceMap.forEach { (directionNumber, sequence) -> 
                val sessionKey = SessionKey(sessionAlias, Direction.forNumber(directionNumber))
                sessionKeyToSequence[sessionKey] = CheckpointData(sequence, null)
            }
        } else {
            directionCheckpoint.directionToCheckpointDataMap.forEach { (directionNumber, checkpointData) ->
                val sessionKey = SessionKey(sessionAlias, Direction.forNumber(directionNumber))
                sessionKeyToSequence[sessionKey] = checkpointData.convert()

            }
        }
    }
    return com.exactpro.th2.check1.entities.Checkpoint(id, sessionKeyToSequence)
}