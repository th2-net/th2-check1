/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.check1.utils

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.Checkpoint.CheckpointData
import com.exactpro.th2.common.grpc.Checkpoint.DirectionCheckpoint
import com.exactpro.th2.common.grpc.Checkpoint.SessionAliasToDirectionCheckpoint
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import mu.KotlinLogging
import com.exactpro.th2.check1.entities.Checkpoint as InternalCheckpoint
import com.exactpro.th2.check1.entities.CheckpointData as InternalCheckpointData

val LOGGER = KotlinLogging.logger {}

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

fun CheckpointData.convert(): InternalCheckpointData = InternalCheckpointData(sequence, timestamp)

fun InternalCheckpointData.convert(): CheckpointData {
    val builder = CheckpointData.newBuilder().setSequence(sequence)
    if (timestamp != null)
        builder.timestamp = timestamp
    return builder.build()
}

fun InternalCheckpoint.convert(): Checkpoint {
    val intermediateMap: MutableMap<String, MutableMap<String, DirectionCheckpoint.Builder>> = HashMap()
    sessionKeyToCheckpointData.forEach { (sessionKey, checkpointData) ->
        intermediateMap
            .computeIfAbsent(sessionKey.bookName) { HashMap() }
            .computeIfAbsent(sessionKey.sessionAlias) { DirectionCheckpoint.newBuilder() }
            .apply {
                sessionKey.direction.number.run {
                    putDirectionToCheckpointData(this, checkpointData.convert())
                }
            }
    }

    val checkpointBuilder = Checkpoint.newBuilder().setId(id)
    intermediateMap.forEach { (bookName, aliasToDirectionCheckpoint) ->
        val builder = SessionAliasToDirectionCheckpoint.newBuilder()
        aliasToDirectionCheckpoint.forEach { (alias, directionCheckpointBuilder) ->
            builder.putSessionAliasToDirectionCheckpoint(alias, directionCheckpointBuilder.build()).build()
        }
        checkpointBuilder.putBookNameToSessionAliasToDirectionCheckpoint(bookName, builder.build())
    }
    return checkpointBuilder.build()
}

fun Checkpoint.convert(): InternalCheckpoint {
    val sessionKeyToSequence: MutableMap<SessionKey, InternalCheckpointData> = HashMap()
    bookNameToSessionAliasToDirectionCheckpointMap.forEach { (bookName, aliasToDirectionCheckpoint) ->
        aliasToDirectionCheckpoint.sessionAliasToDirectionCheckpointMap.forEach { (sessionAlias, directionCheckpoint) ->
            directionCheckpoint.directionToCheckpointDataMap.forEach { (directionNumber, checkpointData) ->
                sessionKeyToSequence[SessionKey(bookName, sessionAlias, Direction.forNumber(directionNumber))] = checkpointData.convert()
            }
        }
    }
    return InternalCheckpoint(id, sessionKeyToSequence)
}

fun generateChainID(): ChainID = ChainID.newBuilder().setId(EventUtils.generateUUID()).build()