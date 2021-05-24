/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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
@file:JvmName("CheckTaskUtils")
package com.exactpro.th2.check1.rule

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventBatch.Builder
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import mu.KotlinLogging
import com.exactpro.th2.common.grpc.Event as ProtoEvent

val OBJECT_MAPPER = ObjectMapper()
val LOGGER = KotlinLogging.logger {}

fun Event.disperseToBatches(maxEventBatchContentSize: Int, parentEventId: EventID): List<EventBatch> {
    require(maxEventBatchContentSize > 0) {
        "'maxEventBatchContentSize' should be greater than zero, actual: $maxEventBatchContentSize"
    }

    val events = toListProto(parentEventId)

    val result = ArrayList<EventBatch>()
    val eventGroups = events.groupBy(ProtoEvent::getParentId)
    batch(maxEventBatchContentSize, result, eventGroups, parentEventId)
    return result
}

private fun batch(maxEventBatchContentSize: Int, result: MutableList<EventBatch>, eventGroups: Map<EventID, List<ProtoEvent>>, eventID: EventID) {
    var builder = EventBatch.newBuilder().setParentEventId(eventID)

    checkNotNull(eventGroups[eventID]) {
        "Neither of events refers to ${TextFormat.shortDebugString(eventID)}"
    }.forEach { event ->
        val checkedEvent = checkAndRebuild(maxEventBatchContentSize, event)

        LOGGER.trace("Process ${checkedEvent.name} ${checkedEvent.type}")
        if(eventGroups.containsKey(checkedEvent.id)) {
            result.add(checkAndBuild(maxEventBatchContentSize, EventBatch.newBuilder()
                .addEvents(checkedEvent)))

            batch(maxEventBatchContentSize, result, eventGroups, checkedEvent.id)
        } else {
            if(builder.eventsCount > 0
                && builder.getContentSize() + checkedEvent.getContentSize() > maxEventBatchContentSize) {
                result.add(checkAndBuild(maxEventBatchContentSize, builder))
                builder = EventBatch.newBuilder().setParentEventId(eventID)
            }
            builder.addEvents(checkedEvent)
        }
    }

    if(builder.eventsCount > 0) {
        result.add(checkAndBuild(maxEventBatchContentSize, builder))
    }
}

private fun checkAndBuild(maxEventBatchContentSize: Int, builder: Builder): EventBatch {
    val contentSize = builder.getContentSize()
    check(contentSize <= maxEventBatchContentSize) {
        "The smallest batch size exceeds the max event batch content size, max $maxEventBatchContentSize, actual $contentSize"
    }

    return builder.build()
}

private fun checkAndRebuild(maxEventBatchContentSize: Int, event: ProtoEvent): ProtoEvent {
    return if (event.getContentSize() > maxEventBatchContentSize) {
        ProtoEvent.newBuilder(event).apply {
            status = EventStatus.FAILED
            body = "Event ${TextFormat.shortDebugString(event.id)} exceeds max size, max $maxEventBatchContentSize, actual ${event.getContentSize()}".toEventBody()
        }.build()
    } else { event }
}

private fun String.toEventBody(): ByteString = ByteString.copyFrom(OBJECT_MAPPER.writeValueAsBytes(listOf(EventUtils.createMessageBean(this))))

private fun ProtoEvent.getContentSize(): Int {
    return body.size()
}

private fun Builder.getContentSize(): Int {
    return eventsList.map { it.getContentSize() }.sum()
}