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

package com.exactpro.th2.check1.rule.nomessage

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.value.toValue
import com.google.protobuf.Timestamp
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestNoMessageCheckTask : AbstractCheckTaskTest() {
    @Test
    fun `no messages outside the prefilter`() {
        val checkpointTimestamp = Instant.now()
        val messageTimeout = 1500L
        val streams = createStreams(
            messages = createMessages(
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 500)),
                MessageData("B", "2".toValue(), getMessageTimestamp(checkpointTimestamp, 1000)),
                MessageData("C", "3".toValue(), getMessageTimestamp(checkpointTimestamp, 1300)),
                MessageData("D", "4".toValue(), getMessageTimestamp(checkpointTimestamp, 1500)),
                MessageData("E", "5".toValue(), getMessageTimestamp(checkpointTimestamp, 1600)),
                // should be skipped because of message timeout
                MessageData("F", "6".toValue(), getMessageTimestamp(checkpointTimestamp, 1600))
            )
        )

        val eventID = createEvent("root")
        val task = noMessageCheckTask(
            eventID,
            streams,
            createPreFilter("E", "5", FilterOperation.EQUAL),
            TaskTimeout(5000, messageTimeout)
        )
        task.begin(createCheckpoint(checkpointTimestamp))

        val eventBatch = awaitEventBatchRequest(1000L, 2)
        val eventsList = eventBatch.flatMap(EventBatch::getEventsList)

        assertAll({
            assertTrue(eventsList.all { it.status == EventStatus.SUCCESS }, "Has messages outside the prefilter")
            assertTrue(eventsList.first().attachedMessageIdsCount == 5)
            assertTrue(eventsList[1].attachedMessageIdsCount == 4)
            assertTrue(eventsList.last().attachedMessageIdsCount == 0)
        })
    }

    @Test
    fun `with messages outside the prefilter`() {
        val checkpointTimestamp = Instant.now()
        val messageTimeout = 1500L
        val streams = createStreams(
            messages = createMessages(
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 100)),
                MessageData("B", "2".toValue(), getMessageTimestamp(checkpointTimestamp, 500)),
                MessageData("C", "3".toValue(), getMessageTimestamp(checkpointTimestamp, 700)),
                MessageData("D", "4".toValue(), getMessageTimestamp(checkpointTimestamp, 1600)),
                // should be skipped because of message timeout
                MessageData("E", "5".toValue(), getMessageTimestamp(checkpointTimestamp, 1700))
                )
        )

        val eventID = createEvent("root")
        val task = noMessageCheckTask(
            eventID,
            streams,
            createPreFilter("A", "1", FilterOperation.EQUAL),
            TaskTimeout(5000, messageTimeout)
        )
        task.begin(createCheckpoint(checkpointTimestamp))

        val eventBatch = awaitEventBatchRequest(1000L, 2)
        val eventsList = eventBatch.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = eventsList.first()
            assertEquals(rootEvent.status, EventStatus.FAILED, "Event status should be failed")
            assertTrue(rootEvent.attachedMessageIdsCount == 4)
            assertTrue(eventsList[1].attachedMessageIdsCount == 2)
            assertTrue(eventsList.last().attachedMessageIdsCount == 1)
        })
    }

    @Test
    fun `check messages without message timeout`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(
            messages = createMessages(
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 100)),
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 500)),
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 700)),
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 1000)),
                MessageData("A", "1".toValue(), getMessageTimestamp(checkpointTimestamp, 1300))
            )
        )

        val eventID = createEvent("root")
        val task = noMessageCheckTask(
            eventID,
            streams,
            createPreFilter("B", "2", FilterOperation.EQUAL),
            TaskTimeout(2000)
        )
        task.begin(createCheckpoint(checkpointTimestamp))

        val eventBatch = awaitEventBatchRequest(1000L, 4)
        val eventsList = eventBatch.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = eventsList.first()
            assertEquals(rootEvent.status, EventStatus.FAILED, "Root event should be failed due to timeout")
            assertTrue(rootEvent.attachedMessageIdsCount == 5)
            assertTrue(eventsList[1].attachedMessageIdsCount == 0)
            assertTrue(eventsList[1].name == "Check passed", "All messages should be ignored due to prefilter")
            assertTrue(eventsList[3].attachedMessageIdsCount == 5)
        })
    }


    private fun createMessages(
        vararg messageData: MessageData,
        sessionAlias: String = SESSION_ALIAS,
        messageType: String = MESSAGE_TYPE,
        direction: Direction = Direction.FIRST
    ): List<Message> {
        var sequence = 1L;
        return messageData.map { data ->
            constructMessage(sequence++, sessionAlias, messageType, direction, data.timestamp)
                .putFields(data.fieldName, data.value)
                .build()
        }
    }

    private fun createPreFilter(fieldName: String, value: String, operation: FilterOperation): PreFilter =
        PreFilter.newBuilder()
            .putFields(fieldName, ValueFilter.newBuilder().setSimpleFilter(value).setKey(true).setOperation(operation).build())
            .build()

    private fun noMessageCheckTask(
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        preFilterParam: PreFilter,
        taskTimeout: TaskTimeout = TaskTimeout(5000L, 3500L),
        maxEventBatchContentSize: Int = 1024 * 1024
    ): NoMessageCheckTask {
        return NoMessageCheckTask(
            description = "Test",
            startTime = Instant.now(),
            sessionKey = SessionKey(SESSION_ALIAS, Direction.FIRST),
            taskTimeout = taskTimeout,
            maxEventBatchContentSize = maxEventBatchContentSize,
            protoPreFilter = preFilterParam,
            parentEventID = parentEventID,
            messageStream = messageStream,
            eventBatchRouter = clientStub
        )
    }

    data class MessageData(val fieldName: String, val value: Value, val timestamp: Timestamp)
}