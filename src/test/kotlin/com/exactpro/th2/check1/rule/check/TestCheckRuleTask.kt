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

package com.exactpro.th2.check1.rule.check

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.util.toSimpleFilter
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageFilter
import io.reactivex.Observable
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import java.util.regex.PatternSyntaxException
import kotlin.test.assertEquals

internal class TestCheckRuleTask : AbstractCheckTaskTest() {
    private fun checkTask(
        messageFilter: RootMessageFilter,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        maxEventBatchContentSize: Int = 1024 * 1024
    ) = CheckRuleTask(
        SESSION_ALIAS,
        Instant.now(),
        SessionKey(SESSION_ALIAS, Direction.FIRST),
        1000,
        maxEventBatchContentSize,
        messageFilter,
        parentEventID,
        messageStream,
        clientStub
    )

    @Test
    fun `success verification`() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL, true)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size)
        assertEquals(4, eventList.filter { it.status == SUCCESS }.size)
    }

    @Test
    internal fun `very little value of max event batch content size`() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL)))
            .build()
        val task = checkTask(filter, eventID, streams, 1)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 1)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(1, eventList.size)
        assertEquals(EventStatus.FAILED, eventList[0].status)
    }

    @Test
    internal fun `exceeds max event batch content size`() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL)))
            .build()
        val task = checkTask(filter, eventID, streams, 200)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 3)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size)
        assertEquals(2, eventList.filter { it.status == EventStatus.FAILED }.size) // Message filter and verification exceed max event batch content size
    }

    @Test
    internal fun findsMessageByMetadata() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatch.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size)
        assertTrue({
            eventList.none { it.status == EventStatus.FAILED }
        }) {
            "Some events are failed $eventBatch"
        }
    }

    @Test
    internal fun ignoresMessageIfMetadataDoesNotMatchByKeys() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "43".toSimpleFilter(FilterOperation.EQUAL, key = true)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L, 2)

        val checkFailedEvent = eventBatch.flatMap(EventBatch::getEventsList).firstOrNull {
            it.status == EventStatus.FAILED && it.type == "Check failed"
        }
        assertNotNull(checkFailedEvent) {
            "No failed event $eventBatch"
        }
    }

    @Test
    fun `failed rule creation due to invalid regex operation`() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
                message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                        .mergeMetadata(MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build())
                        .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
                .setMessageType(MESSAGE_TYPE)
                .setMetadataFilter(MetadataFilter.newBuilder()
                        .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL)))
                .setMessageFilter(messageFilter().putFields("Regex", ValueFilter.newBuilder().setOperation(FilterOperation.LIKE).setSimpleFilter(".[").build()))
                .build()
        
        assertThrows<PatternSyntaxException> {
            checkTask(filter, eventID, streams)
        }

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(2, eventList.size)
            assertEquals(1, eventList.filter { it.type == "invalidRegexOperation" }.size)
        })
    }
}