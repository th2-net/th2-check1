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

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.sf.scriptrunner.StatusType.FAILED
import com.exactpro.sf.scriptrunner.StatusType.PASSED
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.check1.util.toSimpleFilter
import com.exactpro.th2.common.grpc.ComplexFilter
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.FilterOperation.IN
import com.exactpro.th2.common.grpc.FilterOperation.LESS
import com.exactpro.th2.common.grpc.FilterOperation.LIKE
import com.exactpro.th2.common.grpc.FilterOperation.MORE
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import io.reactivex.Observable
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant
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
    private val converter = ProtoToIMessageConverter(VerificationUtil.FACTORY_PROXY, null, null)

    @Test
    internal fun listContainsValueFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("containFilter", ValueFilter.newBuilder()
                    .setComplexFilter(ComplexFilter.newBuilder().apply {
                        addAllComplexValues(listOf("A", "B", "C"))
                    })
                    .setOperation(IN)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("containFilter", "A".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(PASSED, result.getResult("containFilter").status)
    }

    @Test
    internal fun listDoesNotContainValueFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("containFilter", ValueFilter.newBuilder()
                    .setComplexFilter(ComplexFilter.newBuilder().apply {
                        addAllComplexValues(listOf("A", "B", "C"))
                    })
                    .setOperation(IN)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("containFilter", "D".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(FAILED, result.getResult("containFilter").status)
    }

    @Test
    internal fun regExGreedyFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("regexFilter", ValueFilter.newBuilder()
                    .setSimpleFilter("A.+a")
                    .setOperation(LIKE)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("regexFilter", "bbbb Abba Abbbbabbb".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(PASSED, result.getResult("regexFilter").status)
        assertEquals("Abba Abbbba", result.getResult("regexFilter").expressionResult.description)
    }

    @Test
    internal fun regExPossessiveFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("regexFilter", ValueFilter.newBuilder()
                    .setSimpleFilter("A.++a")
                    .setOperation(LIKE)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("regexFilter", "bbbb Abba Abbbbabbb".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(FAILED, result.getResult("regexFilter").status)
        assertEquals("", result.getResult("regexFilter").expressionResult.description )
    }

    @Test
    internal fun regExLazyFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("regexFilter", ValueFilter.newBuilder()
                    .setSimpleFilter("A.+?a")
                    .setOperation(LIKE)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("regexFilter", "bbbb Abba Abbbbabbb".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(PASSED, result.getResult("regexFilter").status)
        assertEquals("AbbaAbbbba", result.getResult("regexFilter").expressionResult.description)
    }

    @Test
    internal fun mathMoreFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("mathFilter", ValueFilter.newBuilder()
                    .setSimpleFilter("10")
                    .setOperation(MORE)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("mathFilter", "15".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(PASSED, result.getResult("mathFilter").status)
    }

    @Test
    internal fun mathLessFilter() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("mathFilter", ValueFilter.newBuilder()
                    .setSimpleFilter("10")
                    .setOperation(LESS)
                    .build())
                .build())
            .build()

        val actual = message(MESSAGE_TYPE).apply {
            putFields("mathFilter", "15".toValue())
        }.build()

        val result = getResult(actual, filter)

        assertEquals(FAILED, result.getResult("mathFilter").status)
    }

    private fun getResult(actual: Message, filter: RootMessageFilter) :ComparisonResult {
        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            metaContainer = container
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)

        return MessageComparator.compare(actualIMessage, filterIMessage, settings)
    }
}