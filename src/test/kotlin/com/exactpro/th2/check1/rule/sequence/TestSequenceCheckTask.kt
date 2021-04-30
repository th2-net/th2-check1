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
package com.exactpro.th2.check1.rule.sequence

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueFilter
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.stream.Stream
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TestSequenceCheckTask : AbstractCheckTaskTest() {

    private val protoMessageFilters: List<RootMessageFilter> = listOf(
        RootMessageFilter.newBuilder()
            .setMessageType("TestMsg")
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(mapOf(
                        "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                        "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                    ))
            ).build(),
        RootMessageFilter.newBuilder()
            .setMessageType("TestMsg")
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(mapOf(
                        "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("43").build(),
                        "B" to ValueFilter.newBuilder().setSimpleFilter("BBB").build()
                    ))
            ).build(),
        RootMessageFilter.newBuilder()
            .setMessageType("TestMsg")
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(mapOf(
                        "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("44").build(),
                        "B" to ValueFilter.newBuilder().setSimpleFilter("CCC").build()
                    ))
            ).build()
    )

    private val messagesInCorrectOrder: List<Message> = listOf(
        constructMessage(1, "test_session", "TestMsg")
            .putAllFields(mapOf(
                "A" to Value.newBuilder().setSimpleValue("42").build(),
                "B" to Value.newBuilder().setSimpleValue("AAA").build()
            ))
            .build(),
        constructMessage(2, "test_session", "TestMsg")
            .putAllFields(mapOf(
                "A" to Value.newBuilder().setSimpleValue("43").build(),
                "B" to Value.newBuilder().setSimpleValue("BBB").build()
            ))
            .build(),
        constructMessage(3, "test_session", "TestMsg")
            .putAllFields(mapOf(
                "A" to Value.newBuilder().setSimpleValue("44").build(),
                "B" to Value.newBuilder().setSimpleValue("CCC").build()
            ))
            .build()
    )

    private val preFilter = PreFilter.newBuilder()
        .putFields("A", ValueFilter.newBuilder().setKey(true).setOperation(FilterOperation.NOT_EMPTY).build())
        .build()

    @ParameterizedTest(name = "checkOrder = {0}")
    @ValueSource(booleans = [true, false])
    fun `messages in right order passes`(checkOrder: Boolean) {
        val messages = Observable.fromIterable(messagesInCorrectOrder)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey("test_session", Direction.FIRST), 10, messages))
        val parentEventID = EventID.newBuilder().setId(EventUtils.generateUUID()).build()

        sequenceCheckRuleTask(parentEventID, messageStream, checkOrder).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        /*
            checkSequenceRule
              preFiltering
                Verification x 3
              checkMessages
                Verification x 3
              checkSequence
         */
        assertAll({
            with(batchRequest[0]) {
                assertEquals(1, eventsCount)
                assertEquals("checkSequenceRule", getEvents(0).type)
            }
            with(batchRequest[1]) {
                assertEquals(1, eventsCount)
                assertEquals("preFiltering", getEvents(0).type)
            }
            with(batchRequest[2]) {
                assertEquals(3, eventsCount)
                assertTrue (getEventsList().all { "Verification" == it.type })
            }
            with(batchRequest[3]) {
                assertEquals(1, eventsCount)
                assertEquals("checkMessages", getEvents(0).type)
            }
            with(batchRequest[4]) {
                assertEquals(3, eventsCount)
                assertTrue (getEventsList().all { "Verification" == it.type })
            }
            with(batchRequest[5]) {
                assertEquals(1, eventsCount)
                assertEquals("checkSequence", getEvents(0).type)
            }
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == "checkMessages" }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not passed: $verifications") { verifications.all { it.status == EventStatus.SUCCESS } }
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList)
        })
    }

    @ParameterizedTest(name = "Messages reordered: {0}")
    @MethodSource("indexesToSwitch")
    fun `verification with check order is failed in case messages are reordered`(indexesToSwitch: Pair<Int, Int>) {
        val messagesUnordered = messagesInCorrectOrder.toMutableList().apply {
            val tmp = get(indexesToSwitch.first)
            set(indexesToSwitch.first, get(indexesToSwitch.second))
            set(indexesToSwitch.second, tmp)
        }
        val idsOrder = messagesUnordered.map { it.metadata.id }
        val messages = Observable.fromIterable(messagesUnordered)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey("test_session", Direction.FIRST), 10, messages))
        val parentEventID = EventID.newBuilder().setId(EventUtils.generateUUID()).build()

        sequenceCheckRuleTask(parentEventID, messageStream, true).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val checkedMessages = assertNotNull(eventsList.find { it.type == "checkMessages" }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")

            val passedVerifications = verifications.filter { it.status == EventStatus.SUCCESS }
            // The messages are reordered but all presents. So, all verifications should be passed
            assertEquals(3, passedVerifications.size, "Unexpected SUCCESS verifications count: $passedVerifications")
            assertTrue("Some verifications have more than one message attached") { passedVerifications.all { it.attachedMessageIdsCount == 1 } }
            assertEquals(idsOrder, passedVerifications.map { it.getAttachedMessageIds(0) })
        }, {
            assertCheckSequenceStatus(EventStatus.FAILED, eventsList)
        })
    }

    @Test
    fun `check ordering is not failed in case key fields are matches the order but the rest are not`() {
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, "test_session", "TestMsg")
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA1").build()
                ))
                .build(),
            constructMessage(2, "test_session", "TestMsg")
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("43").build(),
                    "B" to Value.newBuilder().setSimpleValue("BBB1").build()
                ))
                .build(),
            constructMessage(3, "test_session", "TestMsg")
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("44").build(),
                    "B" to Value.newBuilder().setSimpleValue("CCC1").build()
                ))
                .build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey("test_session", Direction.FIRST), 10, messages))
        val parentEventID = EventID.newBuilder().setId(EventUtils.generateUUID()).build()

        sequenceCheckRuleTask(parentEventID, messageStream, true).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(3, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L, 2L, 3L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == "checkMessages" }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not failed: $verifications") { verifications.all { it.status == EventStatus.FAILED } }
            assertEquals(listOf(1L, 2L, 3L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because all key fields are in a correct order
        })
    }

    @ParameterizedTest(name = "Messages reordered: {0}")
    @MethodSource("indexesToSwitch")
    fun `verification without check order is not failed in case messages are reordered`(indexesToSwitch: Pair<Int, Int>) {
        val messagesUnordered = messagesInCorrectOrder.toMutableList().apply {
            val tmp = get(indexesToSwitch.first)
            set(indexesToSwitch.first, get(indexesToSwitch.second))
            set(indexesToSwitch.second, tmp)
        }
        val messages = Observable.fromIterable(messagesUnordered)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey("test_session", Direction.FIRST), 10, messages))
        val parentEventID = EventID.newBuilder().setId(EventUtils.generateUUID()).build()

        sequenceCheckRuleTask(parentEventID, messageStream, false).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val checkedMessages = assertNotNull(eventsList.find { it.type == "checkMessages" }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not passed: $verifications") { verifications.all { it.status == EventStatus.SUCCESS } }
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList)
        })
    }

    @Test
    fun `rules stops when all filters found match by key fields`() {
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, "test_session", "TestMsg")
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA1").build()
                ))
                .build(),
            constructMessage(2, "test_session", "TestMsg") // goes to processed messages but should not go to actual comparison
                .putAllFields(mapOf(
                    "B" to Value.newBuilder().setSimpleValue("BBB1").build()
                ))
                .build(),
            constructMessage(3, "test_session", "TestMsg")
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("43").build(),
                    "B" to Value.newBuilder().setSimpleValue("BBB1").build()
                ))
                .build(),
            constructMessage(4, "test_session", "TestMsg")
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("44").build(),
                    "B" to Value.newBuilder().setSimpleValue("CCC1").build()
                ))
                .build(),
            constructMessage(5, "test_session", "TestMsg") // should not be processed
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("DDD1").build()
                ))
                .build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey("test_session", Direction.FIRST), 10, messages))
        val parentEventID = EventID.newBuilder().setId(EventUtils.generateUUID()).build()

        sequenceCheckRuleTask(parentEventID, messageStream, false).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(4, rootEvent.attachedMessageIdsCount) // 3 match key + 1 that doesn't match but between others
            assertEquals(listOf(1L, 2L, 3L, 4L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == "checkMessages" }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not failed: $verifications") { verifications.all { it.status == EventStatus.FAILED } }
            assertEquals(listOf(1L, 3L, 4L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because the actual comparisons count equals to expected
        })
    }

    private fun assertCheckSequenceStatus(expectedStatus: EventStatus, eventsList: List<Event>) {
        val checkSequenceEvent = assertNotNull(eventsList.find { it.type == "checkSequence" }, "Cannot find checkSequence event")
        assertEquals(expectedStatus, checkSequenceEvent.status)
    }

    private fun sequenceCheckRuleTask(
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        checkOrder: Boolean,
        preFilterParam: PreFilter = preFilter,
        filtersParam: List<RootMessageFilter> = protoMessageFilters,
        maxEventBatchContentSize: Int = 1024 * 1024
    ): SequenceCheckRuleTask {
        return SequenceCheckRuleTask(
            description = "Test",
            startTime = Instant.now(),
            sessionKey = SessionKey("test_session", Direction.FIRST),
            timeout = 5000L,
            maxEventBatchContentSize = maxEventBatchContentSize,
            protoPreFilter = preFilterParam,
            protoMessageFilters = filtersParam,
            checkOrder = checkOrder,
            parentEventID = parentEventID,
            messageStream = messageStream,
            eventBatchRouter = clientStub
        )
    }

    private fun constructMessage(sequence: Long, alias: String, type: String): Message.Builder {
        return Message.newBuilder()
            .setMetadata(
                MessageMetadata.newBuilder()
                    .setId(MessageID.newBuilder().setSequence(sequence).setConnectionId(ConnectionID.newBuilder().setSessionAlias(alias)))
                    .setMessageType(type)
            )
    }

    companion object {
        @JvmStatic
        fun indexesToSwitch(): Stream<Arguments> {
            return Stream.of(
                arguments(0 to 1),
                arguments(0 to 2),
                arguments(1 to 2)
            )
        }
    }
}