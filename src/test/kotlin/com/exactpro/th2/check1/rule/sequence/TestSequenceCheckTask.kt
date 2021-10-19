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
import com.exactpro.th2.check1.exception.RuleInternalException
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask.Companion.CHECK_MESSAGES_TYPE
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask.Companion.CHECK_SEQUENCE_TYPE
import com.exactpro.th2.check1.util.toSimpleFilter
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageFilter
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
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
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(mapOf(
                        "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                        "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                    ))
            ).build(),
        RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(mapOf(
                        "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("43").build(),
                        "B" to ValueFilter.newBuilder().setSimpleFilter("BBB").build()
                    ))
            ).build(),
        RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(mapOf(
                        "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("44").build(),
                        "B" to ValueFilter.newBuilder().setSimpleFilter("CCC").build()
                    ))
            ).build()
    )

    private val messagesInCorrectOrder: List<Message> = listOf(
        constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE)
            .putAllFields(mapOf(
                "A" to Value.newBuilder().setSimpleValue("42").build(),
                "B" to Value.newBuilder().setSimpleValue("AAA").build()
            ))
            .build(),
        constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE)
            .putAllFields(mapOf(
                "A" to Value.newBuilder().setSimpleValue("43").build(),
                "B" to Value.newBuilder().setSimpleValue("BBB").build()
            ))
            .build(),
        constructMessage(3, SESSION_ALIAS, MESSAGE_TYPE)
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

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey(SESSION_ALIAS, Direction.FIRST), 10, messages))
        val parentEventID = createEvent(EventUtils.generateUUID())

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
                assertTrue (getEventsList().all { VERIFICATION_TYPE == it.type })
            }
            with(batchRequest[3]) {
                assertEquals(1, eventsCount)
                assertEquals(CHECK_MESSAGES_TYPE, getEvents(0).type)
            }
            with(batchRequest[4]) {
                assertEquals(3, eventsCount)
                assertTrue (getEventsList().all { VERIFICATION_TYPE == it.type })
            }
            with(batchRequest[5]) {
                assertEquals(1, eventsCount)
                assertEquals("checkSequence", getEvents(0).type)
            }
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
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

        val messageStream = createStreams(SESSION_ALIAS, Direction.FIRST, messagesUnordered)
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(parentEventID, messageStream, true).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")

            val passedVerifications = verifications.filter { it.status == EventStatus.SUCCESS }
            // The messages are reordered but every verification is presented. So, every verification should be passed
            assertEquals(3, passedVerifications.size, "Unexpected SUCCESS verifications count: $passedVerifications")
            assertTrue("Some verifications have more than one message attached") { passedVerifications.all { it.attachedMessageIdsCount == 1 } }
            // Ids in the result of the rule are in order by filters because the rule creates events related to verifications/filters in the source order.
            assertEquals(messagesInCorrectOrder.map { it.metadata.id }, passedVerifications.map { it.getAttachedMessageIds(0) })
        }, {
            assertCheckSequenceStatus(EventStatus.FAILED, eventsList)
        })
    }

    @ParameterizedTest(name = "check order: {0}")
    @MethodSource("checkOrderToSwitch")
    fun `check sequence should drop a message filter after match by key fields`(checkOrder: Boolean) {
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("BBB").build()
                ))
                .build()
        )

        val messageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(
                        mapOf(
                            "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                            "B" to ValueFilter.newBuilder().setSimpleFilter("CCC").build()
                        )
                    )
            ).build()
        val messageFilters: List<RootMessageFilter> = listOf(
            RootMessageFilter.newBuilder(messageFilter).build(),
            RootMessageFilter.newBuilder(messageFilter).build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey(SESSION_ALIAS, Direction.FIRST), 10, messages))
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(parentEventID, messageStream, checkOrder, filtersParam = messageFilters).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(2, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L, 2L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(2, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not success: $verifications") { verifications.all { it.status == EventStatus.FAILED } }
            assertEquals(listOf(1L, 2L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            val checkedSequence = assertNotNull(eventsList.find { it.type == CHECK_SEQUENCE_TYPE }, "Cannot find check sequence event")
            assertEquals(EventStatus.SUCCESS, checkedSequence.status)
        })
    }

    @Test
    fun `check sequence of messages with the same value of key field`() {
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(3, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build()
        )

        val messageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(
                        mapOf(
                            "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                            "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                        )
                    )
            ).build()
        val messageFilters: List<RootMessageFilter> = listOf(
            RootMessageFilter.newBuilder(messageFilter).build(),
            RootMessageFilter.newBuilder(messageFilter).build(),
            RootMessageFilter.newBuilder(messageFilter).build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey(SESSION_ALIAS, Direction.FIRST), 10, messages))
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(parentEventID, messageStream, true, filtersParam = messageFilters).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(3, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L, 2L, 3L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not success: $verifications") { verifications.all { it.status == EventStatus.SUCCESS } }
            assertEquals(listOf(1L, 2L, 3L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because all key fields are in a correct order
        })
    }

    @Test
    fun `check sequence of messages with the same value of key field and message timeout`() {
        val checkpointTimestamp = Instant.now()
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(0, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 100))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 100))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 200))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(3, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 300))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build()
        )

        val messageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(
                        mapOf(
                            "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                            "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                        )
                    )
            ).build()
        val messageFilters: List<RootMessageFilter> = listOf(
            RootMessageFilter.newBuilder(messageFilter).build(),
            RootMessageFilter.newBuilder(messageFilter).build(),
            RootMessageFilter.newBuilder(messageFilter).build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey(SESSION_ALIAS, Direction.FIRST), 10, messages))
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(
            parentEventID,
            messageStream,
            true,
            filtersParam = messageFilters,
            taskTimeout = TaskTimeout(5000L, 500L)
        ).begin(createCheckpoint(checkpointTimestamp, 0))

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(3, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L, 2L, 3L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not success: $verifications") { verifications.all { it.status == EventStatus.SUCCESS } }
            assertEquals(listOf(1L, 2L, 3L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because all key fields are in a correct order
        })
    }

    @Test
    fun `check sequence of messages with the same value of key field and expired message timeout`() {
        val checkpointTimestamp = Instant.now()
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(0, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 100))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 500))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 600))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(3, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 700))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build()
        )

        val messageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(
                        mapOf(
                            "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                            "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                        )
                    )
            ).build()
        val messageFilters: List<RootMessageFilter> = listOf(
            RootMessageFilter.newBuilder(messageFilter).build()
        )

        val messageStream = createStreams(SESSION_ALIAS, Direction.FIRST, messagesWithKeyFields)
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(
            parentEventID,
            messageStream,
            true,
            filtersParam = messageFilters,
            taskTimeout = TaskTimeout(5000L, 500L)
        ).begin(createCheckpoint(checkpointTimestamp, 0))

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(1, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(1, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not success: $verifications") { verifications.all { it.status == EventStatus.SUCCESS } }
            assertEquals(listOf(1L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because all key fields are in a correct order
        })
    }

    @Test
    fun `check sequence of messages with the same value of key field and one missed message due to message timeout`() {
        val checkpointTimestamp = Instant.now()
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 500))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 600))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build()
        )

        val messageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(
                        mapOf(
                            "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                            "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                        )
                    )
            ).build()
        val messageFilters: List<RootMessageFilter> = listOf(
            RootMessageFilter.newBuilder(messageFilter).build(),
            RootMessageFilter.newBuilder(messageFilter).build()
        )

        val messageStream = createStreams(SESSION_ALIAS, Direction.FIRST, messagesWithKeyFields)
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(
            parentEventID,
            messageStream,
            true,
            filtersParam = messageFilters,
            taskTimeout = TaskTimeout(5000L, 500L)
        ).begin(createCheckpoint(checkpointTimestamp, Long.MIN_VALUE))

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(2, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L, 2L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(2, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("The first verification should be success") { verifications.first().status == EventStatus.SUCCESS }
            assertTrue("The second verification should be failed due to message timeout") { verifications.last().status == EventStatus.FAILED }
            assertEquals(listOf(1L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.FAILED, eventsList) // because the second message was skipped due to message timeout
        })
    }

    @Test
    fun `check sequence of messages with message timeout and missed sequence`() {
        val checkpointTimestamp = Instant.now()
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 500))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE, timestamp = getMessageTimestamp(checkpointTimestamp, 600))
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA").build()
                ))
                .build()
        )

        val messageFilter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putAllFields(
                        mapOf(
                            "A" to ValueFilter.newBuilder().setKey(true).setSimpleFilter("42").build(),
                            "B" to ValueFilter.newBuilder().setSimpleFilter("AAA").build()
                        )
                    )
            ).build()
        val messageFilters: List<RootMessageFilter> = listOf(
            RootMessageFilter.newBuilder(messageFilter).build()
        )

        val messageStream = createStreams(SESSION_ALIAS, Direction.FIRST, messagesWithKeyFields)
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(
            parentEventID,
            messageStream,
            true,
            filtersParam = messageFilters,
            taskTimeout = TaskTimeout(5000L, 500L)
        ).begin(createCheckpoint(checkpointTimestamp, 0))

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(1, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(1, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not success: $verifications") { verifications.all { it.status == EventStatus.SUCCESS } }
            assertEquals(listOf(1L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because all key fields are in a correct order
        }, {
            assertEquals(EventStatus.FAILED, eventsList.last().status) // check event with missed start sequence
        })
    }

    @Test
    fun `check ordering is not failed in case key fields are matches the order but the rest are not`() {
        val messagesWithKeyFields: List<Message> = listOf(
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA1").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("43").build(),
                    "B" to Value.newBuilder().setSimpleValue("BBB1").build()
                ))
                .build(),
            constructMessage(3, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("44").build(),
                    "B" to Value.newBuilder().setSimpleValue("CCC1").build()
                ))
                .build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey(SESSION_ALIAS, Direction.FIRST), 10, messages))
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(parentEventID, messageStream, true).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(3, rootEvent.attachedMessageIdsCount)
            assertEquals(listOf(1L, 2L, 3L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
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

        val messageStream = createStreams(SESSION_ALIAS, Direction.FIRST, messagesUnordered)
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(parentEventID, messageStream, false).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
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
            constructMessage(1, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("AAA1").build()
                ))
                .build(),
            constructMessage(2, SESSION_ALIAS, MESSAGE_TYPE) // goes to processed messages but should not go to the actual comparison
                .putAllFields(mapOf(
                    "B" to Value.newBuilder().setSimpleValue("BBB1").build()
                ))
                .build(),
            constructMessage(3, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("43").build(),
                    "B" to Value.newBuilder().setSimpleValue("BBB1").build()
                ))
                .build(),
            constructMessage(4, SESSION_ALIAS, MESSAGE_TYPE)
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("44").build(),
                    "B" to Value.newBuilder().setSimpleValue("CCC1").build()
                ))
                .build(),
            constructMessage(5, SESSION_ALIAS, MESSAGE_TYPE) // should not be processed
                .putAllFields(mapOf(
                    "A" to Value.newBuilder().setSimpleValue("42").build(),
                    "B" to Value.newBuilder().setSimpleValue("DDD1").build()
                ))
                .build()
        )

        val messages = Observable.fromIterable(messagesWithKeyFields)

        val messageStream: Observable<StreamContainer> = Observable.just(StreamContainer(SessionKey(SESSION_ALIAS, Direction.FIRST), 10, messages))
        val parentEventID = createEvent(EventUtils.generateUUID())

        sequenceCheckRuleTask(parentEventID, messageStream, false).begin()

        val batchRequest = awaitEventBatchRequest(1000L, 6)
        val eventsList: List<Event> = batchRequest.flatMap(EventBatch::getEventsList)

        assertAll({
            val rootEvent = assertNotNull(eventsList.find { it.parentId == parentEventID })
            assertEquals(4, rootEvent.attachedMessageIdsCount) // 3 match key + 1 that doesn't match but is between others
            assertEquals(listOf(1L, 2L, 3L, 4L), rootEvent.attachedMessageIdsList.map { it.sequence })
        }, {
            val checkedMessages = assertNotNull(eventsList.find { it.type == CHECK_MESSAGES_TYPE }, "Cannot find checkMessages event")
            val verifications = eventsList.filter { it.parentId == checkedMessages.id }
            assertEquals(3, verifications.size, "Unexpected verifications count: $verifications")
            assertTrue("Some verifications are not failed: $verifications") { verifications.all { it.status == EventStatus.FAILED } }
            assertEquals(listOf(1L, 3L, 4L), verifications.flatMap { verification -> verification.attachedMessageIdsList.map { it.sequence } })
        }, {
            assertCheckSequenceStatus(EventStatus.SUCCESS, eventsList) // because the actual comparisons count equals to expected
        })
    }

    @Test
    fun `failed rule creation due to invalid regex operation in the message filter`() {
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
                .setMessageFilter(messageFilter().putFields("keyProp", ValueFilter.newBuilder().setOperation(FilterOperation.LIKE).setSimpleFilter(".[").build()))
                .build()

        assertThrows<RuleInternalException> {
            sequenceCheckRuleTask(parentEventID = eventID, messageStream = streams, filtersParam = listOf(filter), checkOrder = false).begin()
        }

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(3, eventList.size)
            assertEquals(1, eventList.filter { it.type == "internalError" }.size)
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
        taskTimeout: TaskTimeout = TaskTimeout(5000L)
    ): SequenceCheckRuleTask {
        return SequenceCheckRuleTask(
            ruleConfiguration = createRuleConfiguration(taskTimeout),
            startTime = Instant.now(),
            sessionKey = SessionKey(SESSION_ALIAS, Direction.FIRST),
            protoPreFilter = preFilterParam,
            protoMessageFilters = filtersParam,
            checkOrder = checkOrder,
            parentEventID = parentEventID,
            messageStream = messageStream,
            eventBatchRouter = clientStub
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
        @JvmStatic
        fun checkOrderToSwitch(): Stream<Arguments> {
            return Stream.of(
                arguments(false),
                arguments(true)
            )
        }
    }
}
