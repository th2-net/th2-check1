/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1.rule

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.check.CheckRuleTask
import com.exactpro.th2.check1.rule.nomessage.NoMessageCheckTask
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask.Companion.CHECK_MESSAGES_TYPE
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.utils.message.ProtoMessageHolder
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import com.exactpro.th2.common.value.toValue
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class TestChain : AbstractCheckTaskTest() {
    private val eventID = createRootEventId()
    private val preFilter = PreFilter.newBuilder()
        .putFields(KEY_FIELD, ValueFilter.newBuilder().setKey(true).setOperation(FilterOperation.NOT_EMPTY).build())
        .build()

    @ParameterizedTest(name = "useTransport = {0}")
    @ValueSource(booleans = [true, false])
    fun `simple rules - two succeed`(useTransport: Boolean) {
        val streams = createStreams(messages = (1..3).map { createMessage(it, useTransport) })

        val task = checkRuleTask(1, eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(2, 2)
        checkSimpleVerifySuccess(eventList, 1)

        checkRuleTask(3, eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(4, 2)
        checkSimpleVerifySuccess(eventList, 3)
    }

    @ParameterizedTest(name = "useTransport = {0}")
    @ValueSource(booleans = [true, false])
    fun `simple rules - failed, succeed`(useTransport: Boolean) {
        val streams = createStreams(messages = (1..3).map { createMessage(it, useTransport) })

        val task = checkRuleTask(4, eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(2, 2)
        checkSimpleVerifyFailure(eventList)

        checkRuleTask(1, eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(4, 2)
        checkSimpleVerifySuccess(eventList, 1)
    }

    @ParameterizedTest(name = "useTransport = {0}")
    @ValueSource(booleans = [true, false])
    fun `sequence rules - two succeed`(useTransport: Boolean) {
        val streams = createStreams(messages = (1..4).map { createMessage(it, useTransport) })

        val task = sequenceCheckRuleTask(listOf(1, 2), eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(6, 6)
        checkSequenceVerifySuccess(eventList, listOf(1, 2))

        sequenceCheckRuleTask(listOf(3, 4), eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(12, 6)
        checkSequenceVerifySuccess(eventList, listOf(3, 4))
    }

    @ParameterizedTest(name = "useTransport = {0}")
    @ValueSource(booleans = [true, false])
    fun `sequence rules - full failed, succeed`(useTransport: Boolean) {
        val streams = createStreams(messages = (1..2).map { createMessage(it, useTransport) })

        val task = sequenceCheckRuleTask(listOf(3, 4), eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(6, 6)
        assertEquals(9, eventList.size)
        assertEquals(5, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(4, eventList.filter { it.status == FAILED }.size)

        sequenceCheckRuleTask(listOf(1, 2), eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(12, 6)
        checkSequenceVerifySuccess(eventList, listOf(1, 2))
    }

    @ParameterizedTest(name = "useTransport = {0}")
    @ValueSource(booleans = [true, false])
    fun `sequence rules - part failed, succeed`(useTransport: Boolean) {
        val streams = createStreams(messages = (1..3).map { createMessage(it, useTransport) })

        val task = sequenceCheckRuleTask(listOf(1, 4), eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(6, 6)
        assertEquals(10, eventList.size)
        assertEquals(6, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(4, eventList.filter { it.status == FAILED }.size)

        sequenceCheckRuleTask(listOf(2, 3), eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(12, 6)
        checkSequenceVerifySuccess(eventList, listOf(2, 3))
    }

    @ParameterizedTest(name = "useTransport = {0}")
    @ValueSource(booleans = [true, false])
    fun `make long chain before begin`(useTransport: Boolean) {
        val streams = createStreams(messages = (1..4).map { createMessage(it, useTransport) })

        val task = checkRuleTask(1, eventID, streams).also { one ->
            one.subscribeNextTask(checkRuleTask(2, eventID, streams).also { two ->
                two.subscribeNextTask(checkRuleTask(3, eventID, streams).also { three ->
                    three.subscribeNextTask(checkRuleTask(4, eventID, streams))
                })
            })
        }

        task.begin()

        val eventList = awaitEventBatchRequest(1000L, 4 * 2).flatMap(EventBatch::getEventsList)
        assertEquals(4 * 4, eventList.size)
        assertEquals(4 * 4, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(
            listOf(1L, 2L, 3L, 4L),
            eventList.filter { it.type == VERIFICATION_TYPE }.flatMap(Event::getAttachedMessageIdsList)
                .map(MessageID::getSequence)
        )
    }

    @Test
    fun `sequence rules - untrusted execution`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(messages = (2..6L).map {
            ProtoMessageHolder(
                constructProtoMessage(it, timestamp = getProtoTimestamp(checkpointTimestamp, it * 1000))
                    .putAllFields(
                        mapOf(
                            KEY_FIELD to "$KEY_FIELD$it".toValue(),
                            NOT_KEY_FIELD to "$NOT_KEY_FIELD$it".toValue()
                        )
                    ).build()
            )
        })

        val task = sequenceCheckRuleTask(
            listOf(2, 3),
            eventID,
            streams,
            taskTimeout = TaskTimeout(2000L, 500)
        ).also { it.begin(createCheckpoint(checkpointTimestamp, 1)) }
        var eventsList = awaitEventBatchAndGetEvents(4, 4)
        assertAll({
            val rootEvent = eventsList.first()
            assertEquals(FAILED, rootEvent.status, "Event status should be failed")
            assertTrue(rootEvent.attachedMessageIdsCount == 1)
        })

        sequenceCheckRuleTask(
            listOf(4, 5),
            eventID,
            streams,
            taskTimeout = TaskTimeout(2000L, 1500L)
        ).also { task.subscribeNextTask(it) }
        eventsList = awaitEventBatchAndGetEvents(10, 6)
        assertEquals(UNTRUSTED_EXECUTION_EVENT_NAME, eventsList.last().name)
    }

    @Test
    fun `no messages sequence rules - untrusted execution`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(messages = (2..6L).map {
            ProtoMessageHolder(
                constructProtoMessage(it, timestamp = getProtoTimestamp(checkpointTimestamp, it * 1000))
                    .putAllFields(
                        mapOf(
                            KEY_FIELD to "$KEY_FIELD$it".toValue(),
                            NOT_KEY_FIELD to "$NOT_KEY_FIELD$it".toValue()
                        )
                    ).build()
            )
        })

        val task = noMessageCheckTask(
            eventID,
            streams,
            taskTimeout = TaskTimeout(2000L, 500),
            preFilterParam = createPreFilter("E", "5")
        ).also { it.begin(createCheckpoint(checkpointTimestamp, 1)) }
        var eventsList = awaitEventBatchAndGetEvents(2, 2)
        assertAll({
            val rootEvent = eventsList.first()
            assertEquals(FAILED, rootEvent.status, "Event status should be failed")
            assertTrue(rootEvent.attachedMessageIdsCount == 1)
        })

        noMessageCheckTask(
            eventID,
            streams,
            taskTimeout = TaskTimeout(2000L, 1500L),
            preFilterParam = createPreFilter("E", "5")
        ).also { task.subscribeNextTask(it) }
        eventsList = awaitEventBatchAndGetEvents(6, 4)
        assertEquals(UNTRUSTED_EXECUTION_EVENT_NAME, eventsList.last().name)
    }

    @Test
    fun `simple rules - ignored untrusted execution`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(messages = (2..6L).map {
            ProtoMessageHolder(
                constructProtoMessage(it, timestamp = getProtoTimestamp(checkpointTimestamp, it * 1000))
                    .putAllFields(
                        mapOf(
                            KEY_FIELD to "$KEY_FIELD$it".toValue(),
                            NOT_KEY_FIELD to "$NOT_KEY_FIELD$it".toValue()
                        )
                    ).build()
            )
        })

        val task = checkRuleTask(
            1, eventID, streams, taskTimeout = TaskTimeout(2000L, 500)
        ).also { it.begin(createCheckpoint(checkpointTimestamp, 1)) }
        var eventsList = awaitEventBatchAndGetEvents(2, 2)
        assertAll({
            val rootEvent = eventsList.first()
            assertEquals(FAILED, rootEvent.status, "Event status should be failed")
            assertTrue(rootEvent.attachedMessageIdsCount == 1)
        })

        checkRuleTask(4, eventID, streams).also { task.subscribeNextTask(it) }
        eventsList = awaitEventBatchAndGetEvents(4, 2)
        assertAll({
            val rootEvent = eventsList.first()
            assertEquals(FAILED, rootEvent.status)
            assertEquals(3, rootEvent.attachedMessageIdsCount)
            assertEquals(1, eventsList.single { it.type == "Verification" }.attachedMessageIdsCount)
            assertEquals(FAILED, eventsList.last().status)
        })
    }


    private fun awaitEventBatchAndGetEvents(times: Int, last: Int): List<Event> =
        awaitEventBatchRequest(1000L, times).drop(times - last).flatMap(EventBatch::getEventsList)

    private fun checkSimpleVerifySuccess(eventList: List<Event>, sequence: Long) {
        assertEquals(4, eventList.size)
        assertEquals(4, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(
            listOf(sequence),
            eventList.filter { it.type == VERIFICATION_TYPE }.flatMap(Event::getAttachedMessageIdsList)
                .map(MessageID::getSequence)
        )
    }

    private fun checkSimpleVerifyFailure(eventList: List<Event>) {
        assertEquals(4, eventList.size)
        assertEquals(2, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(2, eventList.filter { it.status == FAILED }.size)
    }

    private fun checkSequenceVerifySuccess(eventList: List<Event>, sequences: List<Long>) {
        assertEquals(9, eventList.size)
        assertEquals(9, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(sequences, eventList
            .dropWhile { it.type != CHECK_MESSAGES_TYPE } // Skip prefilter
            .filter { it.type == VERIFICATION_TYPE }
            .flatMap(Event::getAttachedMessageIdsList)
            .map(MessageID::getSequence))
    }

    private fun sequenceCheckRuleTask(
        sequence: List<Int>,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        checkOrder: Boolean = true,
        preFilterParam: PreFilter = preFilter,
        taskTimeout: TaskTimeout = TaskTimeout(1000L)
    ): SequenceCheckRuleTask {
        return SequenceCheckRuleTask(
            ruleConfiguration = createRuleConfiguration(taskTimeout),
            startTime = Instant.now(),
            sessionKey = SessionKey(BOOK_NAME, SESSION_ALIAS, FIRST),
            protoPreFilter = preFilterParam,
            protoMessageFilters = sequence.map(::createMessageFilter).toList(),
            checkOrder = checkOrder,
            parentEventID = parentEventID,
            messageStream = messageStream,
            eventBatchRouter = clientStub
        )
    }

    private fun checkRuleTask(
        sequence: Int,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        taskTimeout: TaskTimeout = TaskTimeout(1000L)
    ) = CheckRuleTask(
        createRuleConfiguration(taskTimeout, SESSION_ALIAS),
        Instant.now(),
        SessionKey(BOOK_NAME, SESSION_ALIAS, FIRST),
        createMessageFilter(sequence),
        parentEventID,
        messageStream,
        clientStub
    )

    private fun noMessageCheckTask(
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        preFilterParam: PreFilter,
        taskTimeout: TaskTimeout = TaskTimeout(5000L, 3500L)
    ): NoMessageCheckTask {
        return NoMessageCheckTask(
            ruleConfiguration = createRuleConfiguration(taskTimeout),
            startTime = Instant.now(),
            sessionKey = SessionKey(BOOK_NAME, SESSION_ALIAS, FIRST),
            protoPreFilter = preFilterParam,
            parentEventID = parentEventID,
            messageStream = messageStream,
            eventBatchRouter = clientStub
        )
    }

    private fun createMessage(sequence: Int, useTransport: Boolean) = if (useTransport) {
        createTransportMessage(sequence)
    } else {
        createProtoMessage(sequence)
    }

    private fun createProtoMessage(sequence: Int) = ProtoMessageHolder(
        constructProtoMessage(sequence.toLong())
            .putAllFields(
                mapOf(
                    KEY_FIELD to "$KEY_FIELD$sequence".toValue(),
                    NOT_KEY_FIELD to "$NOT_KEY_FIELD$sequence".toValue()
                )
            ).build()
    )

    private fun createTransportMessage(sequence: Int) = TransportMessageHolder(
        constructTransportMessage(sequence.toLong()).apply {
            setBody(
                hashMapOf(
                    KEY_FIELD to "$KEY_FIELD$sequence",
                    NOT_KEY_FIELD to "$NOT_KEY_FIELD$sequence"
                )
            )
        }.build(),
        BOOK_NAME,
        ""
    )

    private fun createMessageFilter(sequence: Int) = RootMessageFilter.newBuilder()
        .setMessageType(MESSAGE_TYPE)
        .setMessageFilter(
            MessageFilter.newBuilder()
                .putAllFields(
                    mapOf(
                        KEY_FIELD to ValueFilter.newBuilder().setKey(true).setSimpleFilter("$KEY_FIELD$sequence")
                            .build(),
                        NOT_KEY_FIELD to ValueFilter.newBuilder().setSimpleFilter("$NOT_KEY_FIELD$sequence").build()
                    )
                )
        ).build()

    companion object {
        private const val KEY_FIELD = "key"
        private const val NOT_KEY_FIELD = "not_key"
        private const val UNTRUSTED_EXECUTION_EVENT_NAME: String =
            "The current check is untrusted because previous rule in the chain started from approximate start point"
    }
}
