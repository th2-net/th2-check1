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
package com.exactpro.th2.check1.rule

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.check.CheckRuleTask
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
import com.exactpro.th2.common.value.toValue
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals

class TestChain: AbstractCheckTaskTest() {

    private val eventID = EventID.newBuilder().setId("root").build()
    private val preFilter = PreFilter.newBuilder()
        .putFields(KEY_FIELD, ValueFilter.newBuilder().setKey(true).setOperation(FilterOperation.NOT_EMPTY).build())
        .build()

    @Test
    fun `simple rules - two succeed`() {
        val streams = createStreams(messages = (1..3).map(::createMessage))

        val task = checkRuleTask(1, eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(2, 2)
        checkSimpleVerifySuccess(eventList, 1)

        checkRuleTask(3, eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(4, 2)
        checkSimpleVerifySuccess(eventList, 3)
    }

    @Test
    fun `simple rules - failed, succeed`() {
        val streams = createStreams(messages = (1..3).map(::createMessage))

        val task = checkRuleTask(4, eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(2, 2)
        checkSimpleVerifyFailure(eventList)

        checkRuleTask(1, eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(4, 2)
        checkSimpleVerifySuccess(eventList, 1)
    }

    @Test
    fun `sequence rules - two succeed`() {
        val streams = createStreams(messages = (1..4).map(::createMessage))

        val task = sequenceCheckRuleTask(listOf(1, 2), eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(6, 6)
        checkSequenceVerifySuccess(eventList, listOf(1, 2))

        sequenceCheckRuleTask(listOf(3, 4), eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(12, 6)
        checkSequenceVerifySuccess(eventList, listOf(3, 4))
    }

    @Test
    fun `sequence rules - full failed, succeed`() {
        val streams = createStreams(messages = (1..2).map(::createMessage))

        val task = sequenceCheckRuleTask(listOf(3, 4), eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(6, 6)
        assertEquals(8, eventList.size)
        assertEquals(4, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(4, eventList.filter { it.status == FAILED }.size)

        sequenceCheckRuleTask(listOf(1, 2), eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(12, 6)
        checkSequenceVerifySuccess(eventList, listOf(1, 2))
    }

    @Test
    fun `sequence rules - part failed, succeed`() {
        val streams = createStreams(messages = (1..3).map(::createMessage))

        val task = sequenceCheckRuleTask(listOf(1, 4), eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(6, 6)
        assertEquals(9, eventList.size)
        assertEquals(5, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(4, eventList.filter { it.status == FAILED }.size)

        sequenceCheckRuleTask(listOf(2, 3), eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(12, 6)
        checkSequenceVerifySuccess(eventList, listOf(2, 3))
    }

    @Test
    fun `make long chain before begin`() {
        val streams = createStreams(messages = (1..4).map(::createMessage))

        val task = checkRuleTask(1, eventID, streams).also { one ->
            one.subscribeNextTask(checkRuleTask(2, eventID, streams).also { two ->
                two.subscribeNextTask(checkRuleTask(3, eventID, streams).also { three ->
                    three.subscribeNextTask(checkRuleTask(4, eventID, streams))
                })
            })
        }

        task.begin()

        val eventList = awaitEventBatchRequest(1000L, 4 * 2).flatMap(EventBatch::getEventsList)
        assertEquals(4 * 3, eventList.size)
        assertEquals(4 * 3, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(listOf(1L, 2L, 3L, 4L), eventList.filter { it.type == VERIFICATION_TYPE }.flatMap(Event::getAttachedMessageIdsList).map(MessageID::getSequence))
    }

    private fun awaitEventBatchAndGetEvents(times: Int, last: Int): List<Event> =
        awaitEventBatchRequest(1000L, times).drop(times - last).flatMap(EventBatch::getEventsList)

    private fun checkSimpleVerifySuccess(eventList: List<Event>, sequence: Long) {
        assertEquals(3, eventList.size)
        assertEquals(3, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(listOf(sequence), eventList.filter { it.type == VERIFICATION_TYPE }.flatMap(Event::getAttachedMessageIdsList).map(MessageID::getSequence))
    }

    private fun checkSimpleVerifyFailure(eventList: List<Event>) {
        assertEquals(3, eventList.size)
        assertEquals(1, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(2, eventList.filter { it.status == FAILED }.size)
    }

    private fun checkSequenceVerifySuccess(eventList: List<Event>, sequences: List<Long>) {
        assertEquals(8, eventList.size)
        assertEquals(8, eventList.filter { it.status == SUCCESS }.size)
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
        maxEventBatchContentSize: Int = 1024 * 1024
    ): SequenceCheckRuleTask {
        return SequenceCheckRuleTask(
            description = "Test",
            startTime = Instant.now(),
            sessionKey = SessionKey(SESSION_ALIAS, FIRST),
            taskTimeout = TaskTimeout(null, 1000L),
            maxEventBatchContentSize = maxEventBatchContentSize,
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
        maxEventBatchContentSize: Int = 1024 * 1024
    ) = CheckRuleTask(
        SESSION_ALIAS,
        Instant.now(),
        SessionKey(SESSION_ALIAS, FIRST),
        TaskTimeout(1000L),
        maxEventBatchContentSize,
        createMessageFilter(sequence),
        parentEventID,
        messageStream,
        clientStub
    )

    private fun createMessage(sequence: Int) = constructMessage(sequence.toLong())
        .putAllFields(mapOf(
            KEY_FIELD to "$KEY_FIELD$sequence".toValue(),
            NOT_KEY_FIELD to "$NOT_KEY_FIELD$sequence".toValue()
        )).build()

    private fun createMessageFilter(sequence: Int) = RootMessageFilter.newBuilder()
        .setMessageType(MESSAGE_TYPE)
        .setMessageFilter(
            MessageFilter.newBuilder()
                .putAllFields(mapOf(
                    KEY_FIELD to ValueFilter.newBuilder().setKey(true).setSimpleFilter("$KEY_FIELD$sequence").build(),
                    NOT_KEY_FIELD to ValueFilter.newBuilder().setSimpleFilter("$NOT_KEY_FIELD$sequence").build()
                ))
        ).build()

    companion object {
        private const val KEY_FIELD = "key"
        private const val NOT_KEY_FIELD = "not_key"
    }
}
