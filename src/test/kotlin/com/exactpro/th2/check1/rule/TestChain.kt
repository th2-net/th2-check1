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
import com.exactpro.th2.check1.rule.check.CheckRuleTask
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueFilter
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals

class TestChain: AbstractCheckTaskTest() {

    private val eventID = EventID.newBuilder().setId("root").build()

    @Test
    fun `simple rules - two succeed`() {
        val streams = createStreams(messages = (1..3).map(::createMessage))

        val task = checkTask(1, eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(2, 2)
        checkTaskVerifySuccess(eventList, 1)

        val task2 = checkTask(3, eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(4, 2)
        checkTaskVerifySuccess(eventList, 3)
    }

    @Test
    fun `simple rules - failed, succeed`() {
        val streams = createStreams(messages = (1..3).map(::createMessage))

        val task = checkTask(4, eventID, streams).also { it.begin() }
        var eventList = awaitEventBatchAndGetEvents(2, 2)
        checkTaskVerifyFailure(eventList, 4)

        val task2 = checkTask(1, eventID, streams).also { task.subscribeNextTask(it) }
        eventList = awaitEventBatchAndGetEvents(4, 2)
        checkTaskVerifySuccess(eventList, 1)
    }

    private fun awaitEventBatchAndGetEvents(times: Int, last: Int): List<Event> =
        awaitEventBatchRequest(1000L, times).drop(times - last).flatMap(EventBatch::getEventsList)

    private fun checkTaskVerifySuccess(eventList: List<Event>, sequence: Int) {
        assertEquals(3, eventList.size)
        assertEquals(3, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(listOf(sequence.toLong()), eventList.filter { it.type == VERIFICATION_TYPE }.flatMap(Event::getAttachedMessageIdsList).map(MessageID::getSequence))
    }

    private fun checkTaskVerifyFailure(eventList: List<Event>, sequence: Int) {
        assertEquals(3, eventList.size)
        assertEquals(1, eventList.filter { it.status == SUCCESS }.size)
        assertEquals(2, eventList.filter { it.status == FAILED }.size)
    }

    private fun checkTask(
        sequence: Int,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        maxEventBatchContentSize: Int = 1024 * 1024
    ) = CheckRuleTask(
        SESSION_ALIAS,
        Instant.now(),
        SessionKey(SESSION_ALIAS, FIRST),
        1000,
        maxEventBatchContentSize,
        createMessageFilter(sequence),
        parentEventID,
        messageStream,
        clientStub
    )

    private fun createMessage(sequence: Int) = constructMessage(sequence.toLong())
        .putAllFields(mapOf(
            KEY_FIELD to Value.newBuilder().setSimpleValue("$KEY_FIELD$sequence").build(),
            NOT_KEY_FIELD to Value.newBuilder().setSimpleValue("$NOT_KEY_FIELD$sequence").build()
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