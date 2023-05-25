/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.rule.RuleFactory
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.messageFilter
import com.exactpro.th2.common.message.rootMessageFilter
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.utils.message.proto.ProtoMessageWrapper
import com.exactpro.th2.common.value.toValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import kotlin.test.assertNull

class TestSequenceCheckTaskWithSilenceCheck : AbstractCheckTaskTest() {

    @Test
    fun `reports about extra messages when timeout exceeded`() {
        val streams = createStreams(messages = (1..3L).map {
            ProtoMessageWrapper(
                constructProtoMessage(sequence = it)
                    .putFields("A", 42.toValue())
                    .putFields("B", it.toValue())
                    .build()
            )
        })
        val factory = RuleFactory(Check1Configuration(), streams, clientStub)

        val filters = (0..1).map {
            rootMessageFilter(MESSAGE_TYPE)
                .setMessageFilter(
                    messageFilter()
                        .putFields(
                            "A",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter("42")
                                .setKey(true).build()
                        )
                        .putFields(
                            "B",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter(it.toString())
                                .build()
                        )
                ).build()
        }
        val preFilter = createPreFilter("A", "42")
        val parentId = createRootEventId()

        val request = CheckSequenceRuleRequest.newBuilder()
            .setConnectivityId(ConnectionID.newBuilder().setSessionAlias(SESSION_ALIAS))
            .setDirection(Direction.FIRST)
            .setTimeout(1000)
            .addAllRootMessageFilters(filters)
            .setPreFilter(preFilter)
            .setParentEventId(parentId)
            .setBookName(BOOK_NAME)
            .build()
        val sequenceRule = factory.createSequenceCheckRule(request, true)
        val silenceCheck = factory.createSilenceCheck(request, 1000)
        sequenceRule.subscribeNextTask(silenceCheck)
        sequenceRule.begin()

        val events = awaitEventBatchRequest(2000, 10).flatMap(EventBatch::getEventsList)
        val silenceCheckRoot = events.first { it.type == "AutoSilenceCheck" }.id
        val result = events.first { it.type == "noMessagesCheckResult" && it.parentId == silenceCheckRoot }
        assertAll(
            { assertEquals(EventStatus.FAILED, result.status) { "Unexpected status for event: ${result.toJson()}" } },
            {
                assertEquals(
                    "Check failed: 1 extra messages were found.",
                    result.name
                ) { "Unexpected name for event: ${result.toJson()}" }
            },
            {
                assertEquals(listOf(3L), result.attachedMessageIdsList.map { it.sequence }) {
                    "Unexpected messages attached: ${result.attachedMessageIdsList.map { it.toJson() }}"
                }
            }
        )
    }

    @Test
    fun `reports no extra messages found when timeout exceeded`() {
        val streams = createStreams(messages = (1..2L).map {
            ProtoMessageWrapper(
                constructProtoMessage(sequence = it)
                    .putFields("A", 42.toValue())
                    .putFields("B", it.toValue())
                    .build()
            )
        })
        val factory = RuleFactory(Check1Configuration(), streams, clientStub)

        val filters = (0..1).map {
            rootMessageFilter(MESSAGE_TYPE)
                .setMessageFilter(
                    messageFilter()
                        .putFields(
                            "A",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter("42")
                                .setKey(true).build()
                        )
                        .putFields(
                            "B",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter(it.toString())
                                .build()
                        )
                ).build()
        }
        val preFilter = createPreFilter("A", "42")
        val parentId = createRootEventId()

        val request = CheckSequenceRuleRequest.newBuilder()
            .setConnectivityId(ConnectionID.newBuilder().setSessionAlias(SESSION_ALIAS))
            .setDirection(Direction.FIRST)
            .setTimeout(1000)
            .addAllRootMessageFilters(filters)
            .setPreFilter(preFilter)
            .setParentEventId(parentId)
            .setBookName(BOOK_NAME)
            .build()
        val sequenceRule = factory.createSequenceCheckRule(request, true)
        val silenceCheck = factory.createSilenceCheck(request, 1000)
        sequenceRule.subscribeNextTask(silenceCheck)
        sequenceRule.begin()

        val events = awaitEventBatchRequest(2000, 8).flatMap(EventBatch::getEventsList)
        val silenceCheckRoot = events.first { it.type == "AutoSilenceCheck" }.id
        val result = events.first { it.type == "noMessagesCheckResult" && it.parentId == silenceCheckRoot }
        assertAll(
            { assertEquals(EventStatus.SUCCESS, result.status) { "Unexpected status for event: ${result.toJson()}" } },
            { assertEquals("Check passed", result.name) { "Unexpected name for event: ${result.toJson()}" } }
        )
    }

    @Test
    fun `does not report if next rule is subscribed in chain before beginning`() {
        val streams = createStreams(messages = (1..3L).map {
            ProtoMessageWrapper(
                constructProtoMessage(sequence = it)
                    .putFields("A", 42.toValue())
                    .putFields("B", it.toValue())
                    .build()
            )
        })
        val factory = RuleFactory(Check1Configuration(), streams, clientStub)

        val filters = (0..1).map {
            rootMessageFilter(MESSAGE_TYPE)
                .setMessageFilter(
                    messageFilter()
                        .putFields(
                            "A",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter("42")
                                .setKey(true).build()
                        )
                        .putFields(
                            "B",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter(it.toString())
                                .build()
                        )
                ).build()
        }
        val preFilter = createPreFilter("A", "42")
        val parentId = createRootEventId()

        val request = CheckSequenceRuleRequest.newBuilder()
            .setConnectivityId(ConnectionID.newBuilder().setSessionAlias(SESSION_ALIAS))
            .setDescription("1")
            .setDirection(Direction.FIRST)
            .setTimeout(1000)
            .addAllRootMessageFilters(filters)
            .setPreFilter(preFilter)
            .setParentEventId(parentId)
            .setBookName(BOOK_NAME)
            .build()
        val anotherRequest = request.toBuilder()
            .clearRootMessageFilters()
            .setDescription("2")
            .addRootMessageFilters(
                rootMessageFilter(MESSAGE_TYPE)
                    .setMessageFilter(
                        messageFilter()
                            .putFields(
                                "A",
                                ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter("42")
                                    .setKey(true).build()
                            )
                            .putFields(
                                "B",
                                ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter("2")
                                    .build()
                            )
                    ).build()
            )
            .build()
        val sequenceRule = factory.createSequenceCheckRule(request, true)
        val silenceCheck = factory.createSilenceCheck(request, 1000)
        val anotherRule = factory.createSequenceCheckRule(anotherRequest, true)
        sequenceRule.subscribeNextTask(silenceCheck)
        sequenceRule.begin()
        silenceCheck.subscribeNextTask(anotherRule)

        val events = awaitEventBatchRequest(2000, 12).flatMap(EventBatch::getEventsList)
        assertAll(
            { assertNull(events.find { it.type == "AutoSilenceCheck" }, "Unexpected events: $events") },
            {
                events.filter { it.name == "Check sequence rule - 1" }
                    .also {
                        assertEquals(1, it.size) { "Unexpected count of events for the first rule: $it" }
                    }
            },
            {
                events.filter { it.name == "Check sequence rule - 2" }
                    .also {
                        assertEquals(1, it.size) { "Unexpected count of events for the second rule: $it" }
                    }
            }
        )
    }

    @Test
    fun `does not report when next rule added to the chain before timeout exceeds`() {
        val streams = createStreams(messages = (1..2L).map {
            ProtoMessageWrapper(
                constructProtoMessage(sequence = it)
                    .putFields("A", 42.toValue())
                    .putFields("B", it.toValue())
                    .build()
            )
        })
        val factory = RuleFactory(Check1Configuration(), streams, clientStub)

        val filters = (0..1).map {
            rootMessageFilter(MESSAGE_TYPE)
                .setMessageFilter(
                    messageFilter()
                        .putFields(
                            "A",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter("42")
                                .setKey(true).build()
                        )
                        .putFields(
                            "B",
                            ValueFilter.newBuilder().setOperation(FilterOperation.EQUAL).setSimpleFilter(it.toString())
                                .build()
                        )
                ).build()
        }
        val preFilter = createPreFilter("A", "42")
        val parentId = createRootEventId()

        val request = CheckSequenceRuleRequest.newBuilder()
            .setConnectivityId(ConnectionID.newBuilder().setSessionAlias(SESSION_ALIAS))
            .setDescription("1")
            .setDirection(Direction.FIRST)
            .setTimeout(1000)
            .addAllRootMessageFilters(filters)
            .setPreFilter(preFilter)
            .setParentEventId(parentId)
            .setBookName(BOOK_NAME)
            .build()
        val silenceCheck = factory.createSilenceCheck(request, 1000)
        val sequenceRule = factory.createSequenceCheckRule(request.toBuilder().setDescription("2").build(), true)
        silenceCheck.begin()
        silenceCheck.subscribeNextTask(sequenceRule)

        val events = awaitEventBatchRequest(2000, 6).flatMap(EventBatch::getEventsList)
        assertAll(
            { assertNull(events.find { it.type == "AutoSilenceCheck" }, "Unexpected events: $events") },
            {
                events.filter { it.name == "Check sequence rule - 2" }
                    .also {
                        assertEquals(1, it.size) { "Unexpected count of events for the rule: $it" }
                    }
            }
        )
    }
}