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

import com.exactpro.th2.check1.CheckTaskKey
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.exception.RuleCreationException
import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.timeout
import com.nhaarman.mockitokotlin2.verify
import io.reactivex.Observable
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class RuleFactoryTest {
    private val clientStub: MessageRouter<EventBatch> = spy { }

    @Test
    fun `failed rule creation because one of required fields is empty`() {
        val streams = createStreams(AbstractCheckTaskTest.SESSION_ALIAS, Direction.FIRST, listOf(
                message(AbstractCheckTaskTest.MESSAGE_TYPE, Direction.FIRST, AbstractCheckTaskTest.SESSION_ALIAS)
                        .mergeMetadata(MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build())
                        .build()
        ))

        val ruleFactory = RuleFactory(Check1Configuration(), streams, clientStub)

        val request = CheckRuleRequest.newBuilder()
                .setParentEventId(EventID.newBuilder().setId("root").build())
                .setCheckpoint(Checkpoint.newBuilder().setId(EventUtils.generateUUID()).build()).build()

        assertThrows<RuleCreationException> {
            ruleFactory.createCheckRule(request, true)
        }

        val eventBatches = awaitEventBatchRequest(1000L, 1)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(2, eventList.size)
            assertEquals(1, eventList.filter { it.type == "ruleCreationException" }.size)
        })
    }

    @Test
    fun `success rule creation with missed checkpoint`() {
        val streams = createStreams(AbstractCheckTaskTest.SESSION_ALIAS, Direction.FIRST, listOf(
                message(AbstractCheckTaskTest.MESSAGE_TYPE, Direction.FIRST, AbstractCheckTaskTest.SESSION_ALIAS)
                        .mergeMetadata(MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build())
                        .build()
        ))

        val ruleFactory = RuleFactory(Check1Configuration(), streams, clientStub)

        val request = CheckRuleRequest.newBuilder()
                .setParentEventId(EventID.newBuilder().setId("root").build())
                .setConnectivityId(ConnectionID.newBuilder()
                        .setSessionAlias("test_alias")
                )
                .setRootFilter(RootMessageFilter.newBuilder()
                        .setMessageType("TestMsgType")
                )
                .setMessageTimeout(5)
                .setChainId(ChainID.newBuilder().setId("test_chain_id"))
                .build()

        val createCheckRule = assertDoesNotThrow {
            ruleFactory.createCheckRule(request, true)
        }
        assertNotNull(createCheckRule) { "Rule cannot be null" }
    }

    @Test
    fun `failed rule creation with missed checkpoint and invalid chain id`() {
        val streams = createStreams(AbstractCheckTaskTest.SESSION_ALIAS, Direction.FIRST, listOf(
                message(AbstractCheckTaskTest.MESSAGE_TYPE, Direction.FIRST, AbstractCheckTaskTest.SESSION_ALIAS)
                        .mergeMetadata(MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build())
                        .build()
        ))

        val ruleFactory = RuleFactory(Check1Configuration(), streams, clientStub)

        val request = CheckRuleRequest.newBuilder()
                .setParentEventId(EventID.newBuilder().setId("root").build())
                .setConnectivityId(ConnectionID.newBuilder()
                        .setSessionAlias("test_alias")
                )
                .setRootFilter(RootMessageFilter.newBuilder()
                        .setMessageType("TestMsgType")
                )
                .setMessageTimeout(5)
                .setChainId(ChainID.newBuilder().setId("test_chain_id"))
                .build()

        assertThrows<RuleCreationException> {
            ruleFactory.createCheckRule(request, false)
        }

        val eventBatches = awaitEventBatchRequest(1000L, 1)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(2, eventList.size)
            assertEquals(1, eventList.filter { it.type == "ruleCreationException" }.size)
        })
    }

    @Test
    fun `success rule creation with missed chain id`() {
        val streams = createStreams(AbstractCheckTaskTest.SESSION_ALIAS, Direction.FIRST, listOf(
                message(AbstractCheckTaskTest.MESSAGE_TYPE, Direction.FIRST, AbstractCheckTaskTest.SESSION_ALIAS)
                        .mergeMetadata(MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build())
                        .build()
        ))

        val ruleFactory = RuleFactory(Check1Configuration(), streams, clientStub)

        val request = CheckRuleRequest.newBuilder()
                .setParentEventId(EventID.newBuilder().setId("root").build())
                .setConnectivityId(ConnectionID.newBuilder()
                        .setSessionAlias("test_alias")
                )
                .setRootFilter(RootMessageFilter.newBuilder()
                        .setMessageType("TestMsgType")
                )
                .setMessageTimeout(5)
                .setCheckpoint(
                        Checkpoint.newBuilder()
                                .setId(EventUtils.generateUUID())
                                .putSessionAliasToDirectionCheckpoint(
                                        "test_alias",
                                        Checkpoint.DirectionCheckpoint.newBuilder()
                                                .putDirectionToCheckpointData(
                                                        Direction.FIRST.number,
                                                        Checkpoint.CheckpointData.newBuilder()
                                                                .setSequence(1)
                                                                .setTimestamp(Instant.now().toTimestamp())
                                                                .build())
                                                .build())
                                .build()
                )
                .setDirection(Direction.FIRST)
                .build()


        val createCheckRule = assertDoesNotThrow {
            ruleFactory.createCheckRule(request, false)
        }
        assertNotNull(createCheckRule) { "Rule cannot be null" }
    }

    private fun awaitEventBatchRequest(timeoutValue: Long = 1000L, times: Int): List<EventBatch> {
        val argumentCaptor = argumentCaptor<EventBatch>()
        verify(clientStub, timeout(timeoutValue).times(times)).send(argumentCaptor.capture())
        return argumentCaptor.allValues
    }

    private fun createStreams(alias: String = AbstractCheckTaskTest.SESSION_ALIAS, direction: Direction = Direction.FIRST, messages: List<Message>): Observable<StreamContainer> {
        return Observable.just(
                StreamContainer(SessionKey(alias, direction), messages.size + 1, Observable.fromIterable(messages))
        )
    }
}