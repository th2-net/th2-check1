/*
 * Copyright 2021-2022 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.exactpro.th2.check1.rule

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.exception.RuleCreationException
import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.util.assertThrowsWithMessages
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
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class RuleFactoryTest {
    private val clientStub: MessageRouter<EventBatch> = spy { }

    @Test
    fun `failed rule creation because session alias is empty`() {
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

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "Session alias cannot be empty"
        ) { ruleFactory.createCheckRule(request, true) }

        assertEvents()
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

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "The request has an invalid chain ID or connectivity ID. Please use checkpoint instead of chain ID"
        ) { ruleFactory.createCheckRule(request, false) }

        assertEvents()
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

    @Test
    fun `failed rule creation because direction checkpoint is missed`() {
        val sessionAlias = "diff_test_alias"
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
                        .setSessionAlias(sessionAlias)
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

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "The checkpoint doesn't contain a direction checkpoint with session alias '$sessionAlias'"
        ) { ruleFactory.createCheckRule(request, true) }

        assertEvents()
    }

    @Test
    fun `failed rule creation because checkpoint is missed`() {
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
                .setDirection(Direction.FIRST)
                .build()

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "Request must contain a checkpoint, because the 'messageTimeout' is used and no chain ID is specified"
        ) { ruleFactory.createCheckRule(request, true) }

        assertEvents()
    }

    @Test
    fun `failed rule creation because checkpoint data is missed`() {
        val sessionAlias = "test_alias"
        val direction = Direction.SECOND
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
                        .setSessionAlias(sessionAlias)
                )
                .setRootFilter(RootMessageFilter.newBuilder()
                        .setMessageType("TestMsgType")
                )
                .setMessageTimeout(5)
                .setCheckpoint(
                        Checkpoint.newBuilder()
                                .setId(EventUtils.generateUUID())
                                .putSessionAliasToDirectionCheckpoint(
                                        sessionAlias,
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
                .setDirection(direction)
                .build()

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "The direction checkpoint doesn't contain a checkpoint data with direction '$direction'"
        ) { ruleFactory.createCheckRule(request, true) }

        assertEvents()
    }

    @Test
    fun `failed rule creation because checkpoint data has incorrect sequence number`() {
        val sessionAlias = "test_alias"
        val sequence: Long = -1
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
                        .setSessionAlias(sessionAlias)
                )
                .setRootFilter(RootMessageFilter.newBuilder()
                        .setMessageType("TestMsgType")
                )
                .setMessageTimeout(5)
                .setCheckpoint(
                        Checkpoint.newBuilder()
                                .setId(EventUtils.generateUUID())
                                .putSessionAliasToDirectionCheckpoint(
                                        sessionAlias,
                                        Checkpoint.DirectionCheckpoint.newBuilder()
                                                .putDirectionToCheckpointData(
                                                        Direction.FIRST.number,
                                                        Checkpoint.CheckpointData.newBuilder()
                                                                .setSequence(sequence)
                                                                .setTimestamp(Instant.now().toTimestamp())
                                                                .build())
                                                .build())
                                .build()
                )
                .setDirection(Direction.FIRST)
                .build()

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "The checkpoint data has incorrect sequence number '$sequence'"
        ) { ruleFactory.createCheckRule(request, true) }

        assertEvents()
    }

    @Test
    fun `failed rule creation because checkpoint data missed timestamp`() {
        val sessionAlias = "test_alias"
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
                        .setSessionAlias(sessionAlias)
                )
                .setRootFilter(RootMessageFilter.newBuilder()
                        .setMessageType("TestMsgType")
                )
                .setMessageTimeout(5)
                .setCheckpoint(
                        Checkpoint.newBuilder()
                                .setId(EventUtils.generateUUID())
                                .putSessionAliasToDirectionCheckpoint(
                                        sessionAlias,
                                        Checkpoint.DirectionCheckpoint.newBuilder()
                                                .putDirectionToCheckpointData(
                                                        Direction.FIRST.number,
                                                        Checkpoint.CheckpointData.newBuilder()
                                                                .setSequence(1)
                                                                .build())
                                                .build())
                                .build()
                )
                .setDirection(Direction.FIRST)
                .build()

        assertThrowsWithMessages<RuleCreationException>(
                "An error occurred while creating rule",
                "The checkpoint data doesn't contain timestamp"
        ) { ruleFactory.createCheckRule(request, true) }

        assertEvents()
    }

    private fun assertEvents() {
        val eventBatches = awaitEventBatchRequest(1000L, 1)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(2, eventList.size)
            assertEquals(1, eventList.filter { it.type == "ruleCreationException" }.size)
        })
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