/*
 * Copyright 2023-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1

import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest.Companion.BOOK_NAME
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest.Companion.SCOPE
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import kotlin.test.assertContains
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.any
import org.mockito.kotlin.mock
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.system.measureTimeMillis

class CollectorServiceTest {
    private val eventRouterStub: MessageRouter<EventBatch> = mock {}
    private val messageRouterStub: MessageRouter<MessageBatch> = mock {
        on { subscribeAll(any()) }.thenReturn(mock())
    }
    private val transportMessageRouterStub: MessageRouter<GroupBatch> = mock {
        on { subscribeAll(any()) }.thenReturn(mock())
    }

    private val collector = CollectorService(
        messageRouterStub,
        transportMessageRouterStub,
        eventRouterStub,
        Check1Configuration(
            cleanupOlderThan = CLEANUP_OLDER_THAN_MILLIS,
            cleanupTimeUnit = ChronoUnit.MILLIS,
            minCleanupIntervalMs = 0,
        )
    ) {_,_ -> true}

    private fun createCheckRuleRequest(timeout: Long)  = CheckRuleRequest.newBuilder()
        .setBookName(BOOK_NAME)
        .setParentEventId(EventID.newBuilder().setBookName(BOOK_NAME).setScope(SCOPE).setId("root").build())
        .setConnectivityId(ConnectionID.newBuilder()
            .setSessionAlias("test_alias")
            .setSessionGroup("test_alias")
        )
        .setRootFilter(RootMessageFilter.newBuilder()
            .setMessageType("TestMsgType")
        )
        .setMessageTimeout(5)
        .setTimeout(timeout)
        .setStoreResult(true)
        .setCheckpoint(
            Checkpoint.newBuilder()
                .setId(EventUtils.generateUUID())
                .putBookNameToSessionAliasToDirectionCheckpoint(
                    BOOK_NAME,
                    Checkpoint.SessionAliasToDirectionCheckpoint.newBuilder()
                        .putSessionAliasToDirectionCheckpoint(
                            "test_alias",
                            Checkpoint.DirectionCheckpoint.newBuilder()
                                .putDirectionToCheckpointData(
                                    Direction.FIRST.number,
                                    Checkpoint.CheckpointData.newBuilder()
                                        .setSequence(1)
                                        .setTimestamp(Instant.now().toTimestamp())
                                        .build())
                                .build()
                        ).build()
                )
                .build()
        )
        .setDirection(Direction.FIRST)
        .build()

    @Test
    fun `getRuleResult id not found`() {
        val result = collector.getRuleResult(Long.MAX_VALUE, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.NOT_FOUND, result)
    }

    @Test
    fun `getRuleResult timeout`() {
        val request = createCheckRuleRequest(500)
        val (id, _) = collector.verifyCheckRule(request)

        val shortTimeout = REQUEST_TIMEOUT_NANO / 10
        val result: CollectorService.RuleResult
        val requestWaitMillis = measureTimeMillis {
            result = collector.getRuleResult(id, shortTimeout)
        }

        val expectedWaitMillis = shortTimeout / 1_000_000
        assertContains(expectedWaitMillis..expectedWaitMillis + 10, requestWaitMillis)
        assertEquals(CollectorService.RuleResult.TIMEOUT, result)
    }

    @Test
    fun `getRuleResult result received`() {
        val request = createCheckRuleRequest(10)
        val (id, _) = collector.verifyCheckRule(request)
        val result = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.FAILED, result)
    }

    @Test
    fun `getRuleResult result cleanup`() {
        val request = createCheckRuleRequest(10)
        val (id, _) = collector.verifyCheckRule(request)

        val result1 = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO) // wait for result
        assertEquals(CollectorService.RuleResult.FAILED, result1) // result should be available

        Thread.sleep(CLEANUP_OLDER_THAN_MILLIS - 25)
        collector.verifyCheckRule(request) // initiate cleanup (before timeout expired)

        val result2 = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.FAILED, result2) // result should be still available

        Thread.sleep(30) // wait for timeout expiration
        collector.verifyCheckRule(request) // initiate cleanup (after timeout expired)
        val result3 = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.NOT_FOUND, result3) // result should be deleted
    }

    companion object {
        private const val REQUEST_TIMEOUT_NANO = 1_000_000_000L
        private const val CLEANUP_OLDER_THAN_MILLIS = 100L
    }
}