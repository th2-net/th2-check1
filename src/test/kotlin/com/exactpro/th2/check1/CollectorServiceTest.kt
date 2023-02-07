package com.exactpro.th2.check1

import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.grpc.CheckRuleRequest
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
import kotlin.test.assertContains
import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import java.time.Instant
import java.time.temporal.ChronoUnit
import kotlin.system.measureTimeMillis

class CollectorServiceTest {
    private val eventRouterStub: MessageRouter<EventBatch> = mock {}
    private val messageRouterStub: MessageRouter<MessageBatch> = mock<MessageRouter<MessageBatch>> {}
        .apply { whenever(subscribeAll(any())).thenReturn(mock()) }

    private val collector = CollectorService(
        messageRouterStub,
        eventRouterStub,
        Check1Configuration(
            cleanupOlderThan = CLEANUP_OLDER_THAN_MILLIS,
            cleanupTimeUnit = ChronoUnit.MILLIS
        )
    )

    private fun createCheckRuleRequest(timeout: Long)  = CheckRuleRequest.newBuilder()
        .setParentEventId(EventID.newBuilder().setId("root").build())
        .setConnectivityId(ConnectionID.newBuilder()
            .setSessionAlias("test_alias")
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

    @Test
    fun `getRuleResult id not found`() {
        val result = collector.getRuleResult(Long.MAX_VALUE, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.NOT_FOUND, result)
    }

    @Test
    fun `getRuleResult timeout`() {
        val request = createCheckRuleRequest(1000)
        val (id, _) = collector.verifyCheckRule(request)

        val result: CollectorService.RuleResult
        val requestWaitMillis = measureTimeMillis {
            result = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO)
        }

        val expectedWaitMillis = REQUEST_TIMEOUT_NANO / 1_000_000
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

        Thread.sleep(300)
        collector.verifyCheckRule(request) // initiate cleanup
        val result1 = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.FAILED, result1) // result should be available

        Thread.sleep(700)
        collector.verifyCheckRule(request) // initiate cleanup
        val result2 = collector.getRuleResult(id, REQUEST_TIMEOUT_NANO)
        assertEquals(CollectorService.RuleResult.NOT_FOUND, result2) // result should be deleted
    }

    companion object {
        private const val REQUEST_TIMEOUT_NANO = 500_000_000L
        private const val CLEANUP_OLDER_THAN_MILLIS = 500L
    }
}