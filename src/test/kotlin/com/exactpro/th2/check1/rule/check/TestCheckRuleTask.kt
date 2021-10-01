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

package com.exactpro.th2.check1.rule.check

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.util.toSimpleFilter
import com.exactpro.th2.common.event.bean.Verification
import com.exactpro.th2.common.event.bean.VerificationStatus
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.ListValueFilter
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageFilter
import com.exactpro.th2.common.message.rootMessageFilter
import com.exactpro.th2.common.value.add
import com.exactpro.th2.common.value.listValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.common.value.toValueFilter
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.protobuf.StringValue
import io.reactivex.Observable
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.lang.IllegalArgumentException
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

internal class TestCheckRuleTask : AbstractCheckTaskTest() {
    private fun checkTask(
        messageFilter: RootMessageFilter,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        maxEventBatchContentSize: Int = 1024 * 1024,
        timeout: Long = 1000
    ) = CheckRuleTask(
        SESSION_ALIAS,
        Instant.now(),
        SessionKey(SESSION_ALIAS, Direction.FIRST),
        timeout,
        maxEventBatchContentSize,
        messageFilter,
        parentEventID,
        messageStream,
        clientStub
    )

    @Test
    fun `success verification`() {
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
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL, true)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size)
        assertEquals(4, eventList.filter { it.status == SUCCESS }.size)
    }

    @Test
    internal fun `very little value of max event batch content size`() {
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
            .build()
        val task = checkTask(filter, eventID, streams, 1)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 1)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(1, eventList.size)
        assertEquals(EventStatus.FAILED, eventList[0].status)
    }

    @Test
    internal fun `exceeds max event batch content size`() {
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
            .build()
        val task = checkTask(filter, eventID, streams, 200)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 3)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size)
        assertEquals(2, eventList.filter { it.status == EventStatus.FAILED }.size) // Message filter and verification exceed max event batch content size
    }

    @Test
    internal fun findsMessageByMetadata() {
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
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatch.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size)
        assertTrue({
            eventList.none { it.status == EventStatus.FAILED }
        }) {
            "Some events are failed $eventBatch"
        }
    }

    @Test
    internal fun ignoresMessageIfMetadataDoesNotMatchByKeys() {
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
                .putPropertyFilters("keyProp", "43".toSimpleFilter(FilterOperation.EQUAL, key = true)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L, 2)

        val checkFailedEvent = eventBatch.flatMap(EventBatch::getEventsList).firstOrNull {
            it.status == EventStatus.FAILED && it.type == "Check failed"
        }
        assertNotNull(checkFailedEvent) {
            "No failed event $eventBatch"
        }
    }

    @ParameterizedTest(name = "timeout = {0}")
    @ValueSource(longs = [0, -1])
    fun `handle error if the timeout is zero or negative`(timeout: Long) {
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
                        .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL, true)))
                .build()

        val exception = assertThrows<IllegalArgumentException>("Task cannot be created due to invalid timeout") {
            checkTask(filter, eventID, streams, timeout = timeout)
        }
        assertEquals("'timeout' should be set or be greater than zero, actual: $timeout", exception.message)
    }


    @Test
    fun `check that the order is kept in repeating groups`() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
                message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                        .putFields("legs", listValue()
                                .add(message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                                        .putAllFields(mapOf(
                                                "A" to "1".toValue(),
                                                "B" to "1".toValue()
                                        )))
                                .add(message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                                        .putAllFields(mapOf(
                                                "A" to "2".toValue(),
                                                "B" to "2".toValue()
                                        )))
                                .toValue())
                        .build()
        ))

        val messageFilterForCheckOrder: RootMessageFilter = rootMessageFilter(MESSAGE_TYPE).apply {
            messageFilter = messageFilter().apply {
                putFields("legs", ValueFilter.newBuilder()
                        .setListFilter(ListValueFilter.newBuilder().apply {
                            addValues(ValueFilter.newBuilder().apply {
                                messageFilter = messageFilter().apply {
                                    putAllFields(mapOf(
                                            "A" to "2".toValueFilter(),
                                            "B" to "2".toValueFilter()
                                    ))
                                }.build()
                            }.build())
                            addValues(ValueFilter.newBuilder().apply {
                                messageFilter = messageFilter().apply {
                                    putAllFields(mapOf(
                                            "A" to "1".toValueFilter(),
                                            "B" to "3".toValueFilter()
                                    ))
                                }.build()
                            }.build())
                        }.build()).build())
            }.build()
        }.build()

        val eventID = EventID.newBuilder().setId("root").build()

        checkTask(messageFilterForCheckOrder, eventID, streams).begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(3, eventList.size)
        }, {
            val verificationEvent = eventList.find { it.type == "Verification" }
            assertNotNull(verificationEvent) { "Missed verification event" }

            val verification = jacksonObjectMapper().readValue<List<Verification>>(verificationEvent.body.toByteArray()).firstOrNull()
            assertNotNull(verification) { "Verification event does not contain the verification" }
            val actualLegs = verification.fields["legs"]?.fields?.values?.toList()
            assertNotNull(actualLegs) { "Actual legs is missed" }

            val expectedLegs = linkedMapOf(
                    0 to linkedMapOf(
                            "A" to VerificationStatus.PASSED,
                            "B" to VerificationStatus.FAILED
                    ),
                    1 to linkedMapOf(
                            "A" to VerificationStatus.PASSED,
                            "B" to VerificationStatus.PASSED
                    )
            )

            expectedLegs.forEach { (leg, verificationEntryByField) ->
                val actualLeg = actualLegs[leg].fields
                assertNotNull(actualLeg) { "The validation event does not contain the expected leg" }
                verificationEntryByField.forEach { (field, status) ->
                    val expectedVerificationEntry = actualLeg[field]
                    assertNotNull(expectedVerificationEntry) { "Actual leg does not contain the expected field" }
                    assertEquals(status, expectedVerificationEntry.status)
                }
            }
        })
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `check verification description`(includeDescription: Boolean) {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
                message(MESSAGE_TYPE, Direction.FIRST, SESSION_ALIAS)
                        .putFields("A", "1".toValue())
                        .build()
        ))
        val messageFilterForCheckOrder: RootMessageFilter = RootMessageFilter.newBuilder().apply {
            messageType = MESSAGE_TYPE
            messageFilter = messageFilter().putFields("A", "1".toValueFilter()).build()
            if (includeDescription) {
                description = StringValue.of(VERIFICATION_DESCRIPTION)
            }
        }.build()
        val eventID = EventID.newBuilder().setId("root").build()

        checkTask(messageFilterForCheckOrder, eventID, streams).begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(3, eventList.size)
        }, {
            val verificationEvent = eventList.find { it.type == "Verification" }
            assertNotNull(verificationEvent) { "Missed verification event" }
            assertEquals(includeDescription, verificationEvent.name.contains(VERIFICATION_DESCRIPTION))
        })
    }


    companion object {
        private const val VERIFICATION_DESCRIPTION = "Test verification with description"
    }
}