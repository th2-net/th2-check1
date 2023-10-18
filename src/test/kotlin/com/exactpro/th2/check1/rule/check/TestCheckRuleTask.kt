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

package com.exactpro.th2.check1.rule.check

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.util.createDefaultMessage
import com.exactpro.th2.check1.rule.RuleFactory
import com.exactpro.th2.check1.util.createVerificationEntry
import com.exactpro.th2.check1.util.toPropertyFilter
import com.exactpro.th2.check1.util.toValueFilter
import com.exactpro.th2.common.event.bean.VerificationStatus
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.ListValue
import com.exactpro.th2.common.grpc.ListValueFilter
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootComparisonSettings
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageFilter
import com.exactpro.th2.common.message.rootMessageFilter
import com.exactpro.th2.common.utils.message.ProtoMessageHolder
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import com.exactpro.th2.common.value.add
import com.exactpro.th2.common.value.listValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.common.value.toValueFilter
import com.google.protobuf.BoolValue
import com.google.protobuf.StringValue
import com.google.protobuf.TextFormat
import io.reactivex.Observable
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource
import java.time.Instant
import java.util.stream.Stream
import java.util.concurrent.Executors
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

internal class TestCheckRuleTask : AbstractCheckTaskTest() {
    private fun checkTask(
        messageFilter: RootMessageFilter,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        maxEventBatchContentSize: Int = 1024 * 1024,
        taskTimeout: TaskTimeout = TaskTimeout(1000L)
    ) = CheckRuleTask(
        createRuleConfiguration(taskTimeout, SESSION_ALIAS, maxEventBatchContentSize),
        Instant.now(),
        SessionKey(BOOK_NAME, SESSION_ALIAS, Direction.FIRST),
        messageFilter,
        parentEventID,
        messageStream,
        clientStub
    )

    @Test
    fun `success verification several rules on same executor`() {
        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            ProtoMessageHolder(
                createDefaultMessage()
                    .mergeMetadata(MessageMetadata.newBuilder()
                        .putProperties("keyProp", "42")
                        .putProperties("notKeyProp", "2")
                        .build())
                    .build()
            )
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true)))
            .build()
        val executor = Executors.newSingleThreadExecutor()

        checkTask(filter, eventID, streams).apply { begin(executorService = executor) }
        checkTask(filter, eventID, streams).apply { begin(executorService = executor) }

        val eventBatches = awaitEventBatchRequest(1000L, 4)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        val eventsPerRule = 5
        assertEquals(2 * eventsPerRule, eventList.size)
        assertEquals(2 * eventsPerRule, eventList.filter { it.status == SUCCESS }.size)
    }

    @Test
    fun `success verification`() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
            )
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(5, eventList.size)
        assertEquals(5, eventList.filter { it.status == SUCCESS }.size)
    }

    @Test
    fun `success transport verification`() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                TransportMessageHolder(
                    constructTransportMessage()
                        .setMetadata(
                            hashMapOf(
                                "keyProp" to "42",
                                "notKeyProp" to "abc"
                            )
                        ).setBody(
                            hashMapOf(
                                "keyFieldInt" to 42,
                                "keyFieldFloat" to 123456789.12345,
                                "notKeyField" to "abc"
                            )
                        ).build(),
                    BOOK_NAME,
                    ""
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder().apply {
            messageType = MESSAGE_TYPE
            metadataFilterBuilder.apply {
                putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
            }
            messageFilterBuilder.apply {
                putFields("keyFieldInt", "42".toValueFilter(FilterOperation.EQUAL, true))
                putFields("keyFieldFloat", "123456789.12345".toValueFilter(FilterOperation.EQUAL, true))
            }
        }.build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(4, eventList.size, "Incorrect number of events $eventList")
        assertEquals(4, eventList.filter { it.status == SUCCESS }.size)
        assertEquals("Check rule - test_session", eventList[0].name)
        assertEquals("Message filter", eventList[1].name)
        assertEquals("Verification '$MESSAGE_TYPE' message", eventList[2].name)
        assertEquals("Verification '$MESSAGE_TYPE' metadata", eventList[3].name)
    }

    @Test
    internal fun `very little value of max event batch content size`() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL))
            )
            .build()
        val task = checkTask(filter, eventID, streams, 1)
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 1)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(1, eventList.size)
        assertEquals(FAILED, eventList[0].status)
    }

    @Test
    internal fun `exceeds max event batch content size`() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .putProperties(
                                    "notKeyProp2",
                                    "2"
                                ) // for excess max event batch content size and have 2 failed events
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL))
            )
            .build()
        val task = checkTask(
            filter,
            eventID,
            streams,
            200 + TextFormat.shortDebugString(eventID).length // EventID now contains book_name, scope and start_timestamp
        )
        task.begin()

        val eventBatches = awaitEventBatchRequest(1000L, 4)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(5, eventList.size)
        assertEquals(
            2,
            eventList.filter { it.status == FAILED }.size
        ) // Message filter and verification exceed max event batch content size
    }

    @Test
    internal fun findsMessageByMetadata() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL))
            )
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatch.flatMap(EventBatch::getEventsList)
        assertEquals(5, eventList.size)
        assertTrue({
            eventList.none { it.status == FAILED }
        }) {
            "Some events are failed $eventBatch"
        }
    }

    @Test
    internal fun ignoresMessageIfMetadataDoesNotMatchByKeys() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "43".toPropertyFilter(FilterOperation.EQUAL, key = true))
            )
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L, 2)

        val checkFailedEvent = eventBatch.flatMap(EventBatch::getEventsList).firstOrNull {
            it.status == FAILED && it.type == "Check failed"
        }
        assertNotNull(checkFailedEvent) {
            "No failed event $eventBatch"
        }
    }

    @ParameterizedTest(name = "timeout = {0}")
    @ValueSource(longs = [0, -1])
    fun `handle error if the timeout is zero or negative`(timeout: Long) {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
            )
            .build()

        val exception = assertThrows<IllegalArgumentException>("Task cannot be created due to invalid timeout") {
            checkTask(filter, eventID, streams, taskTimeout = TaskTimeout(timeout))
        }
        assertEquals("'timeout' should be set or be greater than zero, actual: $timeout", exception.message)
    }

    @Test
    fun `check that the order is kept in repeating groups`() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .putFields(
                            "legs", listValue()
                                .add(
                                    createDefaultMessage()
                                        .putAllFields(
                                            mapOf(
                                                "A" to "1".toValue(),
                                                "B" to "1".toValue()
                                            )
                                        )
                                )
                                .add(
                                    createDefaultMessage()
                                        .putAllFields(
                                            mapOf(
                                                "A" to "2".toValue(),
                                                "B" to "2".toValue()
                                            )
                                        )
                                ).toValue()
                        ).build()
                )
            )
        )

        val messageFilterForCheckOrder: RootMessageFilter = rootMessageFilter(MESSAGE_TYPE).apply {
            messageFilter = messageFilter().apply {
                putFields(
                    "legs", ValueFilter.newBuilder()
                        .setListFilter(ListValueFilter.newBuilder().apply {
                            addValues(ValueFilter.newBuilder().apply {
                                messageFilter = messageFilter().apply {
                                    putAllFields(
                                        mapOf(
                                            "A" to "2".toValueFilter(),
                                            "B" to "2".toValueFilter()
                                        )
                                    )
                                }.build()
                            }.build())
                            addValues(ValueFilter.newBuilder().apply {
                                messageFilter = messageFilter().apply {
                                    putAllFields(
                                        mapOf(
                                            "A" to "1".toValueFilter(),
                                            "B" to "3".toValueFilter()
                                        )
                                    )
                                }.build()
                            }.build())
                        }.build()).build()
                )
            }.build()
        }.build()

        val eventID = createRootEventId()

        checkTask(messageFilterForCheckOrder, eventID, streams).begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(4, eventList.size)
        }, {
            val verificationEvent = eventList.find { it.type == "Verification" }
            assertNotNull(verificationEvent) { "Missed verification event" }
            val verification = assertVerification(verificationEvent)

            val expectedLegs = mapOf(
                "legs" to createVerificationEntry(
                    "0" to createVerificationEntry(
                        "A" to createVerificationEntry(VerificationStatus.PASSED),
                        "B" to createVerificationEntry(VerificationStatus.FAILED),
                        status = VerificationStatus.FAILED,
                    ),
                    "1" to createVerificationEntry(
                        "A" to createVerificationEntry(VerificationStatus.PASSED),
                        "B" to createVerificationEntry(VerificationStatus.PASSED),
                        status = VerificationStatus.PASSED,
                    ),
                    status = VerificationStatus.FAILED,
                )
            )

            assertVerificationByStatus(verification, expectedLegs)
        })
    }

    @ParameterizedTest
    @MethodSource("argsForSimpleCollectionOrderCheckingConfig")
    fun `simple collection order checking config`(requestValue: Boolean?, defaultValue: Boolean) {
        val expectedComparisonResult = !(requestValue ?: defaultValue)

        val streams = createStreams(SESSION_ALIAS, Direction.FIRST, listOf(
            ProtoMessageHolder(
                createDefaultMessage()
                    .putFields("simple_list", ListValue.newBuilder().addAllValues(
                        listOf("C".toValue(), "A".toValue(), "B".toValue())
                    ).build().toValue())
                    .build()
            )
        ))

        val messageFilterForCheckOrderBuilder = rootMessageFilter(MESSAGE_TYPE).apply {
            messageFilter = messageFilter().apply {
                putFields(
                    "simple_list",
                    ValueFilter.newBuilder().setListFilter(
                        ListValueFilter.newBuilder().
                            addAllValues(listOf(
                                "A".toValueFilter(),
                                "B".toValueFilter(),
                                "C".toValueFilter()
                            ))
                    ).build()
                ).build()
            }.build()
        }

        if (requestValue != null) {
            messageFilterForCheckOrderBuilder.setComparisonSettings(
                RootComparisonSettings.newBuilder().setCheckSimpleCollectionsOrder(BoolValue.newBuilder().setValue(requestValue))
            )
        }

        val config = Check1Configuration(isDefaultCheckSimpleCollectionsOrder = defaultValue)
        val ruleFactory = RuleFactory(config, streams, clientStub)
        val request = CheckRuleRequest.newBuilder()
            .setParentEventId(createEventId("root"))
            .setConnectivityId(ConnectionID.newBuilder().setSessionAlias(SESSION_ALIAS))
            .setRootFilter(messageFilterForCheckOrderBuilder)
            .setMessageTimeout(5)
            .setChainId(ChainID.newBuilder().setId("test_chain_id"))
            .build()

        ruleFactory.createCheckRule(request, true).begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(4, eventList.size)
        }, {
            val verificationEvent = eventList.find { it.type == "Verification" }
            assertNotNull(verificationEvent) { "Missed verification event" }
            val verification = assertVerification(verificationEvent)

            val verificationEntry = createVerificationEntry(if (expectedComparisonResult) VerificationStatus.PASSED else VerificationStatus.FAILED)

            val expected = mapOf("simple_list" to createVerificationEntry(
                "0" to verificationEntry,
                "1" to verificationEntry,
                "2" to verificationEntry,
            ))

            assertVerificationByStatus(verification, expected)
        })
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `check verification description`(includeDescription: Boolean) {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .putFields("A", "1".toValue())
                        .build()
                )
            )
        )
        val messageFilterForCheckOrder: RootMessageFilter = RootMessageFilter.newBuilder().apply {
            messageType = MESSAGE_TYPE
            messageFilter = messageFilter().putFields("A", "1".toValueFilter()).build()
            if (includeDescription) {
                description = StringValue.of(VERIFICATION_DESCRIPTION)
            }
        }.build()
        val eventID = createRootEventId()

        checkTask(messageFilterForCheckOrder, eventID, streams).begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(4, eventList.size)
        }, {
            val verificationEvent = eventList.find { it.type == "Verification" }
            assertNotNull(verificationEvent) { "Missed verification event" }
            assertEquals(includeDescription, verificationEvent.name.contains(VERIFICATION_DESCRIPTION))
        })
    }

    @Test
    fun `success verification with message timeout`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .setId(
                                    MessageID.newBuilder().setSequence(1L)
                                        .setTimestamp(getProtoTimestamp(checkpointTimestamp, 500))
                                )
                                .build()
                        )
                        .build()
                ),
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .setId(
                                    MessageID.newBuilder().setSequence(2L)
                                        .setTimestamp(getProtoTimestamp(checkpointTimestamp, 500))
                                )
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
            )
            .build()
        val task = checkTask(filter, eventID, streams, taskTimeout = TaskTimeout(1000, 500))
        task.begin(createCheckpoint(checkpointTimestamp, 1))

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(5, eventList.size)
        assertEquals(5, eventList.filter { it.status == SUCCESS }.size)
    }

    @Test
    fun `success verification, but failed event because missed start message`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .setId(
                                    MessageID.newBuilder().setSequence(1L)
                                        .setTimestamp(getProtoTimestamp(checkpointTimestamp, 500))
                                )
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
            )
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin(createCheckpoint(sequence = 0))

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(6, eventList.size)
        assertEquals(2, eventList.filter { it.status == SUCCESS && it.type == "Verification" }.size)
        assertEquals(FAILED, eventList.last().status)
    }

    @Test
    fun `failed verification with expired message timeout`() {
        val checkpointTimestamp = Instant.now()
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .setId(
                                    MessageID.newBuilder().setSequence(1L)
                                        .setTimestamp(getProtoTimestamp(checkpointTimestamp, 600))
                                )
                                .build()
                        )
                        .build()
                ),
                ProtoMessageHolder(
                    createDefaultMessage()
                        .mergeMetadata(
                            MessageMetadata.newBuilder()
                                .putProperties("keyProp", "42")
                                .putProperties("notKeyProp", "2")
                                .setId(
                                    MessageID.newBuilder().setSequence(2L)
                                        .setTimestamp(getProtoTimestamp(checkpointTimestamp, 600))
                                )
                                .build()
                        )
                        .build()
                )
            )
        )

        val eventID = createRootEventId()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType(MESSAGE_TYPE)
            .setMetadataFilter(
                MetadataFilter.newBuilder()
                    .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
            )
            .build()
        val task = checkTask(filter, eventID, streams, taskTimeout = TaskTimeout(1000, 500))
        task.begin(createCheckpoint(checkpointTimestamp, 1))

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertEquals(5, eventList.size)
        assertEquals(3, eventList.filter { it.status == FAILED }.size)
    }

    @Test
    fun `verify repeating groups according to defined filters`() {
        val streams = createStreams(
            SESSION_ALIAS, Direction.FIRST, listOf(
                ProtoMessageHolder(
                    createDefaultMessage()
                        .putFields(
                            "legs", listValue()
                                .add(
                                    createDefaultMessage()
                                        .putAllFields(
                                            mapOf(
                                                "A" to "1".toValue(),
                                                "B" to "2".toValue()
                                            )
                                        )
                                )
                                .add(
                                    createDefaultMessage()
                                        .putAllFields(
                                            mapOf(
                                                "C" to "3".toValue(),
                                                "D" to "4".toValue()
                                            )
                                        )
                                )
                                .toValue()
                        )
                        .build()
                )
            )
        )

        val messageFilterForCheckOrder: RootMessageFilter = rootMessageFilter(MESSAGE_TYPE).apply {
            comparisonSettings = RootComparisonSettings.newBuilder().apply {
                checkRepeatingGroupOrder = true
            }.build()
            messageFilter = messageFilter().apply {
                putFields(
                    "legs", ValueFilter.newBuilder()
                        .setListFilter(ListValueFilter.newBuilder().apply {
                            addValues(ValueFilter.newBuilder().apply {
                                messageFilter = messageFilter().apply {
                                    putAllFields(
                                        mapOf(
                                            "C" to "3".toValueFilter(),
                                            "D" to "4".toValueFilter()
                                        )
                                    )
                                }.build()
                            }.build())
                            addValues(ValueFilter.newBuilder().apply {
                                messageFilter = messageFilter().apply {
                                    putAllFields(
                                        mapOf(
                                            "A" to "1".toValueFilter(),
                                            "B" to "2".toValueFilter()
                                        )
                                    )
                                }.build()
                            }.build())
                        }.build()).build()
                )
            }.build()
        }.build()

        val eventID = createRootEventId()

        checkTask(messageFilterForCheckOrder, eventID, streams).begin()

        val eventBatches = awaitEventBatchRequest(1000L, 2)
        val eventList = eventBatches.flatMap(EventBatch::getEventsList)
        assertAll({
            assertEquals(4, eventList.size)
        }, {
            val verificationEvent = eventList.find { it.type == "Verification" }
            assertNotNull(verificationEvent) { "Missed verification event" }

            val verification = assertVerification(verificationEvent)

            val expectedLegs = mapOf(
                "legs" to createVerificationEntry(
                    "0" to createVerificationEntry(
                        "A" to createVerificationEntry(VerificationStatus.NA),
                        "B" to createVerificationEntry(VerificationStatus.NA),
                        "C" to createVerificationEntry(VerificationStatus.FAILED),
                        "D" to createVerificationEntry(VerificationStatus.FAILED),
                        status = VerificationStatus.FAILED,
                    ),
                    "1" to createVerificationEntry(
                        "C" to createVerificationEntry(VerificationStatus.NA),
                        "D" to createVerificationEntry(VerificationStatus.NA),
                        "A" to createVerificationEntry(VerificationStatus.FAILED),
                        "B" to createVerificationEntry(VerificationStatus.FAILED),
                        status = VerificationStatus.FAILED,
                    ),
                    status = VerificationStatus.FAILED,
                )
            )

            assertVerificationByStatus(verification, expectedLegs)
        })
    }

    companion object {
        private const val VERIFICATION_DESCRIPTION = "Test verification with description"

        @JvmStatic
        fun argsForSimpleCollectionOrderCheckingConfig(): Stream<Arguments> = Stream.of(
            Arguments.of(true, true),
            Arguments.of(true, false),
            Arguments.of(false, true),
            Arguments.of(false, true),
            Arguments.of(null, true),
            Arguments.of(null, false)
        )
    }
}
