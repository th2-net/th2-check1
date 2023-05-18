/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.check1.MessageWrapper
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.entities.RuleConfiguration
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.IBodyData
import com.exactpro.th2.common.event.bean.Verification
import com.exactpro.th2.common.event.bean.VerificationEntry
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.toTimestamp
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction.INCOMING
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageId
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.protobuf.Timestamp
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.timeout
import com.nhaarman.mockitokotlin2.verify
import io.reactivex.Observable
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.Direction as TransportDirection

abstract class AbstractCheckTaskTest {
    protected val clientStub: MessageRouter<EventBatch> = spy { }
    protected val configuration = Check1Configuration()

    fun awaitEventBatchRequest(timeoutValue: Long = 1000L, times: Int): List<EventBatch> {
        val argumentCaptor = argumentCaptor<EventBatch>()
        verify(clientStub, timeout(timeoutValue).times(times)).send(argumentCaptor.capture())
        return argumentCaptor.allValues
    }

    fun createStreams(
        alias: String = SESSION_ALIAS,
        direction: Direction = FIRST,
        messages: List<MessageWrapper>
    ): Observable<StreamContainer> {
        return Observable.just(
            StreamContainer(
                SessionKey(BOOK_NAME, alias, direction),
                messages.size + 1,
                Observable.fromIterable(messages)
            )
        )
    }

    fun constructProtoMessage(
        sequence: Long = 1,
        alias: String = SESSION_ALIAS,
        type: String = MESSAGE_TYPE,
        direction: Direction = FIRST,
        timestamp: Timestamp = Instant.now().toTimestamp()
    ): Message.Builder = Message.newBuilder().apply {
        metadataBuilder.apply {
            this.messageType = type
            idBuilder.apply {
                this.sequence = sequence
                this.direction = direction
                connectionIdBuilder.sessionAlias = alias
                this.bookName = BOOK_NAME
                this.timestamp = timestamp
            }
        }
    }

    fun constructTransportMessage(
        sequence: Long = 1,
        alias: String = SESSION_ALIAS,
        type: String = MESSAGE_TYPE,
        direction: TransportDirection = INCOMING,
        timestamp: Instant = Instant.now()
    ): ParsedMessage = ParsedMessage().apply {
        this.type = type
        id = MessageId(
            alias,
            direction,
            sequence,
            timestamp = timestamp
        )
    }

    protected fun createRootEventId(): EventID {
        return createEventId(ROOT_ID)
    }

    protected fun createEventId(id: String): EventID {
        return requireNotNull(EventUtils.toEventID(Instant.now(), BOOK_NAME, id))
    }

    protected fun getProtoTimestamp(start: Instant, delta: Long): Timestamp = getTimestamp(start, delta).toTimestamp()

    protected fun getTimestamp(start: Instant, delta: Long): Instant = start.plusMillis(delta)

    protected fun createCheckpoint(timestamp: Instant? = null, sequence: Long = -1): Checkpoint =
        Checkpoint.newBuilder().apply {
            putBookNameToSessionAliasToDirectionCheckpoint(
                BOOK_NAME,
                Checkpoint.SessionAliasToDirectionCheckpoint
                    .newBuilder()
                    .putSessionAliasToDirectionCheckpoint(
                        SESSION_ALIAS,
                        Checkpoint.DirectionCheckpoint.newBuilder().apply {
                            putDirectionToCheckpointData(
                                FIRST.number,
                                Checkpoint.CheckpointData.newBuilder().apply {
                                    this.sequence = sequence
                                    if (timestamp != null) {
                                        this.timestamp = timestamp.toTimestamp()
                                    }
                                }.build()
                            )
                        }.build()
                    ).build()
            )
        }.build()

    protected fun createPreFilter(fieldName: String, value: String): PreFilter =
        PreFilter.newBuilder()
            .putFields(
                fieldName,
                ValueFilter.newBuilder().setSimpleFilter(value).setKey(true).setOperation(FilterOperation.EQUAL).build()
            )
            .build()

    protected fun List<Event>.findEventByType(eventType: String): Event? =
        this.find { it.type == eventType }

    protected fun extractEventBody(verificationEvent: Event): List<IBodyData> {
        return jacksonObjectMapper()
            .addMixIn(IBodyData::class.java, IBodyDataMixIn::class.java)
            .readValue(verificationEvent.body.toByteArray())
    }

    protected fun assertVerification(verificationEvent: Event): Verification {
        val verifications = extractEventBody(verificationEvent)
        val verification = verifications.filterIsInstance<Verification>().firstOrNull()
        assertNotNull(verification) { "Verification event does not contain the verification" }
        return verification
    }

    protected fun assertVerificationEntries(
        expectedVerificationEntries: Map<String, VerificationEntry>,
        actualVerificationEntries: Map<String, VerificationEntry>?,
        asserts: (VerificationEntry, VerificationEntry) -> Unit
    ) {
        assertNotNull(actualVerificationEntries) { "Actual verification entry is null" }
        expectedVerificationEntries.forEach { (expectedFieldName, expectedVerificationEntry) ->
            val actualVerificationEntry = actualVerificationEntries[expectedFieldName]
            assertNotNull(actualVerificationEntry) {
                "Actual verification entry does not contains field '${expectedFieldName}'"
            }
            asserts(expectedVerificationEntry, actualVerificationEntry)
            if (expectedVerificationEntry.fields == null) {
                return@forEach
            }
            assertVerificationEntries(expectedVerificationEntry.fields, actualVerificationEntry.fields, asserts)
        }
    }

    protected fun assertVerificationByStatus(
        verification: Verification,
        expectedVerificationEntries: Map<String, VerificationEntry>
    ) {
        assertVerificationEntries(expectedVerificationEntries, verification.fields) { expected, actual ->
            assertEquals(expected.status, actual.status)
        }
    }

    protected fun createRuleConfiguration(
        taskTimeout: TaskTimeout,
        description: String = "Test",
        maxEventBatchContentSize: Int = 1024 * 1024
    ): RuleConfiguration {
        return RuleConfiguration(
            taskTimeout,
            description,
            configuration.timePrecision,
            configuration.decimalPrecision,
            maxEventBatchContentSize,
            true
        )
    }


    @JsonSubTypes(
        value = [
            JsonSubTypes.Type(value = Verification::class, name = Verification.TYPE),
            JsonSubTypes.Type(
                value = com.exactpro.th2.common.event.bean.Message::class,
                name = com.exactpro.th2.common.event.bean.Message.TYPE
            )
        ]
    )
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", include = JsonTypeInfo.As.PROPERTY, visible = true)
    private interface IBodyDataMixIn

    companion object {
        const val MESSAGE_TYPE = "TestMsg"
        const val BOOK_NAME = "test_book"
        const val SESSION_ALIAS = "test_session"
        const val VERIFICATION_TYPE = "Verification"
        const val ROOT_ID = "root"
    }
}
