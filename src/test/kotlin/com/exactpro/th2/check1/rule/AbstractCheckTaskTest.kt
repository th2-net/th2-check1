/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

abstract class AbstractCheckTaskTest {
    protected val clientStub: MessageRouter<EventBatch> = spy { }
    protected val configuration = Check1Configuration()

    fun awaitEventBatchRequest(timeoutValue: Long = 1000L, times: Int): List<EventBatch> {
        val argumentCaptor = argumentCaptor<EventBatch>()
        verify(clientStub, timeout(timeoutValue).times(times)).send(argumentCaptor.capture())
        return argumentCaptor.allValues
    }

    fun createStreams(alias: String = SESSION_ALIAS, direction: Direction = FIRST, messages: List<Message>): Observable<StreamContainer> {
        return Observable.just(
            StreamContainer(SessionKey(alias, direction), messages.size + 1, Observable.fromIterable(messages))
        )
    }

    fun constructMessage(
        sequence: Long = 0,
        alias: String = SESSION_ALIAS,
        type: String = MESSAGE_TYPE,
        direction: Direction = FIRST,
        timestamp: Timestamp = Timestamp.getDefaultInstance()
    ): Message.Builder = Message.newBuilder().apply {
        metadataBuilder.apply {
            this.messageType = type
            this.timestamp = timestamp
            idBuilder.apply {
                this.sequence = sequence
                this.direction = direction
                connectionIdBuilder.sessionAlias = alias
            }
        }
    }

    protected fun createEvent(id: String): EventID {
        return requireNotNull(EventUtils.toEventID(id))
    }

    protected fun getMessageTimestamp(start: Instant, delta: Long): Timestamp =
        start.plusMillis(delta).toTimestamp()

    protected fun createCheckpoint(timestamp: Instant? = null, sequence: Long = -1) : Checkpoint =
        Checkpoint.newBuilder().apply {
            putSessionAliasToDirectionCheckpoint(
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
            )
        }.build()

    protected fun createPreFilter(fieldName: String, value: String, operation: FilterOperation): PreFilter =
        PreFilter.newBuilder()
            .putFields(fieldName, ValueFilter.newBuilder().setSimpleFilter(value).setKey(true).setOperation(operation).build())
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
            asserts: (VerificationEntry, VerificationEntry) -> Unit) {
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

    protected fun assertVerifications(
            expectedVerification: Verification,
            actualVerification: Verification,
            asserts: (VerificationEntry, VerificationEntry) -> Unit) =  assertVerificationEntries(expectedVerification.fields, actualVerification.fields, asserts)

    protected fun assertVerificationByStatus(verification: Verification, expectedVerificationEntries: Map<String, VerificationEntry>) {
        assertVerificationEntries(expectedVerificationEntries, verification.fields) { expected, actual ->
            assertEquals(expected.status, actual.status)
        }
    }

    protected fun createRuleConfiguration(taskTimeout: TaskTimeout, description: String = "Test", maxEventBatchContentSize: Int = 1024 * 1024): RuleConfiguration {
        return RuleConfiguration(
                taskTimeout,
                description,
                configuration.timePrecision,
                configuration.decimalPrecision,
                maxEventBatchContentSize,
                true,
                true
        )
    }


    @JsonSubTypes(value = [
        JsonSubTypes.Type(value = Verification::class, name = Verification.TYPE),
        JsonSubTypes.Type(value = com.exactpro.th2.common.event.bean.Message::class, name = com.exactpro.th2.common.event.bean.Message.TYPE)
    ])
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", include = JsonTypeInfo.As.PROPERTY, visible = true)
    private interface IBodyDataMixIn

    companion object {
        const val MESSAGE_TYPE = "TestMsg"
        const val SESSION_ALIAS = "test_session"
        const val VERIFICATION_TYPE = "Verification"
    }
}
