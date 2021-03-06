/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.Direction.FIRST
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.schema.message.MessageRouter
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.spy
import com.nhaarman.mockitokotlin2.timeout
import com.nhaarman.mockitokotlin2.verify
import io.reactivex.Observable

abstract class AbstractCheckTaskTest {
    protected val clientStub: MessageRouter<EventBatch> = spy { }

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

    fun constructMessage(sequence: Long = 0, alias: String = SESSION_ALIAS, type: String = MESSAGE_TYPE, direction: Direction = FIRST): Message.Builder = Message.newBuilder().apply {
        metadataBuilder.apply {
            messageType = type
            idBuilder.apply {
                this.sequence = sequence
                this.direction = direction
                connectionIdBuilder.sessionAlias = alias
            }
        }
    }

    companion object {
        const val MESSAGE_TYPE = "TestMsg"
        const val SESSION_ALIAS = "test_session"
        const val VERIFICATION_TYPE = "Verification"
    }
}
