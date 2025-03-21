/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.common.utils.message.MessageHolder
import io.reactivex.rxjava3.core.Observable

class StreamContainer(
    val sessionKey: SessionKey,
    limitSize: Int,
    messageObservable: Observable<MessageHolder>
) {
    val bufferedMessages: Observable<MessageHolder>
    private val currentMessage: Observable<MessageHolder>

    init {
        val replay = messageObservable.replay(1)
        currentMessage = replay
        bufferedMessages = currentMessage.replay(limitSize).apply { connect() }

        // if we connect it before [bufferedMessages] stream is constructed we might lose some data
        replay.connect()
    }

    val lastMessage: MessageHolder
        get() = requireNotNull(currentMessage.firstElement().blockingGet()) {
            "'Last message' can't be null"
        }

    override fun equals(other: Any?): Boolean =
        if (other is StreamContainer) {
            sessionKey == other.sessionKey
        } else false

    override fun hashCode(): Int = sessionKey.hashCode()
}