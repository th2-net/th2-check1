/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.grpc.Message

class MessageContainer(
    val protoMessage: Message,
    val sailfishMessage: IMessage
) {
    val metadataMessage: IMessage by lazy {
        VerificationUtil.toMessage(protoMessage.metadata)
    }

    companion object {
        private val EMPTY_MESSAGE = DefaultMessageFactory.getFactory().createMessage("empty", "empty")
        @JvmField
        val FAKE = MessageContainer(Message.getDefaultInstance(), EMPTY_MESSAGE)
    }
}

class SailfishFilter(
    val message: IMessage,
    val comparatorSettings: ComparatorSettings
)

fun MessageContainer.isNotFake(): Boolean = this !== MessageContainer.FAKE