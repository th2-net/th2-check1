/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.th2.check1.rule.AbstractCheckTask.Companion.CONVERTER
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.messageType

sealed interface MessageWrapper {

    val properties: Map<String, String>
    val id: MessageID
    val messageType: String

    fun toSailfishMessage(): IMessage

    fun toMetadataMessage(): IMessage
}

class TransportMessageWrapper : MessageWrapper {
    override val id: MessageID
        get() = TODO("Not yet implemented")
    override val messageType: String
        get() = TODO("Not yet implemented")
    override val properties: Map<String, String>
        get() = TODO("Not yet implemented")

    override fun toSailfishMessage(): IMessage {
        TODO("Not yet implemented")
    }

    override fun toMetadataMessage(): IMessage {
        TODO("Not yet implemented")
    }
}

class ProtoMessageWrapper(
    private val message: Message
) : MessageWrapper {
    override val id: MessageID = message.metadata.id
    override val messageType: String = message.messageType
    override val properties: Map<String, String> = message.metadata.propertiesMap

    //FIXME: move CONVERTER this this class
    override fun toSailfishMessage(): IMessage = CONVERTER.fromProtoMessage(message, false)
    override fun toMetadataMessage(): IMessage = VerificationUtil.toMessage(message.metadata)
}