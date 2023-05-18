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
import com.exactpro.th2.check1.rule.AbstractCheckTask.Companion.PROTO_CONVERTER
import com.exactpro.th2.check1.rule.AbstractCheckTask.Companion.TRANSPORT_CONVERTER
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.message.messageType
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.toProto

sealed interface MessageWrapper {
    val id: MessageID
    val messageType: String
    val properties: Map<String, String>

    val message: IMessage
    val metadata: IMessage
}

class TransportMessageWrapper(
    private val transport: ParsedMessage,
    private val book: String,
    private val sessionGroup: String
) : MessageWrapper {
    override val id: MessageID by lazy { this.transport.id.toProto(book, sessionGroup) }
    override val messageType: String
        get() = this.transport.type
    override val properties: Map<String, String>
        get() = this.transport.metadata

    override val message: IMessage by lazy { TRANSPORT_CONVERTER.fromTransport(book, sessionGroup, transport, false) }
    override val metadata: IMessage by lazy { VerificationUtil.toSailfishMessage(this.transport.metadata) }
}

class ProtoMessageWrapper(
    private val proto: Message
) : MessageWrapper {
    override val id: MessageID
        get() = this.proto.metadata.id
    override val messageType: String
        get() = this.proto.messageType
    override val properties: Map<String, String> = this.proto.metadata.propertiesMap

    override val message: IMessage by lazy { PROTO_CONVERTER.fromProtoMessage(this.proto, false) }
    override val metadata: IMessage by lazy { VerificationUtil.toSailfishMessage(properties) }
}