/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.verifier

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.services.ICSHIterator
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.infra.grpc.Message
import java.util.concurrent.atomic.AtomicReference

//TODO: rename
class SingleCSHIterator(private val converter: ProtoToIMessageConverter,
                        val protoMessage: Message) : ICSHIterator<IMessage> {
    private val single : AtomicReference<Message> = AtomicReference(protoMessage)

    override fun next(): IMessage =
        converter.fromProtoMessage(single.getAndSet(null), false)

    override fun updateCheckPoint() {
        TODO("Not yet implemented")
    }

    override fun hasNext(timeout: Long): Boolean = hasNext()

    override fun hasNext(): Boolean = single.get() != null
}