/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1;

import com.exactpro.sf.common.impl.messages.AbstractMessageFactory;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.MsgMetaData;

public class MessageFactory extends AbstractMessageFactory {
    private static final String UNKNOWN_NAMESPACE = "unknown";

    @Override
    public IMessage createMessage(MsgMetaData metadata) {
        return super.createMessage(metadata);
    }

    @Override
    public IMessage createMessage(long id, String name, String namespace) {
        return super.createMessage(id, name, UNKNOWN_NAMESPACE);
    }

    @Override
    public IMessage createMessage(String name, String namespace) {
        return super.createMessage(name, UNKNOWN_NAMESPACE);
    }

    @Override
    public IMessage createMessage(String name) {
        return super.createMessage(name, UNKNOWN_NAMESPACE);
    }

    @Override
    public String getProtocol() {
        return null;
    }
}
