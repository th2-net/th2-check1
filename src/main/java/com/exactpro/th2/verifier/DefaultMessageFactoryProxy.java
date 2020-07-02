/******************************************************************************
 * Copyright 2009-2020 Exactpro (Exactpro Systems Limited)
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
 ******************************************************************************/
package com.exactpro.th2.verifier;

import com.exactpro.sf.common.impl.messages.DefaultMessageFactory;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.configuration.suri.SailfishURI;
import com.exactpro.sf.externalapi.IMessageFactoryProxy;

public class DefaultMessageFactoryProxy implements IMessageFactoryProxy {
    private static final String UNKNOWN_NAMESPACE = "unknown";
    @Override
    public IMessage createMessage(SailfishURI dictionary, String name) {
        return DefaultMessageFactory.getFactory().createMessage(name, UNKNOWN_NAMESPACE);
    }

    @Override
    public IMessage createStrictMessage(SailfishURI dictionary, String name) {
        return DefaultMessageFactory.getFactory().createMessage(name, UNKNOWN_NAMESPACE);
    }
}
