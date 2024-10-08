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
package com.exactpro.th2.check1.util;

import com.exactpro.sf.aml.script.MetaContainer;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.messages.IMessageFactory;
import com.exactpro.sf.comparison.ComparatorConst;
import com.exactpro.th2.check1.MessageFactory;
import com.exactpro.th2.common.grpc.FailUnexpected;
import com.exactpro.th2.common.grpc.ListValueFilter;
import com.exactpro.th2.common.grpc.MessageFilter;
import com.exactpro.th2.common.grpc.MetadataFilter;
import com.exactpro.th2.common.grpc.ValueFilter;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class VerificationUtil {
    public static final String METADATA_MESSAGE_NAME = "Th2MetadataMessage";
    public static final IMessageFactory FACTORY_PROXY = new MessageFactory();

    public static IMessage toSailfishMessage(Map<String, String> properties) {
        IMessage message = FACTORY_PROXY.createMessage(METADATA_MESSAGE_NAME, null);
        properties.forEach(message::addField);
        return message;
    }

    public static MetaContainer toMetaContainer(MetadataFilter metadataFilter) {
        MetaContainer metaContainer = new MetaContainer();
        Set<String> keyFields = new HashSet<>();

        metadataFilter.getPropertyFiltersMap().forEach((name, value) -> {
            if (value.getKey()) {
                keyFields.add(name);
            }
        });

        metaContainer.setKeyFields(keyFields);
        return metaContainer;
    }

    public static MetaContainer toMetaContainer(MessageFilter messageFilter, boolean listItemAsSeparate) {
        MetaContainer metaContainer = new MetaContainer();

        if (messageFilter.hasComparisonSettings()) {
            FailUnexpected failUnexpected = messageFilter.getComparisonSettings().getFailUnexpected();

            if (failUnexpected == FailUnexpected.FIELDS) {
                metaContainer.setFailUnexpected(ComparatorConst.YES);
            } else if (failUnexpected == FailUnexpected.FIELDS_AND_MESSAGES) {
                metaContainer.setFailUnexpected(ComparatorConst.ALL);
            }
        }

        Set<String> keyFields = new HashSet<>();

        messageFilter.getFieldsMap().forEach((name, value) ->
                toMetaContainer(name, value, metaContainer, keyFields, listItemAsSeparate));


        metaContainer.setKeyFields(keyFields);

        return metaContainer;
    }

    private static void toMetaContainer(
            String fieldName, ValueFilter value, MetaContainer parent,
            Set<String> keyFields, boolean listItemAsSeparate
    ) {
        if (value.hasMessageFilter()) {
            parent.add(fieldName, toMetaContainer(value.getMessageFilter(), listItemAsSeparate));
        } else if (value.hasListFilter() && containsMessageFilter(value.getListFilter())) {
            if (listItemAsSeparate) {
                convertListAsSeparateContainers(parent, fieldName, value.getListFilter());
            } else {
                convertList(parent, fieldName, value.getListFilter());
            }
        }
        if (value.getKey()) {
            keyFields.add(fieldName);
        }
    }

    private static void convertList(MetaContainer parent, String fieldName, ListValueFilter listFilter) {
        for (ValueFilter valueFilter : listFilter.getValuesList()) {
            if (valueFilter.hasMessageFilter()) {
                parent.add(fieldName, toMetaContainer(valueFilter.getMessageFilter(), false));
            }
        }
    }

    private static void convertListAsSeparateContainers(
            MetaContainer parent, String fieldName,
            ListValueFilter listFilter
    ) {
        MetaContainer result = new MetaContainer();
        int i = 0;
        for (ValueFilter valueFilter : listFilter.getValuesList()) {
            if (valueFilter.hasMessageFilter()) {
                result.add(String.valueOf(i++), toMetaContainer(valueFilter.getMessageFilter(), true));
            }
        }
        parent.add(fieldName, result);
    }

    private static boolean containsMessageFilter(ListValueFilter listFilter) {
        return listFilter.getValuesCount() > 0 && listFilter.getValues(0).hasMessageFilter();
    }
}
