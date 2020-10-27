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
package com.exactpro.th2.check1.util;

import com.exactpro.sf.aml.script.MetaContainer;
import com.exactpro.th2.common.grpc.ListValueFilter;
import com.exactpro.th2.common.grpc.MessageFilter;
import com.exactpro.th2.common.grpc.ValueFilter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VerificationUtil {

    public static MetaContainer toMetaContainer(MessageFilter messageFilter, boolean listItemAsSeparate) {
        MetaContainer metaContainer = new MetaContainer();
        Set<String> keyFields = new HashSet<>();
        for (Map.Entry<String, ValueFilter> filterEntry : messageFilter.getFieldsMap().entrySet()) {
            toMetaContainer(filterEntry.getKey(), filterEntry.getValue(), metaContainer, keyFields,
                    listItemAsSeparate);
        }
        metaContainer.setKeyFields(keyFields);
        return metaContainer;
    }

    private static void toMetaContainer(String fieldName, ValueFilter value, MetaContainer parent,
                                        Set<String> keyFields, boolean listItemAsSeparate) {
        if (value.hasMessageFilter()) {
            parent.add(fieldName, toMetaContainer(value.getMessageFilter(), listItemAsSeparate));
        } else if (value.hasListFilter() && value.getListFilter().getValues(0).hasMessageFilter()) {
            if (listItemAsSeparate) {
                convertListAsSeparateContainers(parent, fieldName, value.getListFilter());
            } else {
                convertList(parent, fieldName, value.getListFilter());
            }
        } else if (value.getKey()) {
            keyFields.add(fieldName);
        }
    }

    private static void convertList(MetaContainer parent, String fieldName, ListValueFilter listFilter) {
        List<MetaContainer> result = new ArrayList<>();
        for (ValueFilter valueFilter : listFilter.getValuesList()) {
            if (valueFilter.hasMessageFilter()) {
                result.add(toMetaContainer(valueFilter.getMessageFilter(), false));
            }
        }
        parent.getChildren().put(fieldName, result);
    }

    private static void convertListAsSeparateContainers(MetaContainer parent, String fieldName,
                                                        ListValueFilter listFilter) {
        MetaContainer result = new MetaContainer();
        int i = 0;
        for (ValueFilter valueFilter : listFilter.getValuesList()) {
            if (valueFilter.hasMessageFilter()) {
                result.add(String.valueOf(i++), toMetaContainer(valueFilter.getMessageFilter(), true));
            }
        }
        parent.add(fieldName, result);
    }
}
