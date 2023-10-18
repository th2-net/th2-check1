/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1.event;

import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.th2.check1.event.bean.CheckSequenceRow;
import com.exactpro.th2.common.grpc.FilterOperation;
import com.exactpro.th2.common.grpc.MessageFilter;
import com.exactpro.th2.common.grpc.MetadataFilter;
import com.exactpro.th2.common.grpc.MetadataFilter.SimpleFilter;
import com.exactpro.th2.common.grpc.RootMessageFilter;
import com.exactpro.th2.common.grpc.ValueFilter;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class CheckSequenceUtils {

    private String expectedMessage = "";
    private String actualMessage = "";

    /**
     * In the case when no actual message was found for the MessageFitler.
     */
    public static CheckSequenceRow createOnlyExpectedSide(RootMessageFilter filter, String connectivityId) {
        CheckSequenceRow row = new CheckSequenceRow();
        row.setActualMessage("");
        row.setActualMetadata("");
        row.setExpectedMessage(filter.getMessageType() + ", " + connectivityId + getKeyFields(filter.getMessageFilter()));
        row.setExpectedMetadata("Metadata, " + connectivityId + getKeyFields(filter.getMetadataFilter()));
        return row;
    }

    /**
     * In the case when actual message was found for the MessageFilter.
     */
    public static CheckSequenceRow createBothSide(IMessage actual, Map<String, String> properties, RootMessageFilter filter, String sessionAlias) {
        CheckSequenceRow row = new CheckSequenceRow();
        row.setActualMessage(actual.getName() + ", " + sessionAlias + getKeyFields(actual, filter.getMessageFilter()));
        row.setActualMetadata("Metadata, " + sessionAlias + getKeyFields(properties, filter.getMetadataFilter()));
        row.setExpectedMessage(filter.getMessageType() + ", " + sessionAlias + getKeyFields(filter.getMessageFilter()));
        row.setExpectedMetadata("Metadata, " + sessionAlias + getKeyFields(filter.getMetadataFilter()));
        return row;
    }

    /**
     * In the case when no MessageFilter was found for the actual message.
     */
    public static CheckSequenceRow createOnlyActualSide(IMessage actualMessage, String connectivityId) {
        CheckSequenceRow row = new CheckSequenceRow();
        row.setActualMessage(actualMessage.getName() + ", " + connectivityId);
        row.setActualMetadata("");
        row.setExpectedMessage("");
        row.setExpectedMetadata("");
        return row;
    }

    private static String getKeyFields(Map<String, String> properties, MetadataFilter metadataFilter) {
        StringBuilder builder = new StringBuilder();
        for (Entry<String, SimpleFilter> filters : metadataFilter.getPropertyFiltersMap().entrySet()) {
            String value = properties.get(filters.getKey());
            if (value == null) {
                continue;
            }
            if (filters.getValue().getKey()) {
                builder.append(", ").append(filters.getKey()).append('=').append(value);
            }
        }

        return builder.toString();
    }

    private static String getKeyFields(MetadataFilter metadataFilter) {
        StringBuilder builder = new StringBuilder();
        for (Entry<String, SimpleFilter> filters : metadataFilter.getPropertyFiltersMap().entrySet()) {
            builder.append(getKeyFields(filters.getKey(), filters.getValue()));
        }

        return builder.toString();
    }

    private static String getKeyFields(MessageFilter messageFilter) {
        StringBuilder result = new StringBuilder();
        messageFilter.getFieldsMap().forEach((name, filter) -> result.append(getKeyFields(name, filter)));
        return result.toString();
    }

    private static String getKeyFields(IMessage actual, MessageFilter messageFilter) {
        StringBuilder result = new StringBuilder();
        messageFilter.getFieldsMap().forEach((key, filter) -> result.append(getKeyFields(key, actual.getField(key), filter)));
        return result.toString();
    }

    private static <T> String getKeyFields(String name, T actual, ValueFilter filter) {
        if (actual == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        if (filter.hasMessageFilter()) {
            if (actual instanceof IMessage) {
                filter.getMessageFilter().getFieldsMap().forEach((childName, valFilter) -> result.append(getKeyFields(childName, ((IMessage) actual).getField(childName), valFilter)));
            }
        } else if (filter.hasListFilter()) {
            if (actual instanceof List) {
                List<ValueFilter> listFilter = filter.getListFilter().getValuesList();
                for (int i = 0; i < listFilter.size(); i++) {
                    if (((List<?>) actual).size() > i) {
                        result.append(getKeyFields(name, ((List<?>) actual).get(i), listFilter.get(i)));
                    }
                }
            }
        } else if (filter.getKey()) {
            result.append(", ").append(name).append('=').append(actual);
        }
        return result.toString();
    }

    private static String getKeyFields(String name, ValueFilter valueFilter) {
        StringBuilder result = new StringBuilder();
        if (valueFilter.hasMessageFilter()) {
            MessageFilter messageFilter = valueFilter.getMessageFilter();
            messageFilter.getFieldsMap().forEach((childName, filter) -> result.append(getKeyFields(childName, filter)));
        } else if (valueFilter.hasListFilter()) {
            valueFilter.getListFilter().getValuesList().forEach(filter -> result.append(getKeyFields(name, filter)));
        } else if (valueFilter.hasSimpleList() && (valueFilter.getOperation() == FilterOperation.IN || valueFilter.getOperation() == FilterOperation.NOT_IN)) {
            result.append(", ").append(name).append(' ').append(valueFilter.getOperation()).append(' ').append(valueFilter.getSimpleList().getSimpleValuesList());
        } else if (valueFilter.getKey()) {
            result.append(", ").append(name).append('=').append(valueFilter.getSimpleFilter());
        }
        return result.toString();
    }

    private static String getKeyFields(String name, SimpleFilter valueFilter) {
        if (valueFilter.getKey()) {
            if (valueFilter.hasSimpleList() && (valueFilter.getOperation() == FilterOperation.IN || valueFilter.getOperation() == FilterOperation.NOT_IN)) {
                return ", " + name + ' ' + valueFilter.getOperation() + ' ' + valueFilter.getSimpleList().getSimpleValuesList();
            }
            return ", " + name + '=' + valueFilter.getValue();
        }
        return "";
    }

    public String getExpectedMessage() {
        return expectedMessage;
    }

    public void setExpectedMessage(String expectedMessage) {
        this.expectedMessage = expectedMessage;
    }

    public String getActualMessage() {
        return actualMessage;
    }

    public void setActualMessage(String actualMessage) {
        this.actualMessage = actualMessage;
    }
}