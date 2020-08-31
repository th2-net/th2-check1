/******************************************************************************
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
 ******************************************************************************/
package com.exactpro.th2.verifier.event;

import java.util.List;

import com.exactpro.th2.infra.grpc.MessageFilter;
import com.exactpro.th2.infra.grpc.ValueFilter;
import com.exactpro.th2.verifier.CollectorService.Result;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.th2.verifier.event.bean.CheckSequenceRow;

public class CheckSequenceUtils {

    private String expectedMessage = "";
    private String actualMessage = "";

    /**
     * In the case when no actual message was found for the MessageFitler.
     */
    public static CheckSequenceRow createOnlyExpectedSide(MessageFilter messageFilter, String connectivityId) {
        CheckSequenceRow row = new CheckSequenceRow();
        row.setActualMessage("");
        row.setExpectedMessage(messageFilter.getMessageType() + ", " + connectivityId + getKeyFields(messageFilter));
        return row;
    }

    /**
     * In the case when actual message was found for the MessageFitler.
     */
    public static CheckSequenceRow createBothSide(Result result, String connectivityId) {
        CheckSequenceRow row = new CheckSequenceRow();
        row.setActualMessage(result.getActual().getName() + ", " + connectivityId + getKeyFields(result.getActual(), result.getMessageFilter()));
        row.setExpectedMessage(result.getMessageFilter().getMessageType() + ", " + connectivityId + getKeyFields(result.getMessageFilter()));
        return row;
    }

    /**
     * In the case when no MessageFilter was found for the actual message.
     */
    public static CheckSequenceRow createOnlyActualSide(IMessage actualMessage, String connectivityId) {
        CheckSequenceRow row = new CheckSequenceRow();
        row.setActualMessage(actualMessage.getName() + ", " + connectivityId);
        row.setExpectedMessage("");
        return row;
    }

    private static String getKeyFields(MessageFilter messageFilter) {
        StringBuilder result = new StringBuilder();
        messageFilter.getFieldsMap().forEach((name, filter) -> result.append(getKeyFields(name, filter)));
        return result.toString();
    }

    private static String getKeyFields(IMessage actual, MessageFilter messageFilter) {
        StringBuilder result = new StringBuilder();
        messageFilter.getFieldsMap().forEach((key, filter) -> {
            result.append(getKeyFields(key, actual.getField(key), filter));
        });
        return result.toString();
    }

    private static <T> String getKeyFields(String name, T actual, ValueFilter filter) {
        if (actual == null) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        if (filter.hasMessageFilter()) {
            if (actual instanceof IMessage) {
                filter.getMessageFilter().getFieldsMap().forEach((childName, valFilter) -> result.append(getKeyFields(childName, ((IMessage)actual).getField(childName), valFilter)));
            }
        } else if (filter.hasListFilter()) {
            if (actual instanceof List) {
                List<ValueFilter> listFilter = filter.getListFilter().getValuesList();
                for (int i = 0; i < listFilter.size(); i++) {
                    if (((List)actual).size() > i) {
                        result.append(getKeyFields(name, ((List)actual).get(i), listFilter.get(i)));
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
        } else if (valueFilter.getKey()) {
            result.append(", ").append(name).append('=').append(valueFilter.getSimpleFilter());
        }
        return result.toString();
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