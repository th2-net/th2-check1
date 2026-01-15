/*
 * Copyright 2020-2026 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1.event.bean;

import com.exactpro.th2.common.event.bean.IRow;

public class CheckSequenceRow implements IRow {

    private String expectedMessage;
    private String expectedMetadata;
    private String actualMessage;
    private String actualMetadata;

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

    public String getExpectedMetadata() {
        return expectedMetadata;
    }

    public void setExpectedMetadata(String expectedMetadata) {
        this.expectedMetadata = expectedMetadata;
    }

    public String getActualMetadata() {
        return actualMetadata;
    }

    public void setActualMetadata(String actualMetadata) {
        this.actualMetadata = actualMetadata;
    }
}