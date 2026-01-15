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
package com.exactpro.th2.check1.event.bean.builder;

import java.util.HashMap;
import java.util.Map;

import com.exactpro.sf.comparison.ComparisonResult;
import com.exactpro.th2.common.event.IBodyData;
import com.exactpro.th2.common.event.bean.Verification;
import com.exactpro.th2.common.event.bean.VerificationEntry;
import com.exactpro.th2.common.grpc.MessageFilter;
import com.exactpro.th2.common.grpc.MetadataFilter;

import static com.exactpro.th2.check1.event.VerificationEntryUtils.createVerificationEntry;

@SuppressWarnings("unused")
public class VerificationBuilder {

    public static final String VERIFICATION_TYPE = "verification";
    public static final String PASSED_STATUS = "PASSED";
    public static final String FAILED_STATUS = "FAILED";

    protected String status;
    protected final Map<String, VerificationEntry> fields = new HashMap<>();
    protected final boolean hideOperationInExpected;

    public VerificationBuilder(boolean hideOperationInExpected) {
        this.hideOperationInExpected = hideOperationInExpected;
    }

    public VerificationBuilder() {
        this(false);
    }

    public VerificationBuilder status(String status) {
        this.status = status;
        return this;
    }

    public VerificationBuilder verification(String fieldName, ComparisonResult comparisonResult, MessageFilter messageFilter, boolean listItemAsSeparate) {
        fields.put(fieldName, createVerificationEntry(comparisonResult, hideOperationInExpected));
        return this;
    }

    public VerificationBuilder verification(String fieldName, ComparisonResult comparisonResult, MetadataFilter messageFilter) {
        fields.put(fieldName, createVerificationEntry(comparisonResult, hideOperationInExpected));
        return this;
    }

    public IBodyData build() {
        Verification verification = new Verification();
        verification.setType(VERIFICATION_TYPE);
        verification.setStatus(status);
        verification.setFields(fields);
        return verification;
    }
}
