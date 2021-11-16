/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1.event;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import com.exactpro.sf.aml.scriptutil.StaticUtil.IFilter;
import com.exactpro.sf.comparison.ComparisonResult;
import com.exactpro.sf.comparison.Formatter;
import com.exactpro.sf.scriptrunner.StatusType;
import com.exactpro.th2.common.event.bean.VerificationEntry;
import com.exactpro.th2.common.event.bean.VerificationStatus;
import com.exactpro.th2.common.grpc.FilterOperation;
import com.exactpro.th2.sailfish.utils.filter.IOperationFilter;

public class VerificationEntryUtils {

    public static VerificationEntry createVerificationEntry(ComparisonResult result) {
        VerificationEntry verificationEntry = new VerificationEntry();
        verificationEntry.setActual(Objects.toString(result.getActual(), null));
        verificationEntry.setExpected(convertExpectedResult(result));
        verificationEntry.setStatus(toVerificationStatus(result.getStatus()));
        verificationEntry.setKey(result.isKey());
        verificationEntry.setOperation(resolveOperation(result));
        verificationEntry.setHint(extractHint(result));
        if (result.hasResults()) {
            verificationEntry.setFields(result.getResults().entrySet().stream()
                    .collect(Collectors.toMap(
                            Entry::getKey,
                            entry -> createVerificationEntry(entry.getValue()),
                            (entry1, entry2) -> entry1,
                            LinkedHashMap::new)));
            verificationEntry.setType("collection");
        } else {
            verificationEntry.setType("field");
        }

        return verificationEntry;
    }

    private static String resolveOperation(ComparisonResult result) {
        Object expected = result.getExpected();
        if (expected instanceof IFilter) {
            if (expected instanceof IOperationFilter) {
                return ((IOperationFilter)expected).getOperation().name();
            }
            String condition = ((IFilter)expected).getCondition();
            if (condition.contains("!=")) {
                return FilterOperation.NOT_EQUAL.name();
            }
            if ("*".equals(condition)) {
                return FilterOperation.NOT_EMPTY.name();
            }
            if ("#".equals(condition)) {
                return FilterOperation.EMPTY.name();
            }
        }
        return FilterOperation.EQUAL.name();
    }

    private static VerificationStatus toVerificationStatus(StatusType statusType) {
        if (statusType == null) {
            return null;
        }

        switch (statusType) {
        case NA:
            return VerificationStatus.NA;
        case FAILED:
            return VerificationStatus.FAILED;
        case PASSED:
            return VerificationStatus.PASSED;
        default:
            throw new IllegalArgumentException("Unsupported status type '" + statusType + '\'');
        }
    }

    private static String convertExpectedResult(ComparisonResult result) {
        return result.getExpected() == null ? null : Formatter.formatExpected(result);
    }

    private static String extractHint(ComparisonResult result) {
        Exception exception = result.getException();
        return exception == null ? null : exception.getMessage();
    }
}
