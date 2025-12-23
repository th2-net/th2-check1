/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.comparison.ComparisonResult;
import com.exactpro.sf.comparison.ComparisonUtil;
import com.exactpro.sf.comparison.Formatter;
import com.exactpro.sf.comparison.IComparisonFilter;
import com.exactpro.sf.scriptrunner.StatusType;
import com.exactpro.th2.common.event.bean.VerificationEntry;
import com.exactpro.th2.common.event.bean.VerificationStatus;
import com.exactpro.th2.common.grpc.FilterOperation;
import com.exactpro.th2.sailfish.utils.filter.CompareFilter;
import com.exactpro.th2.sailfish.utils.filter.IOperationFilter;
import com.exactpro.th2.sailfish.utils.filter.ListContainFilter;
import com.exactpro.th2.sailfish.utils.filter.RegExFilter;
import com.exactpro.th2.sailfish.utils.filter.WildcardFilter;
import com.exactpro.th2.sailfish.utils.filter.util.FilterUtils;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.exactpro.sf.comparison.Formatter.formatForHtml;

public class VerificationEntryUtils {

    public static VerificationEntry createVerificationEntry(ComparisonResult result, boolean hideOperationInExpected) {
        VerificationEntry verificationEntry = new VerificationEntry();
        verificationEntry.setActual(convertActual(result));
        verificationEntry.setExpected(convertExpectedResult(result, hideOperationInExpected));
        verificationEntry.setStatus(toVerificationStatus(result));
        verificationEntry.setKey(result.isKey());
        verificationEntry.setOperation(resolveOperation(result));
        verificationEntry.setHint(extractHint(result));
        if (result.hasResults()) {
            verificationEntry.setFields(result.getResults().entrySet().stream()
                    .collect(Collectors.toMap(
                            Entry::getKey,
                            entry -> createVerificationEntry(entry.getValue(), hideOperationInExpected),
                            (entry1, entry2) -> entry1,
                            LinkedHashMap::new)));
            verificationEntry.setType("collection");
        } else {
            verificationEntry.setType("field");
        }

        return verificationEntry;
    }

    @Nullable
    private static String convertActual(ComparisonResult result) {
        Object actual = result.getActual();
        if (actual == FilterUtils.NULL_VALUE) {
            return null;
        }
        return Objects.toString(actual, null);
    }

    private static String resolveOperation(ComparisonResult result) {
        Object expected = result.getExpected();
        if (expected instanceof IComparisonFilter) {
            if (expected instanceof IOperationFilter) {
                return ((IOperationFilter) expected).getOperation().name();
            }
            String condition = ((IComparisonFilter) expected).getCondition();
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

    private static VerificationStatus toVerificationStatus(ComparisonResult result) {
        StatusType statusType = result.getStatus();
        if (statusType == null) {
            statusType = ComparisonUtil.getStatusType(result);
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

    public static VerificationEntry createVerificationEntry(ComparisonResult result) {
        return createVerificationEntry(result, false);
    }

    private static String convertExpectedResult(ComparisonResult result, boolean hideOperationInExpected) {
        if (result.getExpected() == null) {
            return null;
        }
        if (hideOperationInExpected) {
            // this code is modified copy of Formatter.formatExpected method
            Object expected = result.getExpected();

            if (expected instanceof IComparisonFilter) {
                if (expected.getClass().equals(RegExFilter.class)) {
                    return ((Pattern) ((RegExFilter) expected).getValue()).pattern();
                } else if (expected.getClass().equals(ListContainFilter.class)) {
                    return ((ListContainFilter) expected).getValue().toString();
                } else if (expected.getClass().equals(WildcardFilter.class)) {
                    return ((WildcardFilter) expected).getValue().toString();
                } else if (expected.getClass().equals(CompareFilter.class)) {
                    return ((CompareFilter) expected).getValue().toString();
                }
                return ((IComparisonFilter) expected).getCondition(result.getActual());
            }

            return formatForHtml(expected, true);
        }
        return Formatter.formatExpected(result);
    }

    private static String extractHint(ComparisonResult result) {
        Exception exception = result.getException();
        return exception == null ? null : exception.getMessage();
    }
}