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

package com.exactpro.th2.check1.event

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.event.bean.VerificationEntry
import com.exactpro.th2.common.event.bean.VerificationStatus
import com.exactpro.th2.common.event.bean.VerificationStatus.FAILED
import com.exactpro.th2.common.event.bean.VerificationStatus.NA
import com.exactpro.th2.common.event.bean.VerificationStatus.PASSED
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.FilterOperation.EMPTY
import com.exactpro.th2.common.grpc.FilterOperation.EQUAL
import com.exactpro.th2.common.grpc.FilterOperation.EQ_DECIMAL_PRECISION
import com.exactpro.th2.common.grpc.FilterOperation.EQ_TIME_PRECISION
import com.exactpro.th2.common.grpc.FilterOperation.IN
import com.exactpro.th2.common.grpc.FilterOperation.LESS
import com.exactpro.th2.common.grpc.FilterOperation.LIKE
import com.exactpro.th2.common.grpc.FilterOperation.MORE
import com.exactpro.th2.common.grpc.FilterOperation.NOT_EMPTY
import com.exactpro.th2.common.grpc.FilterOperation.NOT_EQUAL
import com.exactpro.th2.common.grpc.FilterOperation.NOT_IN
import com.exactpro.th2.common.grpc.FilterOperation.NOT_LESS
import com.exactpro.th2.common.grpc.FilterOperation.NOT_LIKE
import com.exactpro.th2.common.grpc.FilterOperation.NOT_MORE
import com.exactpro.th2.common.grpc.FilterOperation.NOT_WILDCARD
import com.exactpro.th2.common.grpc.FilterOperation.WILDCARD
import com.exactpro.th2.common.grpc.ListValueFilter
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootComparisonSettings
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.SimpleList
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageFilter
import com.exactpro.th2.common.utils.message.toProtoDuration
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.common.value.toValueFilter
import com.exactpro.th2.sailfish.utils.RootComparisonSettingsUtils.convertToFilterSettings
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.MethodSource
import strikt.api.Assertion
import strikt.api.expectThat
import strikt.assertions.hasSize
import strikt.assertions.isEqualTo
import strikt.assertions.isNotNull
import strikt.assertions.isNull
import java.time.Duration
import java.util.stream.Stream

class TestVerificationEntryUtils {
    private val converter = AbstractCheckTask.PROTO_CONVERTER

    @ParameterizedTest(name = "[{index}] hide operation in expected: {0}, is key: {1}")
    @CsvSource(textBlock = """
        true,true
        false,true
        true,false
        false,false""")
    fun `simple fields test`(hideOperationInExpected: Boolean, isKey: Boolean) {
        val filter = rootFilter(timePrecision = Duration.ofMillis(1), decimalPrecision = 0.001) {
            simpleFilter("simple-equal", "test-equal", EQUAL, isKey)
            simpleFilter("simple-not_equal", "test-not_equal", NOT_EQUAL, isKey)
            emptyFilter("simple-empty", isKey)
            notEmptyFilter("simple-not_empty", isKey)
            simpleFilter("simple-eq_time_precision", "2025-12-22T12:36:27.123456789", EQ_TIME_PRECISION, isKey)
            simpleFilter("simple-eq_decimal_precision", "10.123456789", EQ_DECIMAL_PRECISION, isKey)
        }

        val message = message("test")
            .putFields("simple-equal", "test-equal".toValue())
            .putFields("simple-not_equal", "test-equal".toValue())
//            .putFields("simple-empty", nullValue())
            .putFields("simple-not_empty", "test-not_empty".toValue())
            .putFields("simple-eq_time_precision", "2025-12-22T12:36:27.123".toValue())
            .putFields("simple-eq_decimal_precision", "10.123".toValue())
            .build()

        val expected = converter.fromProtoFilter(
            filter.messageFilter,
            convertToFilterSettings(filter.comparisonSettings),
            filter.messageType,
        )
        val actual = converter.fromProtoMessage(message, false)
        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            isKeepResultGroupOrder = true
            metaContainer = container
        }
        val result = MessageComparator.compare(actual, expected, settings)
            .assertNotNull { "Result must not be null" }

        val entry = VerificationEntryUtils.createVerificationEntry(result, hideOperationInExpected)
        expectThat(entry) {
            expectCollection("6", "5") {
                hasSize(6)
                get { get("simple-equal") }.isNotNull() and {
                    expectField("test-equal", "test-equal", isKey = isKey)
                }
                get { get("simple-not_equal") }.isNotNull() and {
                    expectField("test-not_equal", "test-equal", operation = NOT_EQUAL, isKey = isKey)
                }
                get { get("simple-empty") }.isNotNull() and {
                    expectField("#", null, operation = EMPTY, isKey = isKey)
                }
                get { get("simple-not_empty") }.isNotNull() and {
                    expectField("*", "test-not_empty", operation = NOT_EMPTY, isKey = isKey)
                }
                get { get("simple-eq_time_precision") }.isNotNull() and {
                    expectField("2025-12-22T12:36:27.123456789 ± 0.001s", "2025-12-22T12:36:27.123", operation = EQ_TIME_PRECISION, isKey = isKey)
                }
                get { get("simple-eq_decimal_precision") }.isNotNull() and {
                    expectField("10.123456789 ± 0.001", "10.123", operation = EQ_DECIMAL_PRECISION, isKey = isKey)
                }
            }
        }
    }

    @Test
    fun `not hide operation in expected test`() {
        val filter = rootFilter(timePrecision = Duration.ofMillis(1), decimalPrecision = 0.001) {
            inFilter("simple-in", listOf("test-1", "test-2", "test-3"))
            notInFilter("simple-not_in", listOf("test-1", "test-2", "test-3"))
            simpleFilter("simple-like", ".*-like", LIKE)
            simpleFilter("simple-not_like", ".*-not_like", NOT_LIKE)
            simpleFilter("simple-wildcard", "t?*-wildcard", WILDCARD)
            simpleFilter("simple-not_wildcard", "t?*-not_wildcard", NOT_WILDCARD)
            simpleFilter("simple-more", "10", MORE)
            simpleFilter("simple-not_more", "10", NOT_MORE)
            simpleFilter("simple-less", "10", LESS)
            simpleFilter("simple-not_less", "10", NOT_LESS)
        }

        val message = message("test")
            .putFields("simple-in", "test-2".toValue())
            .putFields("simple-not_in", "test-4".toValue())
            .putFields("simple-like", "test-like".toValue())
            .putFields("simple-not_like", "test-like".toValue())
            .putFields("simple-wildcard", "test-wildcard".toValue())
            .putFields("simple-not_wildcard", "test-wildcard".toValue())
            .putFields("simple-more", "11".toValue())
            .putFields("simple-not_more", "10".toValue())
            .putFields("simple-less", "9".toValue())
            .putFields("simple-not_less", "10".toValue())
            .build()

        val expected = converter.fromProtoFilter(
            filter.messageFilter,
            convertToFilterSettings(filter.comparisonSettings),
            filter.messageType,
        )
        val actual = converter.fromProtoMessage(message, false)
        val settings = ComparatorSettings().apply { isKeepResultGroupOrder = true }
        val result = MessageComparator.compare(actual, expected, settings)
            .assertNotNull { "Result must not be null" }

        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        expectThat(entry) {
            expectCollection("10", "10") {
                hasSize(10)
                get { get("simple-in") }.isNotNull() and {
                    expectField("IN [test-1, test-2, test-3]", "test-2", operation = IN)
                }
                get { get("simple-not_in") }.isNotNull() and {
                    expectField("NOT_IN [test-1, test-2, test-3]", "test-4", operation = NOT_IN)
                }
                get { get("simple-like") }.isNotNull() and {
                    expectField("LIKE .*-like", "test-like", operation = LIKE)
                }
                get { get("simple-not_like") }.isNotNull() and {
                    expectField("NOT_LIKE .*-not_like", "test-like", operation = NOT_LIKE)
                }
                get { get("simple-wildcard") }.isNotNull() and {
                    expectField("WILDCARD t?*-wildcard", "test-wildcard", operation = WILDCARD)
                }
                get { get("simple-not_wildcard") }.isNotNull() and {
                    expectField("NOT_WILDCARD t?*-not_wildcard", "test-wildcard", operation = NOT_WILDCARD)
                }
                get { get("simple-more") }.isNotNull() and {
                    expectField(">10", "11", operation = MORE)
                }
                get { get("simple-not_more") }.isNotNull() and {
                    expectField("<=10", "10", operation = NOT_MORE)
                }
                get { get("simple-less") }.isNotNull() and {
                    expectField("<10", "9", operation = LESS)
                }
                get { get("simple-not_less") }.isNotNull() and {
                    expectField(">=10", "10", operation = NOT_LESS)
                }
            }
        }
    }

    @Test
    fun `hide operation in expected test`() {
        val filter = rootFilter(timePrecision = Duration.ofMillis(1), decimalPrecision = 0.001) {
            inFilter("simple-in", listOf("test-1", "test-2", "test-3"))
            notInFilter("simple-not_in", listOf("test-1", "test-2", "test-3"))
            simpleFilter("simple-like", ".*-like", LIKE)
            simpleFilter("simple-not_like", ".*-not_like", NOT_LIKE)
            simpleFilter("simple-wildcard", "t?*-wildcard", WILDCARD)
            simpleFilter("simple-not_wildcard", "t?*-not_wildcard", NOT_WILDCARD)
            simpleFilter("simple-more", "10", MORE)
            simpleFilter("simple-not_more", "10", NOT_MORE)
            simpleFilter("simple-less", "10", LESS)
            simpleFilter("simple-not_less", "10", NOT_LESS)
        }

        val message = message("test")
            .putFields("simple-in", "test-2".toValue())
            .putFields("simple-not_in", "test-4".toValue())
            .putFields("simple-like", "test-like".toValue())
            .putFields("simple-not_like", "test-like".toValue())
            .putFields("simple-wildcard", "test-wildcard".toValue())
            .putFields("simple-not_wildcard", "test-wildcard".toValue())
            .putFields("simple-more", "11".toValue())
            .putFields("simple-not_more", "10".toValue())
            .putFields("simple-less", "9".toValue())
            .putFields("simple-not_less", "10".toValue())
            .build()

        val expected = converter.fromProtoFilter(
            filter.messageFilter,
            convertToFilterSettings(filter.comparisonSettings),
            filter.messageType,
        )
        val actual = converter.fromProtoMessage(message, false)
        val settings = ComparatorSettings().apply { isKeepResultGroupOrder = true }
        val result = MessageComparator.compare(actual, expected, settings)
            .assertNotNull { "Result must not be null" }

        val entry = VerificationEntryUtils.createVerificationEntry(result, true)
        expectThat(entry) {
            expectCollection("10", "10") {
                hasSize(10)
                get { get("simple-in") }.isNotNull() and {
                    expectField("[test-1, test-2, test-3]", "test-2", operation = IN)
                }
                get { get("simple-not_in") }.isNotNull() and {
                    expectField("[test-1, test-2, test-3]", "test-4", operation = NOT_IN)
                }
                get { get("simple-like") }.isNotNull() and {
                    expectField(".*-like", "test-like", operation = LIKE)
                }
                get { get("simple-not_like") }.isNotNull() and {
                    expectField(".*-not_like", "test-like", operation = NOT_LIKE)
                }
                get { get("simple-wildcard") }.isNotNull() and {
                    expectField("t?*-wildcard", "test-wildcard", operation = WILDCARD)
                }
                get { get("simple-not_wildcard") }.isNotNull() and {
                    expectField("t?*-not_wildcard", "test-wildcard", operation = NOT_WILDCARD)
                }
                get { get("simple-more") }.isNotNull() and {
                    expectField("10", "11", operation = MORE)
                }
                get { get("simple-not_more") }.isNotNull() and {
                    expectField("10", "10", operation = NOT_MORE)
                }
                get { get("simple-less") }.isNotNull() and {
                    expectField("10", "9", operation = LESS)
                }
                get { get("simple-not_less") }.isNotNull() and {
                    expectField("10", "10", operation = NOT_LESS)
                }
            }
        }
    }

    @Test
    fun `null value in message`() {
        val filter = rootFilter { emptyFilter("A") }

        val message = message("test")
            .putFields("A", nullValue())
            .putFields("B", nullValue())
            .build()

        val expected = converter.fromProtoFilter(filter.messageFilter, "test")
        val actual = converter.fromProtoMessage(message, false)
        val settings = ComparatorSettings().apply {
            isKeepResultGroupOrder = true
        }

        val result = MessageComparator.compare(actual, expected, settings)
            .assertNotNull { "Result must not be null" }
        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        expectThat(entry) {
            expectCollection("1", "2", status = FAILED) {
                hasSize(2)
                get { get("A") }.isNotNull() and {
                    expectField("#", null, operation = EMPTY, status = FAILED)
                }
                get { get("B") }.isNotNull() and {
                    expectField(null, null, status = NA)
                }
            }
        }
    }

    @Test
    fun `key field in reordered collection`() {
        val filter: RootMessageFilter = rootFilter {
            messageFilter("msg") { simpleFilter("A", "1") }
            collectionFilter("collection") {
                messageFilter { simpleFilter("B", "2") }
                messageFilter { simpleFilter("A", "1", isKey = true) }
            }
        }

        val actual = message("test").apply {
            putFields(
                "collection", listOf(
                    message().putFields("A", "1".toValue()).build(),
                    message().putFields("B", "2".toValue()).build()
                ).toValue()
            )
            putFields("msg", message().putFields("A", "1".toValue()).toValue())
        }.build()

        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            metaContainer = container
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(actualIMessage, filterIMessage, settings)
            .assertNotNull { "Result must not be null" }
        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        expectThat(entry) {
            expectCollection("2", "2") {
                hasSize(2)
                get { get("collection") }.isNotNull() and {
                    expectCollection("2", "2") {
                        hasSize(2)
                        get { get("0") }.isNotNull() and {
                            expectCollection("1", "1") {
                                hasSize(1)
                                get { get("A") }.isNotNull() and { expectField("1", "1", isKey = true) }
                            }
                        }
                        get { get("1") }.isNotNull() and {
                            expectCollection("1", "1") {
                                hasSize(1)
                                get { get("B") }.isNotNull() and { expectField("2", "2") }
                            }
                        }
                    }
                }
                get { get("msg") }.isNotNull() and {
                    expectCollection("1", "1") {
                        hasSize(1)
                        get { get("A") }.isNotNull() and { expectField("1", "1") }
                    }
                }
            }
        }
    }

    @Test
    fun `collection as key field`() {
        val filter: RootMessageFilter = rootFilter {
            messageFilter("msg") { simpleFilter("A", "1") }
            collectionFilter("collection", isKey = true) {
                messageFilter { simpleFilter("B", "2") }
                messageFilter { simpleFilter("A", "1") }
            }
        }

        val actual = message("test").apply {
            putFields(
                "collection", listOf(
                    message().putFields("A", "1".toValue()).build(),
                    message().putFields("B", "2".toValue()).build()
                ).toValue()
            )
            putFields("msg", message().putFields("A", "1".toValue()).toValue())
        }.build()

        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            metaContainer = container
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(
            actualIMessage,
            filterIMessage,
            settings
        )
        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        expectThat(entry) {
            expectCollection("2", "2") {
                hasSize(2)
                get { get("collection") }.isNotNull() and {
                    expectCollection("2", "2", isKey = true) {
                        hasSize(2)
                        get { get("0") }.isNotNull() and {
                            expectCollection("1", "1") {
                                hasSize(1)
                                get { get("A") }.isNotNull() and { expectField("1", "1") }
                            }
                        }
                        get { get("1") }.isNotNull() and {
                            expectCollection("1", "1") {
                                hasSize(1)
                                get { get("B") }.isNotNull() and { expectField("2", "2") }
                            }
                        }
                    }
                }
                get { get("msg") }.isNotNull() and {
                    expectCollection("1", "1") {
                        hasSize(1)
                        get { get("A") }.isNotNull() and { expectField("1", "1") }
                    }
                }
            }
        }
    }

    @Test
    fun `message as key field`() {
        val filter: RootMessageFilter = rootFilter {
            messageFilter("msg", isKey = true) { simpleFilter("A", "1") }
            collectionFilter("collection") {
                messageFilter { simpleFilter("B", "2") }
                messageFilter { simpleFilter("A", "1") }
            }
        }

        val actual = message("test").apply {
            putFields(
                "collection", listOf(
                    message().putFields("A", "1".toValue()).build(),
                    message().putFields("B", "2".toValue()).build()
                ).toValue()
            )
            putFields("msg", message().putFields("A", "1".toValue()).toValue())
        }.build()

        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            metaContainer = container
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(actualIMessage, filterIMessage, settings)
            .assertNotNull { "Result must not be null" }
        val entry = VerificationEntryUtils.createVerificationEntry(result, false)

        expectThat(entry) {
            expectCollection("2", "2") {
                hasSize(2)
                get { get("collection") }.isNotNull() and {
                    expectCollection("2", "2") {
                        hasSize(2)
                        get { get("0") }.isNotNull() and {
                            expectCollection("1", "1") {
                                hasSize(1)
                                get { get("A") }.isNotNull() and { expectField("1", "1") }
                            }
                        }
                        get { get("1") }.isNotNull() and {
                            expectCollection("1", "1") {
                                hasSize(1)
                                get { get("B") }.isNotNull() and { expectField("2", "2") }
                            }
                        }
                    }
                }
                get { get("msg") }.isNotNull() and {
                    expectCollection("1", "1", isKey = true) {
                        hasSize(1)
                        get { get("A") }.isNotNull() and { expectField("1", "1") }
                    }
                }
            }
        }
    }

    @Test
    fun `expected value converted as null value`() {
        val filter = rootFilter { simpleFilter("A", "1") }

        val actual = message("test").apply {
            putFields("A", "1".toValue())
            putFields("B", "2".toValue())
        }.build()

        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            metaContainer = container
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(actualIMessage, filterIMessage, settings)
            .assertNotNull { "Result must not be null" }

        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        expectThat(entry) {
            expectCollection("1", "2") {
                hasSize(2)
                get { get("A") }.isNotNull() and { expectField("1", "1") }
                get { get("B") }.isNotNull() and { expectField(null, "2", status = NA) }
            }
        }
    }


    @Test
    fun `actual value converted as null value`() {
        val filter = rootFilter {
            simpleFilter("A", "1")
            simpleFilter("B", "2")
        }

        val actual = message("test").apply {
            putFields("A", "1".toValue())
        }.build()

        val container = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        val settings = ComparatorSettings().apply {
            metaContainer = container
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(actualIMessage, filterIMessage, settings)
            .assertNotNull { "Result must not be null" }

        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        expectThat(entry) {
            expectCollection("2", "1", status = FAILED) {
                hasSize(2)
                get { get("A") }.isNotNull() and { expectField("1", "1") }
                get { get("B") }.isNotNull() and { expectField("2", null, status = FAILED) }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("unexpectedTypeMismatch")
    fun `verify messages with different value type`(
        actualValue: Value,
        expectedValueFilter: ValueFilter,
        expectedHint: String?
    ) {
        val filter = rootFilter {
            simpleFilter("A", "1")
            filter("B", expectedValueFilter)
        }

        val actual = message("test").apply {
            putFields("A", "1".toValue())
            putFields("B", actualValue)
        }.build()

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(actualIMessage, filterIMessage, ComparatorSettings())
            .assertNotNull { "Result must not be null" }

        val entry = VerificationEntryUtils.createVerificationEntry(result, false)
        val keyEntry = entry.fields["B"].assertNotNull { "The key 'B' is missing in ${entry.toDebugString()}" }
        assertEquals(expectedHint, keyEntry.hint, "Hint must be equal")
    }

    @Test
    fun `fail in nested field visible from outside`() {
        val expected = msgFilter {
            messageFilter("A") {
                simpleFilter("B", "42")
            }
        }
        val actualMessage = message("test")
            .putFields(
                "A", message()
                    .putFields("B", 41.toValue())
                    .toValue()
            ).build()
        val settings = ComparatorSettings()

        val result = MessageComparator.compare(
            converter.fromProtoFilter(expected, "test"),
            converter.fromProtoMessage(actualMessage, false),
            settings,
        ).assertNotNull { "Result must not be null" }
        val verification = VerificationEntryUtils.createVerificationEntry(result, false)

        expectThat(verification) {
            expectCollection("1", "1", status = FAILED) {
                hasSize(1)
                get { get("A") }.isNotNull() and {
                    expectCollection("1", "1", status = FAILED) {
                        hasSize(1)
                        get { get("B") }.isNotNull() and {
                            expectField(
                                "String",
                                "Filter",
                                hint = "Value type mismatch - actual: Filter, expected: String",
                                status = FAILED
                            )
                        }
                    }
                }
            }
        }
    }

    companion object {
        private fun VerificationEntry.toDebugString(): String = ObjectMapper().writeValueAsString(this)
        private fun <T : Any> T?.assertNotNull(msg: () -> String): T {
            assertNotNull(this, msg)
            return this!!
        }

        private fun Assertion.Builder<VerificationEntry>.expectField(
            expected: String?,
            actual: String?,
            isKey: Boolean = false,
            hint: String? = null,
            operation: FilterOperation = EQUAL,
            status: VerificationStatus = PASSED,
        ) {
            get { this.actual } isEqualTo actual
            get { this.expected } isEqualTo expected
            get { this.isKey } isEqualTo isKey
            get { this.fields }.isNull()
            get { this.hint } isEqualTo hint
            get { this.operation } isEqualTo operation.name
            get { this.status } isEqualTo status
            get { this.type } isEqualTo "field"
        }

        private fun Assertion.Builder<VerificationEntry>.expectCollection(
            expected: String?,
            actual: String?,
            isKey: Boolean = false,
            hint: String? = null,
            operation: FilterOperation = EQUAL,
            status: VerificationStatus = PASSED,
            expectFields: Assertion.Builder<Map<String, VerificationEntry>>.() -> Unit
        ) {
            get { this.actual } isEqualTo actual
            get { this.expected } isEqualTo expected
            get { this.isKey } isEqualTo isKey
            get { this.fields }.isNotNull() and expectFields
            get { this.hint } isEqualTo hint
            get { this.operation } isEqualTo operation.name
            get { this.status } isEqualTo status
            get { this.type } isEqualTo "collection"
        }

        @JvmStatic
        fun unexpectedTypeMismatch(): Stream<Arguments> = Stream.of(
            arguments("2".toValue(), "2".toValueFilter(), null),
            arguments(
                message().putFields("A", "1".toValue()).toValue(),
                messageFilter().putFields("A", "1".toValueFilter()).toValueFilter(),
                null
            ),
            arguments(
                listOf("2".toValue()).toValue(),
                listOf("2".toValueFilter()).toValueFilter(),
                null
            ),
            arguments(
                "2".toValue(),
                messageFilter().putFields("A", "1".toValueFilter()).toValueFilter(),
                "Value type mismatch - actual: String, expected: Message"
            ),
            arguments(
                "2".toValue(),
                listOf(messageFilter().putFields("A", "1".toValueFilter())).toValueFilter(),
                "Value type mismatch - actual: String, expected: Collection of Messages"
            ),
            arguments(
                message().putFields("A", "1".toValue()).toValue(),
                listOf(messageFilter().putFields("A", "1".toValueFilter())).toValueFilter(),
                "Value type mismatch - actual: Message, expected: Collection of Messages"
            ),
            arguments(
                "2".toValue(),
                listOf("2".toValueFilter()).toValueFilter(),
                "Value type mismatch - actual: String, expected: Collection"
            ),
            arguments(
                message().putFields("A", "1".toValue()).toValue(),
                "2".toValueFilter(),
                "Value type mismatch - actual: Message, expected: String"
            ),
            arguments(
                listOf(message().putFields("A", "1".toValue()).build()).toValue(),
                "2".toValueFilter(),
                "Value type mismatch - actual: Collection of Messages, expected: String"
            ),
            arguments(
                listOf(message().putFields("A", "1".toValue()).build()).toValue(),
                messageFilter().putFields("A", "1".toValueFilter()).toValueFilter(),
                "Value type mismatch - actual: Collection of Messages, expected: Message"
            ),
            arguments(
                message().putFields("A", "1".toValue()).toValue(),
                listOf("2".toValueFilter()).toValueFilter(),
                "Value type mismatch - actual: Message, expected: Collection"
            )
        )

        @DslMarker
        private annotation class DslMFilter
        @DslMarker
        private annotation class DslCFilter

        @DslMFilter
        private class MFilter {
            private val builder = MessageFilter.newBuilder()

            fun simpleFilter(key: String, value: String, operation: FilterOperation = EQUAL, isKey: Boolean = false): MFilter = this.apply {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setOperation(operation)
                    .setSimpleFilter(value)
                    .build())
            }

            fun inFilter(key: String, values: List<String>, isKey: Boolean = false): MFilter = this.apply {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setOperation(IN)
                    .setSimpleList(SimpleList.newBuilder().addAllSimpleValues(values).build())
                    .build())
            }

            fun notInFilter(key: String, values: List<String>, isKey: Boolean = false): MFilter = this.apply {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setOperation(NOT_IN)
                    .setSimpleList(SimpleList.newBuilder().addAllSimpleValues(values).build())
                    .build())
            }

            fun emptyFilter(key: String, isKey: Boolean = false): MFilter = this.apply {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setOperation(EMPTY)
                    .build())
            }

            fun notEmptyFilter(key: String, isKey: Boolean = false): MFilter = this.apply {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setOperation(NOT_EMPTY)
                    .build())
            }

            fun filter(key: String, filter: ValueFilter): MFilter = this.apply {
                builder.putFields(key, filter)
            }

            fun messageFilter(key: String, isKey: Boolean = false, block: MFilter.() -> Unit = {}) {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setMessageFilter(MFilter().apply(block).build())
                    .build())
            }

            fun collectionFilter(key: String, isKey: Boolean = false, block: CFilter.() -> Unit = {}) {
                builder.putFields(key, ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setListFilter(CFilter().apply(block).build())
                    .setOperation(NOT_EMPTY)
                    .build())
            }

            fun build(): MessageFilter = builder.build()
        }

        @Suppress("unused")
        @DslCFilter
        private class CFilter() {
            private val builder = ListValueFilter.newBuilder()

            fun simpleFilter(value: String, operation: FilterOperation = EQUAL): CFilter = this.apply {
                builder.addValues(ValueFilter.newBuilder()
                    .setOperation(operation)
                    .setSimpleFilter(value)
                    .build())
            }

            fun inFilter(values: List<String>): CFilter = this.apply {
                builder.addValues(ValueFilter.newBuilder()
                    .setOperation(IN)
                    .setSimpleList(SimpleList.newBuilder().addAllSimpleValues(values).build())
                    .build())
            }

            fun notInFilter(values: List<String>): CFilter = this.apply {
                builder.addValues(ValueFilter.newBuilder()
                    .setOperation(NOT_IN)
                    .setSimpleList(SimpleList.newBuilder().addAllSimpleValues(values).build())
                    .build())
            }

            fun emptyFilter(): CFilter = this.apply {
                builder.addValues(ValueFilter.newBuilder()
                    .setOperation(EMPTY)
                    .build())
            }

            fun notEmptyFilter(): CFilter = this.apply {
                builder.addValues(ValueFilter.newBuilder()
                    .setOperation(NOT_EMPTY)
                    .build())
            }

            fun messageFilter(isKey: Boolean = false, block: MFilter.() -> Unit = {}) {
                builder.addValues(ValueFilter.newBuilder()
                    .setKey(isKey)
                    .setMessageFilter(MFilter().apply(block).build())
                    .build())
            }

            fun build(): ListValueFilter = builder.build()
        }

        @Suppress("SameParameterValue")
        private fun rootFilter(
            name: String = "test",
            timePrecision: Duration = Duration.ZERO,
            decimalPrecision: Double = 0.0,
            block: MFilter.() -> Unit = {}
        ): RootMessageFilter = RootMessageFilter.newBuilder().apply {
            messageType = name
            comparisonSettings = RootComparisonSettings.newBuilder()
                .setTimePrecision(timePrecision.toProtoDuration())
                .setDecimalPrecision(decimalPrecision.toString())
                .build()
            messageFilter = MFilter().apply(block).build()
        }.build()

        private fun msgFilter(
            block: MFilter.() -> Unit = {}
        ): MessageFilter = MFilter().apply(block).build()
    }
}