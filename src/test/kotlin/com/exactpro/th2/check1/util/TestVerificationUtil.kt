/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.util

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonUtil
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.common.grpc.ComparisonSettings
import com.exactpro.th2.common.grpc.FailUnexpected
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.Value
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.message.messageFilter
import com.exactpro.th2.common.value.nullValue
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.common.value.toValueFilter
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.stream.Stream

internal class TestVerificationUtil {
    @Test
    internal fun convertsMetadataFilterToMetaContainer() {
        val metaContainer = VerificationUtil.toMetaContainer(
            MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toPropertyFilter(FilterOperation.EQUAL, true))
                .build()
        )
        Assertions.assertTrue(metaContainer.hasKeyFields()) {
            "MetaContainer does not have key fields: $metaContainer"
        }
        Assertions.assertEquals(
            mapOf(
                "keyProp" to false /*not transitive*/
            ), metaContainer.keyFields
        )
    }

    @ParameterizedTest
    @MethodSource("failUnexpectedByFieldsAndMessages")
    fun `fail unexpected test`(valueFilter: ValueFilter, value: Value) {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType("Test")
            .setMessageFilter(
                MessageFilter.newBuilder()
                    .putFields("A", valueFilter)
                    .setComparisonSettings(
                        ComparisonSettings.newBuilder()
                            .setFailUnexpected(FailUnexpected.FIELDS_AND_MESSAGES)
                            .build()
                    )
                    .build()
            )
            .build()

        val actual = message("Test").apply {
            putFields("A", value)
        }.build()

        val settings = ComparatorSettings().apply {
            metaContainer = VerificationUtil.toMetaContainer(filter.messageFilter, false)
        }

        val actualIMessage = converter.fromProtoMessage(actual, false)
        val filterIMessage = converter.fromProtoFilter(filter.messageFilter, filter.messageType)
        val result = MessageComparator.compare(
            actualIMessage,
            filterIMessage,
            settings
        )

        Assertions.assertNotNull(result) { "Result cannot be null" }
        Assertions.assertEquals(StatusType.FAILED, ComparisonUtil.getStatusType(result))
    }

    companion object {
        private val converter = AbstractCheckTask.PROTO_CONVERTER

        @JvmStatic
        fun failUnexpectedByFieldsAndMessages(): Stream<Arguments> = Stream.of(
            arguments(
                listOf(
                    messageFilter().putFields("A1", "1".toValueFilter())
                ).toValueFilter(),
                listOf(
                    message().putFields("A1", "1".toValue()).putFields("B1", "2".toValue()).build()
                ).toValue()
            ),
            arguments(
                messageFilter().putFields("A1", "1".toValueFilter()).toValueFilter(),
                message().putFields("A1", "1".toValue()).putFields("B1", "2".toValue()).build().toValue()
            ),
            arguments(
                messageFilter().putFields(
                    "A1", listOf("1", "2").toValueFilter()
                ).toValueFilter(),
                message().putFields(
                    "A1",
                    listOf("1", "2", "3").toValue()
                ).build().toValue()
            ),
            arguments(
                messageFilter().putFields(
                    "A1",
                    messageFilter().putFields("A2", "1".toValueFilter()).toValueFilter()
                ).toValueFilter(),
                message().putFields(
                    "A1",
                    message()
                        .putFields("A2", "1".toValue())
                        .putFields("B2", "2".toValue())
                        .build().toValue()
                ).build().toValue()
            ),
            arguments(
                listOf(
                    "1".toValueFilter()
                ).toValueFilter(),
                listOf(
                    "1".toValue(),
                    "2".toValue()
                ).toValue()
            ),
            arguments(
                messageFilter().putFields("A", 42.toValueFilter()).toValueFilter(),
                message().putFields("A", 42.toValue()).putFields("B", nullValue()).toValue()
            )
        )
    }
}