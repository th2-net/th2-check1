/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.event

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.event.bean.VerificationEntry
import com.exactpro.th2.common.grpc.ListValueFilter
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import com.exactpro.th2.common.value.toValue
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll

class TestVerificationEntryUtils {
    private val converter = ProtoToIMessageConverter(VerificationUtil.FACTORY_PROXY, null, null)

    @Test
    fun `key field in reordered collection`() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType("Test")
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("collection", ValueFilter.newBuilder()
                    .setListFilter(ListValueFilter.newBuilder().apply {
                        addValues(ValueFilter.newBuilder()
                            .setMessageFilter(MessageFilter.newBuilder().putFields("B", ValueFilter.newBuilder().setSimpleFilter("2").build()).build())
                            .build())

                        addValues(ValueFilter.newBuilder()
                            .setMessageFilter(MessageFilter.newBuilder().putFields("A", ValueFilter.newBuilder().setKey(true).setSimpleFilter("1").build())
                                .build())
                            .build())
                    }).build())
                .putFields("msg", ValueFilter.newBuilder()
                    .setMessageFilter(MessageFilter.newBuilder().putFields("A", ValueFilter.newBuilder().setSimpleFilter("1").build()).build())
                    .build())
                .build())
            .build()

        val actual = message("Test").apply {
            putFields("collection", listOf(
                message().putFields("A", "1".toValue()).build(),
                message().putFields("B", "2".toValue()).build()
            ).toValue())
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
        val entry = VerificationEntryUtils.createVerificationEntry(result)
        val collectionEntry = entry.fields["collection"].assertNotNull { "collection field is missing in ${entry.toDebugString()}" }
        assertAll(
            {
                collectionEntry.fields["0"].assertNotNull { "no 0 element in ${collectionEntry.toDebugString()}" }
                    .fields["A"].assertNotNull { "no filed A in ${collectionEntry.toDebugString()}" }.apply {
                        Assertions.assertTrue(isKey) { "field A is not a key ${collectionEntry.toDebugString()}" }
                }
            },
            {
                collectionEntry.fields["1"].assertNotNull { "no 1 element in ${collectionEntry.toDebugString()}" }
                    .fields["B"].assertNotNull { "no filed B in ${collectionEntry.toDebugString()}" }.apply {
                    Assertions.assertFalse(isKey) { "field B is a key ${collectionEntry.toDebugString()}" }
                }
            }
        )
    }

    @Test
    fun `collection as key field`() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType("Test")
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("collection", ValueFilter.newBuilder()
                    .setKey(true)
                    .setListFilter(ListValueFilter.newBuilder().apply {
                        addValues(ValueFilter.newBuilder()
                            .setMessageFilter(MessageFilter.newBuilder().putFields("B", ValueFilter.newBuilder().setSimpleFilter("2").build()).build())
                            .build())

                        addValues(ValueFilter.newBuilder()
                            .setMessageFilter(MessageFilter.newBuilder().putFields("A", ValueFilter.newBuilder().setSimpleFilter("1").build())
                                .build())
                            .build())
                    }).build())
                .putFields("msg", ValueFilter.newBuilder()
                    .setMessageFilter(MessageFilter.newBuilder().putFields("A", ValueFilter.newBuilder().setSimpleFilter("1").build()).build())
                    .build())
                .build())
            .build()

        val actual = message("Test").apply {
            putFields("collection", listOf(
                message().putFields("A", "1".toValue()).build(),
                message().putFields("B", "2".toValue()).build()
            ).toValue())
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
        val entry = VerificationEntryUtils.createVerificationEntry(result)
        val collectionEntry = entry.fields["collection"].assertNotNull { "collection field is missing in ${entry.toDebugString()}" }
        Assertions.assertTrue(collectionEntry.isKey) { "collection is not a key field in ${collectionEntry.toDebugString()}" }
    }

    @Test
    fun `message as key field`() {
        val filter: RootMessageFilter = RootMessageFilter.newBuilder()
            .setMessageType("Test")
            .setMessageFilter(MessageFilter.newBuilder()
                .putFields("collection", ValueFilter.newBuilder()
                    .setListFilter(ListValueFilter.newBuilder().apply {
                        addValues(ValueFilter.newBuilder()
                            .setMessageFilter(MessageFilter.newBuilder().putFields("B", ValueFilter.newBuilder().setSimpleFilter("2").build()).build())
                            .build())

                        addValues(ValueFilter.newBuilder()
                            .setMessageFilter(MessageFilter.newBuilder().putFields("A", ValueFilter.newBuilder().setSimpleFilter("1").build())
                                .build())
                            .build())
                    }).build())
                .putFields("msg", ValueFilter.newBuilder()
                    .setKey(true)
                    .setMessageFilter(MessageFilter.newBuilder().putFields("A", ValueFilter.newBuilder().setSimpleFilter("1").build()).build())
                    .build())
                .build())
            .build()

        val actual = message("Test").apply {
            putFields("collection", listOf(
                message().putFields("A", "1".toValue()).build(),
                message().putFields("B", "2".toValue()).build()
            ).toValue())
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
        val entry = VerificationEntryUtils.createVerificationEntry(result)
        val msgEntry = entry.fields["msg"].assertNotNull { "msg field is missing in ${entry.toDebugString()}" }
        Assertions.assertTrue(msgEntry.isKey) { "msg is not a key field in ${msgEntry.toDebugString()}" }
    }

    companion object {
        private fun VerificationEntry.toDebugString(): String = ObjectMapper().writeValueAsString(this)
        private fun <T : Any> T?.assertNotNull(msg: () -> String): T {
            Assertions.assertNotNull(this, msg)
            return this!!
        }
    }
}