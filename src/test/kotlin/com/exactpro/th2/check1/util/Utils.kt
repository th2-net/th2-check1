/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.util

import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.common.event.bean.VerificationEntry
import com.exactpro.th2.common.event.bean.VerificationStatus
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.ValueFilter
import com.exactpro.th2.common.message.message
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals

fun String.toPropertyFilter(op: FilterOperation, key: Boolean = false): MetadataFilter.SimpleFilter =
    MetadataFilter.SimpleFilter.newBuilder()
        .setOperation(op)
        .setValue(this)
        .setKey(key)
        .build()

fun String.toValueFilter(op: FilterOperation = FilterOperation.EQUAL, key: Boolean = false): ValueFilter =
    ValueFilter.newBuilder().apply {
        this.operation = op
        this.setSimpleFilter(this@toValueFilter)
        this.key = key
    }.build()

fun createVerificationEntry(status: VerificationStatus): VerificationEntry = VerificationEntry().apply {
    this.status = status
}

fun createVerificationEntry(
    vararg verificationEntries: Pair<String, VerificationEntry>,
    status: VerificationStatus? = null,
): VerificationEntry =
    VerificationEntry().apply {
        if (verificationEntries.isNotEmpty()) {
            fields = linkedMapOf(*verificationEntries)
        }
        this.status = status
    }

inline fun <reified T : Throwable> assertThrowsWithMessages(
    vararg exceptionMessages: String?,
    crossinline action: () -> Unit
) {
    val exception = assertThrows<T> {
        action()
    }
    var currentException: Throwable? = exception
    for (exceptionMessage in exceptionMessages) {
        assertEquals(exceptionMessage, currentException?.message)
        currentException = currentException?.cause
    }
}

fun createDefaultMessage() = message(
    AbstractCheckTaskTest.BOOK_NAME,
    AbstractCheckTaskTest.MESSAGE_TYPE,
    Direction.FIRST,
    AbstractCheckTaskTest.SESSION_ALIAS
).also {
    it.metadataBuilder.mergeId(MessageID.newBuilder().setSequence(1).build())
}