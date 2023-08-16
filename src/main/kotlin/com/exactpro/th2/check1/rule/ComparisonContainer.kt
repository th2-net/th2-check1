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
package com.exactpro.th2.check1.rule

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.check1.utils.FilterUtils
import com.exactpro.th2.check1.utils.FilterUtils.fullMatch
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.utils.message.MessageHolder

class ComparisonContainer(
    val messageContainer: MessageContainer,
    val protoFilter: RootMessageFilter,
    val result: AggregatedFilterResult
) {
    val sailfishActual: IMessage
        get() = messageContainer.sailfishMessage

    val holderActual: MessageHolder
        get() = messageContainer.messageHolder

    /**
     * If [RootMessageFilter.hasMetadataFilter] for [protoFilter] is `true`
     * returns `true` if [result] contains all not `null` fields.
     *
     * Otherwise, returns `true` only if [AggregatedFilterResult.messageResult] is not `null`
     */
    val matchesByKeys: Boolean
        get() = FilterUtils.allMatches(result, protoFilter) { it != null }

    /**
     * If [RootMessageFilter.hasMetadataFilter] for [protoFilter] is `true`
     * returns `true` if [result] contains all not `null` fields and aggregated status is not [StatusType.FAILED].
     *
     * Otherwise, returns `true` only if [AggregatedFilterResult.messageResult] is not `null`
     * and its aggregated status is not [StatusType.FAILED]
     */
    val fullyMatches: Boolean
        get() = FilterUtils.allMatches(result, protoFilter) { it.fullMatch }
}

class AggregatedFilterResult(
    val messageResult: ComparisonResult?,
    val metadataResult: ComparisonResult?
) {
    operator fun component1(): ComparisonResult? = messageResult
    operator fun component2(): ComparisonResult? = metadataResult

    companion object {
        @JvmField
        val EMPTY = AggregatedFilterResult(null, null)
    }
}