/*
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
 */

package com.exactpro.th2.verifier

import com.exactpro.sf.aml.script.actions.WaitAction.waitMessage
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.util.Pair
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.infra.grpc.MessageFilter
import io.reactivex.Observable
import java.time.Instant

/**
 *
 */
class CheckRuleTask(description: String,
                    startTime: Instant,
                    sessionAlias: String,
                    messageStream: Observable<StreamContainer>,
                    private val protoMessageFilter: MessageFilter) : AbstractCheckTask(description, startTime, sessionAlias, messageStream) {

    private val filter : IMessage = converter.fromProtoFilter(protoMessageFilter, protoMessageFilter.messageType)
    private val settings : ComparatorSettings = protoMessageFilter.toCompareSettings()

    override fun onNext(singleCSHIterator: SingleCSHIterator) {
        try {
            val comparisonResults: List<Pair<IMessage, ComparisonResult>> = waitMessage(settings, filter,
                singleCSHIterator, 0, emptyList(), false)

            if (LOGGER.isDebugEnabled) {
                comparisonResults.forEach { LOGGER.debug("'${it.first.name}': ${it.second}") }
            }

            if (comparisonResults.isNotEmpty()) {
                dispose()
//                lastSequence = singleCSHIterator.protoMessage.metadata.id.sequence
                rootEvent.endTimestamp()
                    .appendEventWithVerification(singleCSHIterator.protoMessage, protoMessageFilter, comparisonResults[0].second)
            }
        } catch (e: Exception) {
            throw e.also {
                rootEvent.status(FAILED)
                    .bodyData(EventUtils.createMessageBean(it.message))
            }
        }
    }
}