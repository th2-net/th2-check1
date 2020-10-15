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

package com.exactpro.th2.check1.rule.check

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.estore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.rule.AbstractCheckTask
import io.reactivex.Observable
import java.time.Instant

/**
 * This rule checks the presents of the single message in the messages stream.
 */
class CheckRuleTask(
        description: String?,
        startTime: Instant,
        sessionKey: SessionKey,
        timeout: Long,
        private val protoMessageFilter: MessageFilter,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        eventStoreStub: EventStoreServiceFutureStub
) : AbstractCheckTask(description, timeout, startTime, sessionKey, parentEventID, messageStream, eventStoreStub) {

    private val filter: IMessage = converter.fromProtoFilter(protoMessageFilter, protoMessageFilter.messageType)
    private val settings: ComparatorSettings = protoMessageFilter.toCompareSettings()

    init {
        rootEvent
            .name("Check rule $sessionKey")
            .type("Check rule")
    }

    override fun onStart() {
        super.onStart()

        val subEvent = Event.start()
            .endTimestamp()
            .name("Message filter")
            .type("Filter")
            .bodyData(EventUtils.createMessageBean("Passed filter will be placed there")) // TODO: Write filter

        rootEvent.addSubEvent(subEvent)

    }

    override fun onNext(messageContainer: MessageContainer) {
        val comparisonResult: ComparisonResult? = MessageComparator.compare(messageContainer.sailfishMessage, filter, settings)
        LOGGER.debug("Compare message '{}' result\n{}", messageContainer.sailfishMessage.name, comparisonResult)

        if (comparisonResult != null) {
            rootEvent.addSubEventWithSamePeriod()
                .appendEventWithVerification(messageContainer.protoMessage, protoMessageFilter, comparisonResult)
            checkComplete()
        }
    }

    override fun onTimeout() {
        rootEvent.addSubEventWithSamePeriod()
            .name("No message found by target keys")
            .type("Check failed")
            .status(FAILED)
    }
}