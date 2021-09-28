/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.rule.check

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.ComparisonContainer
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.util.VerificationUtil.METADATA_MESSAGE_NAME
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toReadableBodyCollection
import com.exactpro.th2.common.schema.message.MessageRouter
import io.reactivex.Observable
import java.time.Instant

/**
 * This rule checks for the presence of a single message in the messages stream.
 */
class CheckRuleTask(
    description: String?,
    startTime: Instant,
    sessionKey: SessionKey,
    taskTimeout: TaskTimeout,
    maxEventBatchContentSize: Int,
    private val protoMessageFilter: RootMessageFilter,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>
) : AbstractCheckTask(description, taskTimeout, maxEventBatchContentSize, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

    private val messageFilter: SailfishFilter = SailfishFilter(
        converter.fromProtoPreFilter(protoMessageFilter),
        protoMessageFilter.toCompareSettings()
    )
    private val metadataFilter: SailfishFilter? = protoMessageFilter.metadataFilterOrNull()?.let {
        SailfishFilter(
            converter.fromMetadataFilter(it, METADATA_MESSAGE_NAME),
            it.toComparisonSettings()
        )
    }

    override fun onStart() {
        super.onStart()

        val subEvent = Event.start()
            .endTimestamp()
            .name("Message filter")
            .type("Filter")
            .bodyData(protoMessageFilter.toReadableBodyCollection())

        rootEvent.addSubEvent(subEvent)

    }

    override fun onNext(messageContainer: MessageContainer) {
        val aggregatedResult = matchFilter(messageContainer, messageFilter, metadataFilter)

        val container = ComparisonContainer(messageContainer, protoMessageFilter, aggregatedResult)

        if (container.matchesByKeys) {
            rootEvent.appendEventsWithVerification(container)
            checkComplete()
        }
    }

    override fun onTimeout() {
        rootEvent.addSubEventWithSamePeriod()
            .name("No message found by target keys")
            .type("Check failed")
            .status(FAILED)
    }

    override fun name(): String = "Check rule"

    override fun type(): String = "Check rule"

    override fun setup(rootEvent: Event) {
        rootEvent.bodyData(EventUtils.createMessageBean("Check rule for messages from ${sessionKey.run { "$sessionAlias ($direction direction)"} }"))
    }
}
