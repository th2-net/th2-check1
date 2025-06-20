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

package com.exactpro.th2.check1.rule.check

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.RuleConfiguration
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.ComparisonContainer
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.OnTaskFinished
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
import io.reactivex.rxjava3.core.Observable
import java.time.Instant

/**
 * This rule checks for the presence of a single message in the messages stream.
 */

class CheckRuleTask(
    ruleConfiguration: RuleConfiguration,
    startTime: Instant,
    sessionKey: SessionKey,
    protoMessageFilter: RootMessageFilter,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>,
    onTaskFinished: OnTaskFinished = EMPTY_STATUS_CONSUMER
) : AbstractCheckTask(ruleConfiguration, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

    protected class Refs(
        rootEvent: Event,
        onTaskFinished: OnTaskFinished,
        val protoMessageFilter: RootMessageFilter,
        val messageFilter: SailfishFilter,
        val metadataFilter: SailfishFilter?
    ) : AbstractCheckTask.Refs(rootEvent, onTaskFinished)

    override val refsKeeper = RefsKeeper(
        Refs(
            rootEvent = createRootEvent(),
            onTaskFinished = onTaskFinished,
            protoMessageFilter = protoMessageFilter,
            messageFilter = SailfishFilter(
                PROTO_CONVERTER.fromProtoPreFilter(protoMessageFilter),
                protoMessageFilter.toCompareSettings()
            ),
            metadataFilter = protoMessageFilter.metadataFilterOrNull()?.let {
                SailfishFilter(
                    PROTO_CONVERTER.fromMetadataFilter(it, METADATA_MESSAGE_NAME),
                    it.toComparisonSettings()
                )
            }
        )
    )

    private val refs get() = refsKeeper.refs

    override fun onStartInit() {
        val subEvent = Event.start()
            .endTimestamp()
            .name("Message filter")
            .type("Filter")
            .bodyData(refs.protoMessageFilter.toReadableBodyCollection())

        refs.rootEvent.addSubEvent(subEvent)
    }

    override fun onNext(messageContainer: MessageContainer) {
        val aggregatedResult = matchFilter(messageContainer, refs.messageFilter, refs.metadataFilter)

        val container = ComparisonContainer(messageContainer, refs.protoMessageFilter, aggregatedResult)

        if (container.matchesByKeys) {
            refs.rootEvent.appendEventsWithVerification(container)
            checkComplete()
        }
    }

    override fun onTimeout() {
        refs.rootEvent.addSubEventWithSamePeriod()
            .name("No message found by target keys")
            .type("Check failed")
            .status(FAILED)
    }

    override fun name(): String = "Check rule"

    override fun type(): String = "Check rule"

    override fun setup(rootEvent: Event) {
        rootEvent.bodyData(EventUtils.createMessageBean("Check rule for messages from ${sessionKey.run { "$bookName $sessionAlias ($direction direction)" }}"))
    }
}