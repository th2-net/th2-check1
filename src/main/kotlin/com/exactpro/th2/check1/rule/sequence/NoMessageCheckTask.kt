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

package com.exactpro.th2.check1.rule.sequence

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.ComparisonContainer
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask.Companion.PRE_FILTER_MESSAGE_NAME
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toTreeTable
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.google.protobuf.TextFormat
import io.reactivex.Observable
import java.time.Instant

class NoMessageCheckTask(
    description: String?,
    startTime: Instant,
    sessionKey: SessionKey,
    messageTimeout: Long? = null,
    timeout: Long,
    maxEventBatchContentSize: Int,
    protoPreFilter: PreFilter,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>
    ) : AbstractCheckTask(description, messageTimeout, timeout, maxEventBatchContentSize, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

    private val protoPreMessageFilter: RootMessageFilter = protoPreFilter.toRootMessageFilter()
    private val messagePreFilter = SailfishFilter(
        converter.fromProtoPreFilter(protoPreMessageFilter),
        protoPreMessageFilter.toCompareSettings()
    )

    private val metadataPreFilter: SailfishFilter? = protoPreMessageFilter.metadataFilterOrNull()?.let {
        SailfishFilter(
            converter.fromMetadataFilter(it, VerificationUtil.METADATA_MESSAGE_NAME),
            it.toComparisonSettings()
        )
    }

    private lateinit var preFilterEvent: Event
    private lateinit var resultEvent: Event

    private var preFilterMessagesCounter: Int = 0
    private var extraMessagesCounter: Int = 0

    init {
        rootEvent
            .name("No message check $sessionKey")
            .type("noMessageCheck")
    }

    override fun onStart() {
        super.onStart()
        preFilterEvent = Event.start()
            .type("preFiltering")
            .bodyData(protoPreMessageFilter.toTreeTable())
        rootEvent.addSubEvent(preFilterEvent)
        resultEvent = Event.start()
            .type("noMessagesCheckResult")
        rootEvent.addSubEvent(resultEvent)
    }

    override fun onNext(messageContainer: MessageContainer) {
        LOGGER.debug(
            "Received message with id: {}",
            TextFormat.shortDebugString(messageContainer.protoMessage.metadata.id)
        )
        val result = matchFilter(
            messageContainer,
            messagePreFilter,
            metadataPreFilter,
            matchNames = false,
            significant = false
        )
        val comparisonContainer = ComparisonContainer(messageContainer, protoPreMessageFilter, result)
        if (comparisonContainer.fullyMatches) {
            preFilterMessagesCounter++
            preFilterEvent.messageID(comparisonContainer.protoActual.metadata.id)
        } else {
            extraMessagesCounter++
            resultEvent.messageID(comparisonContainer.protoActual.metadata.id)
        }
    }

    override fun completeEvent(canceled: Boolean) {
        preFilterEvent.name("Prefilter: $preFilterMessagesCounter messages were filtered.")

        if (extraMessagesCounter == 0)
            resultEvent.status(Event.Status.PASSED).name("Check passed")
        else
            resultEvent.status(Event.Status.FAILED).name("Check failed: $extraMessagesCounter extra messages were found.")
    }

    private fun ProtoToIMessageConverter.fromProtoPreFilter(protoPreMessageFilter: RootMessageFilter): IMessage =
        fromProtoFilter(protoPreMessageFilter.messageFilter, PRE_FILTER_MESSAGE_NAME)

    private fun PreFilter.toRootMessageFilter() = RootMessageFilter.newBuilder()
        .setMessageType(PRE_FILTER_MESSAGE_NAME)
        .setMessageFilter(toMessageFilter())
        .also {
            if (hasMetadataFilter()) {
                it.metadataFilter = metadataFilter
            }
        }
        .build()

    private fun PreFilter.toMessageFilter() = MessageFilter.newBuilder()
        .putAllFields(fieldsMap)
        .build()
}