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

package com.exactpro.th2.check1.rule.nomessage

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.check1.utils.FilterUtils
import com.exactpro.th2.check1.utils.FilterUtils.fullMatch
import com.exactpro.th2.check1.utils.fromProtoPreFilter
import com.exactpro.th2.check1.utils.toRootMessageFilter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toTreeTable
import com.exactpro.th2.common.schema.message.MessageRouter
import io.reactivex.Observable
import java.time.Instant

class NoMessageCheckTask(
    description: String?,
    startTime: Instant,
    sessionKey: SessionKey,
    taskTimeout: TaskTimeout,
    maxEventBatchContentSize: Int,
    protoPreFilter: PreFilter,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>
    ) : AbstractCheckTask(description, taskTimeout, maxEventBatchContentSize, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

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
        if (LOGGER.isDebugEnabled) {
            LOGGER.debug("Received message with id: {}", messageContainer.protoMessage.metadata.id.toJson())
        }

        val result = matchFilter(
            messageContainer,
            messagePreFilter,
            metadataPreFilter,
            matchNames = false,
            significant = false
        )
        messageContainer.protoMessage.metadata.apply {
            if (FilterUtils.allMatches(result, protoPreMessageFilter) { it.fullMatch }) {
                extraMessagesCounter++
                resultEvent.messageID(id)
            } else {
                preFilterMessagesCounter++
                preFilterEvent.messageID(id)
            }
        }
    }

    override fun completeEvent(taskState: State) {
        preFilterEvent.name("Prefilter: $preFilterMessagesCounter messages were filtered.")

        if (extraMessagesCounter == 0) {
            resultEvent.status(Event.Status.PASSED).name("Check passed")
        } else {
            resultEvent.status(Event.Status.FAILED)
                .name("Check failed: $extraMessagesCounter extra messages were found.")
        }

        resultEvent.description("Task has been completed because: {$taskState}")
    }
}