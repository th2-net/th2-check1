/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.ComparisonContainer
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.check1.utils.toRootMessageFilter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toReadableBodyCollection
import com.exactpro.th2.common.schema.message.MessageRouter
import io.reactivex.Observable
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

class SilenceCheckTask(
    protoPreFilter: PreFilter,
    description: String?,
    taskTimeout: TaskTimeout,
    maxEventBatchContentSize: Int,
    submitTime: Instant,
    sessionKey: SessionKey,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>
) : AbstractCheckTask(
    description,
    taskTimeout,
    maxEventBatchContentSize,
    submitTime,
    sessionKey,
    parentEventID,
    messageStream,
    eventBatchRouter
) {
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
    private var extraMessagesCounter: Int = 0

    @Volatile
    private var started = false
    private val isCanceled = AtomicBoolean()

    override fun onStart() {
        super.onStart()
        started = true
        val hasNextTask = hasNextTask()
        if (isParentCompleted == false || hasNextTask) {
            if (hasNextTask) {
                LOGGER.info("Has subscribed task. Skip checking extra messages")
            } else {
                LOGGER.info("Parent task was not finished normally. Skip checking extra messages")
            }
            cancel()
            return
        }
        preFilterEvent = Event.start()
            .type("preFiltering")
            .bodyData(protoPreMessageFilter.toReadableBodyCollection())

        rootEvent.addSubEvent(preFilterEvent)

        resultEvent = Event.start()
            .type("noMessagesCheckResult")
        rootEvent.addSubEvent(resultEvent)
    }

    override fun onChainedTaskSubscription() {
        if (started) { // because we cannot cancel task before it is actually started
            cancel()
        } else {
            if (LOGGER.isInfoEnabled) {
                LOGGER.info("The ${type()} task '$description' will be automatically canceled when it begins")
            }
        }
    }

    override fun name(): String = "AutoSilenceCheck"

    override fun type(): String = "AutoSilenceCheck"

    override fun setup(rootEvent: Event) {
        rootEvent.bodyData(createMessageBean("AutoSilenceCheck for session ${sessionKey.run { "$sessionAlias ($direction)" }}"))
    }

    override fun Observable<MessageContainer>.taskPipeline(): Observable<MessageContainer> =
        map { messageContainer -> // Compare the message with pre-filter
            if (LOGGER.isDebugEnabled) {
                LOGGER.debug("Pre-filtering message with id: {}", messageContainer.protoMessage.metadata.id.toJson())
            }
            val result = matchFilter(messageContainer, messagePreFilter, metadataPreFilter, matchNames = false, significant = false)
            ComparisonContainer(messageContainer, protoPreMessageFilter, result)
        }.filter { preFilterContainer -> // Filter  check result of pre-filter
            preFilterContainer.fullyMatches
        }.doOnNext { preFilterContainer -> // Update pre-filter state
            with(preFilterContainer) {
                preFilterEvent.appendEventsWithVerification(preFilterContainer)
                preFilterEvent.messageID(protoActual.metadata.id)
            }
        }.map(ComparisonContainer::messageContainer)

    override fun onNext(container: MessageContainer) {
        container.protoMessage.metadata.apply {
            extraMessagesCounter++
            resultEvent.messageID(id)
        }
    }

    override fun completeEvent(taskState: State): Boolean {
        if (isCanceled.get()) {
            return false
        }
        preFilterEvent.name("Prefilter: $extraMessagesCounter messages were filtered.")

        if (extraMessagesCounter == 0) {
            resultEvent.status(Event.Status.PASSED).name("Check passed")
        } else {
            resultEvent.status(Event.Status.FAILED)
                .name("Check failed: $extraMessagesCounter extra messages were found.")
        }
        return true
    }

    private fun cancel() {
        if (isCanceled.compareAndSet(false, true)) {
            checkComplete()
        } else {
            LOGGER.debug("Task {} '{}' already canceled", type(), description)
        }
    }
}