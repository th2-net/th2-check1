/*
 * Copyright 2021-2025 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.rule.sequence

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.RuleConfiguration
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.OnTaskFinished
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.rule.preFilterBy
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.check1.utils.toRootMessageFilter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toReadableBodyCollection
import com.exactpro.th2.common.schema.message.MessageRouter
import io.reactivex.rxjava3.core.Observable
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

class SilenceCheckTask(
    ruleConfiguration: RuleConfiguration,
    protoPreFilter: PreFilter,
    submitTime: Instant,
    sessionKey: SessionKey,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>,
    onTaskFinished: OnTaskFinished = EMPTY_STATUS_CONSUMER
) : AbstractCheckTask(ruleConfiguration, submitTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

    protected class Refs(
        rootEvent: Event,
        onTaskFinished: OnTaskFinished,
        val protoPreMessageFilter: RootMessageFilter,
        val messagePreFilter: SailfishFilter,
        val metadataPreFilter: SailfishFilter?
    ) : AbstractCheckTask.Refs(rootEvent, onTaskFinished) {
        val preFilterEvent: Event by lazy {
            Event.start()
                .type("preFiltering")
                .bodyData(protoPreMessageFilter.toReadableBodyCollection())
        }
        val resultEvent: Event by lazy {
            Event.start()
                .type("noMessagesCheckResult")
        }
    }

    override val refsKeeper = RefsKeeper(protoPreFilter.toRootMessageFilter().let { protoPreMessageFilter ->
        Refs(
            rootEvent = createRootEvent(),
            onTaskFinished = onTaskFinished,
            protoPreMessageFilter = protoPreMessageFilter,
            messagePreFilter = SailfishFilter(
                PROTO_CONVERTER.fromProtoPreFilter(protoPreMessageFilter),
                protoPreMessageFilter.toCompareSettings()
            ),
            metadataPreFilter = protoPreMessageFilter.metadataFilterOrNull()?.let {
                SailfishFilter(
                    PROTO_CONVERTER.fromMetadataFilter(it, VerificationUtil.METADATA_MESSAGE_NAME),
                    it.toComparisonSettings()
                )
            }
        )
    })

    private val refs get() = refsKeeper.refs
    private var extraMessagesCounter: Int = 0

    private val isCanceled = AtomicBoolean()

    override fun onStartInit() {
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

        with(refs) {
            rootEvent.addSubEvent(preFilterEvent)
            rootEvent.addSubEvent(resultEvent)
        }
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

    override val errorEventOnTimeout: Boolean
        get() = false

    override fun name(): String = "AutoSilenceCheck"

    override fun type(): String = "AutoSilenceCheck"

    override fun setup(rootEvent: Event) {
        rootEvent.bodyData(createMessageBean("AutoSilenceCheck for session ${sessionKey.run { "$bookName $sessionAlias ($direction)" }}"))
    }

    override fun Observable<MessageContainer>.taskPipeline(): Observable<MessageContainer> =
        preFilterBy(
            this,
            refs.protoPreMessageFilter,
            refs.messagePreFilter,
            refs.metadataPreFilter,
            LOGGER
        ) { preFilterContainer -> // Update pre-filter state
            with(preFilterContainer) {
                refs.preFilterEvent.appendEventsWithVerification(preFilterContainer)
                refs.preFilterEvent.messageID(holderActual.id)
            }
        }

    override fun onNext(container: MessageContainer) {
        container.messageHolder.apply {
            extraMessagesCounter++
            refs.resultEvent.messageID(id)
        }
    }

    override fun completeEvent(taskState: State) {
        if (skipPublication) {
            return
        }

        refs.preFilterEvent.name("Prefilter: $extraMessagesCounter messages were filtered.")

        if (extraMessagesCounter == 0) {
            refs.resultEvent.status(Event.Status.PASSED).name("Check passed")
        } else {
            refs.resultEvent.status(Event.Status.FAILED)
                .name("Check failed: $extraMessagesCounter extra messages were found.")
        }
    }

    override val skipPublication: Boolean
        get() = isCanceled.get()

    private fun cancel() {
        if (isCanceled.compareAndSet(false, true)) {
            checkComplete()
        } else {
            LOGGER.debug("Task {} '{}' already canceled", type(), description)
        }
    }
}