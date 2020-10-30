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

package com.exactpro.th2.check1.rule.sequence

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.event.CheckSequenceUtils
import com.exactpro.th2.check1.event.bean.CheckSequenceRow
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.event.bean.builder.MessageBuilder
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import io.reactivex.Observable
import java.time.Instant
import java.util.LinkedHashMap

/**
 * This rule checks the sequence of specified messages.
 *
 * If **protoFilter** parameter is specified the messages will be pre-filtered before going to the actual comparison.
 *
 * If **checkOrder** parameter is set to `true` the messages must be received in the exact same order as filters were specified.
 * If this parameter is `false` the order won't be checked.
 */
class SequenceCheckRuleTask(
        description: String?,
        startTime: Instant,
        sessionKey: SessionKey,
        timeout: Long,
        protoPreFilter: PreFilter,
        private val protoMessageFilters: List<MessageFilter>,
        private val checkOrder: Boolean,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>,
        eventBatchRouter: MessageRouter<EventBatch>
) : AbstractCheckTask(description, timeout, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

    private val protoPreMessageFilter: MessageFilter = protoPreFilter.toMessageFilter()
    private val preFilter: IMessage = converter.fromProtoPreFilter(protoPreMessageFilter)
    private val settingsPreFilter: ComparatorSettings = protoPreFilter.toCompareSettings()
    private lateinit var preFilteringResults: MutableMap<MessageID, ComparisonContainer>

    private lateinit var messageFilters: MutableList<MessageFilterContainer>
    private lateinit var messageFilteringResults: MutableMap<MessageID, ComparisonContainer>

    private lateinit var preFilterEvent: Event

    private var reordered: Boolean = false

    init {
        rootEvent
            .name("Check sequence rule $sessionKey")
            .type("checkSequenceRule")
    }

    override fun onStart() {
        super.onStart()

        //Init or re-init variable in TASK_SCHEDULER thread
        preFilteringResults = LinkedHashMap()

        messageFilteringResults = LinkedHashMap()
        messageFilters = protoMessageFilters.map {
            MessageFilterContainer(it, converter.fromProtoFilter(it, it.messageType), it.toCompareSettings())
        }.toMutableList()

        preFilterEvent = Event.start()
            .type("preFiltering")
            .bodyData(createMessageBean("Passed pre-filter will be placed there")) // TODO: Write filter

        rootEvent.addSubEvent(preFilterEvent)
    }

    override fun Observable<MessageContainer>.taskPipeline(): Observable<MessageContainer> =
        map { messageContainer -> // Compare the message with pre-filter
            val comparisonResult = MessageComparator.compare(messageContainer.sailfishMessage, preFilter, settingsPreFilter, false)
            LOGGER.debug("Pre-filter compare message '{}' result\n{}", messageContainer.sailfishMessage.name, comparisonResult)
            ComparisonContainer(messageContainer, protoPreMessageFilter, comparisonResult)
        }.filter { preFilterContainer -> // Filter  check result of pre-filter
            preFilterContainer.comparisonResult?.getStatusType() != StatusType.FAILED
        }.doOnNext { preFilterContainer -> // Update pre-filter state
            with(preFilterContainer.messageContainer.protoMessage) {
                preFilterEvent.addSubEventWithSamePeriod()
                    .appendEventWithVerification(this, protoPreMessageFilter, preFilterContainer.comparisonResult!!)
                preFilterEvent.messageID(metadata.id)

                preFilteringResults[metadata.id] = preFilterContainer
            }
        }.map(ComparisonContainer::messageContainer)

    override fun onNext(messageContainer: MessageContainer) {
        for (index in messageFilters.indices) {
            val messageFilter = messageFilters[index]
            val comparisonResult = MessageComparator.compare(
                messageContainer.sailfishMessage,
                messageFilter.sailfishMessageFilter,
                messageFilter.comparatorSettings
            )

            if (comparisonResult != null) {
                val comparisonStatus = comparisonResult.getStatusType()
                reordered = reordered || index != 0 || comparisonStatus != StatusType.PASSED
                messageFilteringResults[messageContainer.protoMessage.metadata.id] = ComparisonContainer(
                    messageContainer,
                    messageFilter.protoMessageFilter,
                    comparisonResult
                )
                if (checkOrder || comparisonStatus == StatusType.PASSED) {
                    messageFilters.removeAt(index)
                    break
                }
            }
        }

        if (messageFilters.isEmpty()) {
            checkComplete()
        }
    }

    override fun completeEvent(canceled: Boolean) {
        preFilterEvent.name("Pre-filtering (filtered ${preFilteringResults.size} / processed $handledMessageCounter) messages")

        fillSequenceEvent()
        fillCheckMessagesEvent()
    }

    /**
     * Creates events for check messages
     */
    private fun fillCheckMessagesEvent() {
        val checkMessagesEvent = rootEvent.addSubEventWithSamePeriod()
            .name("Check messages")
            .type("checkMessages")
            .appendEventWithVerifications(messageFilteringResults.values)
        if (protoMessageFilters.size != messageFilteringResults.size) {
            checkMessagesEvent.status(FAILED)
                .bodyData(createMessageBean("Incorrect number of comparisons (expected ${protoMessageFilters.size} / actual ${messageFilteringResults.size})"))
        } else {
            checkMessagesEvent.bodyData(createMessageBean("Contains comparisons"))
        }
    }

    /**
     * Creates events for check sequence
     */
    private fun fillSequenceEvent() {
        val sequenceTable = TableBuilder<CheckSequenceRow>()
        preFilteringResults.forEach { (messageID: MessageID, comparisonContainer: ComparisonContainer) ->
            sequenceTable.row(
                messageFilteringResults[messageID]?.let {
                    CheckSequenceUtils.createBothSide(it.sailfishActual, it.protoFilter, sessionKey.sessionAlias)
                } ?: CheckSequenceUtils.createOnlyActualSide(comparisonContainer.sailfishActual, sessionKey.sessionAlias)
            )
        }
        messageFilters.forEach { messageFilter: MessageFilterContainer ->
            sequenceTable.row(CheckSequenceUtils.createOnlyExpectedSide(messageFilter.protoMessageFilter, sessionKey.sessionAlias))
        }

        rootEvent.addSubEventWithSamePeriod()
            .name("Check sequence (expected ${protoMessageFilters.size} / actual ${preFilteringResults.size} , check order $checkOrder)")
            .type("checkSequence")
            .status(if (protoMessageFilters.size == preFilteringResults.size
                && !(checkOrder && reordered)) PASSED else FAILED)
            .bodyData(MessageBuilder()
                .text("Expected ${protoMessageFilters.size}, Actual ${preFilteringResults.size}" +
                    if (checkOrder)
                        ", " + if (reordered) "Out of order"
                        else "In order"
                    else "")
                .build())
            .bodyData(sequenceTable.build())
    }

    private fun ProtoToIMessageConverter.fromProtoPreFilter(protoPreMessageFilter: MessageFilter): IMessage =
        fromProtoFilter(protoPreMessageFilter, PRE_FILTER_MESSAGE_NAME)

    private fun PreFilter.toCompareSettings() = toMessageFilter().toCompareSettings()

    private fun PreFilter.toMessageFilter() = MessageFilter.newBuilder()
        .setMessageType(PRE_FILTER_MESSAGE_NAME)
        .putAllFields(fieldsMap)
        .build()

    companion object {
        const val PRE_FILTER_MESSAGE_NAME = "PreFilter"
    }
}
