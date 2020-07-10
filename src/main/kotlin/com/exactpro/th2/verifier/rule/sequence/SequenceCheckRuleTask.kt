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

package com.exactpro.th2.verifier.rule.sequence

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonUtil.getStatusType
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.event.bean.builder.MessageBuilder
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.verifier.StreamContainer
import com.exactpro.th2.verifier.event.CheckSequenceUtils
import com.exactpro.th2.verifier.event.bean.CheckSequenceRow
import com.exactpro.th2.verifier.grpc.PreFilter
import com.exactpro.th2.verifier.rule.AbstractCheckTask
import com.exactpro.th2.verifier.rule.MessageContainer
import io.reactivex.Observable
import java.time.Instant
import java.util.LinkedHashMap

/**
 *
 */
class SequenceCheckRuleTask(description: String?,
                            startTime: Instant,
                            sessionAlias: String,
                            timeout: Long,
                            protoPreFilter: PreFilter,
                            private val protoMessageFilters: List<MessageFilter>,
                            private val checkOrder: Boolean,
                            parentEventID: EventID,
                            messageStream: Observable<StreamContainer>,
                            eventStoreStub: EventStoreServiceFutureStub) : AbstractCheckTask(description, timeout, startTime, sessionAlias, parentEventID, messageStream, eventStoreStub) {

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
            .name("Check sequence rule $sessionAlias")
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
        reordered = false

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
            with(preFilterContainer.comparisonResult) {
                this != null && getStatusType(this) != StatusType.FAILED
            }
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
            val comparisonResult = MessageComparator.compare(messageContainer.sailfishMessage, messageFilter.sailfishMessageFilter, messageFilter.comparatorSettings)

            if (comparisonResult != null) {
                reordered = reordered && index == 0
                messageFilteringResults[messageContainer.protoMessage.metadata.id] = ComparisonContainer(messageContainer, messageFilter.protoMessageFilter, comparisonResult)
                messageFilters.removeAt(index)
                break
            }
        }

        if (messageFilters.isEmpty()) {
            fillEvent()
            checkComplete()
        }
    }

    override fun onTimeout() {
        fillEvent()
    }

    private fun fillEvent() {
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
            when (val messageFilteringResult = messageFilteringResults[messageID]) {
                null -> sequenceTable.row(CheckSequenceUtils.createOnlyActualSide(comparisonContainer.sailfishActual, sessionAlias))
                else -> sequenceTable.row(CheckSequenceUtils.createBothSide(messageFilteringResult.sailfishActual, messageFilteringResult.protoFilter, sessionAlias))
            }
        }
        messageFilters.forEach { messageFilter: MessageFilterContainer ->
            sequenceTable.row(CheckSequenceUtils.createOnlyExpectedSide(messageFilter.protoMessageFilter, sessionAlias))
        }

        rootEvent.addSubEventWithSamePeriod()
            .name("Check sequence (expected ${protoMessageFilters.size} / actual ${preFilteringResults.size} , check order " + checkOrder + ')')
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