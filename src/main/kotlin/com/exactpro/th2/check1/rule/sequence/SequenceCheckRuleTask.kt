/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.event.CheckSequenceUtils
import com.exactpro.th2.check1.event.bean.CheckSequenceRow
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.AggregatedFilterResult
import com.exactpro.th2.check1.rule.ComparisonContainer
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.rule.getStatusType
import com.exactpro.th2.check1.util.VerificationUtil
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
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toTreeTable
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import java.time.Instant
import java.util.LinkedHashMap
import kotlin.collections.component1
import kotlin.collections.component2
import kotlin.collections.set

/**
 * This rule checks the sequence of specified messages.
 *
 * If **protoFilter** parameter is specified the messages will be pre-filtered before going to the actual comparison.
 *
 * If **checkOrder** parameter is set to `true` the messages must be received in the exact same order as filters were specified.
 * If this parameter is set to `false`, the order won't be checked.
 */
class SequenceCheckRuleTask(
    description: String?,
    startTime: Instant,
    sessionKey: SessionKey,
    timeout: Long,
    maxEventBatchContentSize: Int,
    protoPreFilter: PreFilter,
    private val protoMessageFilters: List<RootMessageFilter>,
    private val checkOrder: Boolean,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>
) : AbstractCheckTask(description, timeout, maxEventBatchContentSize, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

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
    private lateinit var preFilteringResults: MutableMap<MessageID, ComparisonContainer>

    private lateinit var messageFilters: MutableList<MessageFilterContainer>
    private lateinit var messageFilteringResults: MutableMap<MessageID, ComparisonContainer>

    private lateinit var preFilterEvent: Event

    private var reordered: Boolean = false
    private lateinit var matchedByKeys: MutableSet<MessageFilterContainer>

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
            MessageFilterContainer(
                it,
                SailfishFilter(converter.fromProtoFilter(it.messageFilter, it.messageType), it.toCompareSettings()),
                it.metadataFilterOrNull()?.let { metadataFilter ->
                    SailfishFilter(converter.fromMetadataFilter(metadataFilter, VerificationUtil.METADATA_MESSAGE_NAME),
                        metadataFilter.toComparisonSettings())
                }
            )
        }.toMutableList()

        matchedByKeys = HashSet(messageFilters.size)

        preFilterEvent = Event.start()
            .type("preFiltering")
            .bodyData(protoPreMessageFilter.toTreeTable())

        rootEvent.addSubEvent(preFilterEvent)
    }

    override fun Observable<MessageContainer>.taskPipeline(): Observable<MessageContainer> =
        map { messageContainer -> // Compare the message with pre-filter
            if (LOGGER.isDebugEnabled) {
                LOGGER.debug("Pre-filtering message with id: {}", shortDebugString(messageContainer.protoMessage.metadata.id))
            }
            val result = matchFilter(messageContainer, messagePreFilter, metadataPreFilter, matchNames = false, significant = false)
            ComparisonContainer(messageContainer, protoPreMessageFilter, result)
        }.filter { preFilterContainer -> // Filter  check result of pre-filter
            preFilterContainer.fullyMatches
        }.doOnNext { preFilterContainer -> // Update pre-filter state
            with(preFilterContainer) {
                preFilterEvent.appendEventsWithVerification(preFilterContainer)
                preFilterEvent.messageID(protoActual.metadata.id)

                preFilteringResults[protoActual.metadata.id] = preFilterContainer
            }
        }.map(ComparisonContainer::messageContainer)

    override fun onNext(messageContainer: MessageContainer) {
        for (index in messageFilters.indices) {
            val messageFilterContainer = messageFilters[index]

            val messageFilter: SailfishFilter = messageFilterContainer.messageFilter
            val metadataFilter: SailfishFilter? = messageFilterContainer.metadataFilter
            val result: AggregatedFilterResult = matchFilter(messageContainer, messageFilter, metadataFilter)

            val comparisonContainer = ComparisonContainer(
                messageContainer,
                messageFilterContainer.protoMessageFilter,
                result
            )
            if (comparisonContainer.matchesByKeys) {
                reordered = reordered || index != 0
                messageFilteringResults[messageContainer.protoMessage.metadata.id] = comparisonContainer
                matchedByKeys.add(messageFilterContainer)

                val comparisonStatus = requireNotNull(result.messageResult) {
                    "Message result must not be null because the result said the message is matched by key fields. Filter: " +
                        shortDebugString(messageFilterContainer.protoMessageFilter)
                }.getStatusType()

                if (checkOrder || comparisonStatus == StatusType.PASSED) {
                    messageFilters.removeAt(index)
                    break
                }
            }
        }

        val expectedMatches = protoMessageFilters.size
        // rule has found complete match for all filters or each filter has found a match by key fields at least
        if (messageFilters.isEmpty() || (matchedByKeys.size == expectedMatches && messageFilteringResults.size >= expectedMatches)) {
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
            .type(CHECK_MESSAGES_TYPE)
            .appendEventWithVerificationsAndFilters(protoMessageFilters, messageFilteringResults.values)
        if (protoMessageFilters.size != messageFilteringResults.size) {
            messageFilteringResults.values.map(ComparisonContainer::protoFilter)
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
            val container = messageFilteringResults[messageID]
            sequenceTable.row(
                container?.let {
                    CheckSequenceUtils.createBothSide(it.sailfishActual, it.protoActual.metadata, it.protoFilter, sessionKey.sessionAlias)
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

    private fun ProtoToIMessageConverter.fromProtoPreFilter(protoPreMessageFilter: RootMessageFilter): IMessage =
        fromProtoFilter(protoPreMessageFilter.messageFilter, PRE_FILTER_MESSAGE_NAME)

    private fun PreFilter.toCompareSettings() = toMessageFilter().toCompareSettings()

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

    companion object {
        const val PRE_FILTER_MESSAGE_NAME = "PreFilter"
        const val CHECK_MESSAGES_TYPE = "checkMessages"
    }
}
