/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.check1.event.CheckSequenceUtils
import com.exactpro.th2.check1.event.bean.CheckSequenceRow
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.AggregatedFilterResult
import com.exactpro.th2.check1.rule.ComparisonContainer
import com.exactpro.th2.check1.rule.MessageContainer
import com.exactpro.th2.check1.rule.SailfishFilter
import com.exactpro.th2.check1.rule.preFilterBy
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.event.bean.builder.MessageBuilder
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toReadableBodyCollection
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import java.time.Instant
import kotlin.collections.HashSet
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
    ruleConfiguration: RuleConfiguration,
    startTime: Instant,
    sessionKey: SessionKey,
    protoPreFilter: PreFilter,
    protoMessageFilters: List<RootMessageFilter>,
    private val checkOrder: Boolean,
    parentEventID: EventID,
    messageStream: Observable<StreamContainer>,
    eventBatchRouter: MessageRouter<EventBatch>,
    onTaskFinished: ((EventStatus) -> Unit)? = null
) : AbstractCheckTask(ruleConfiguration, startTime, sessionKey, parentEventID, messageStream, eventBatchRouter) {

    protected class Refs(
        rootEvent: Event,
        onTaskFinished: ((EventStatus) -> Unit)?,
        val protoMessageFilters: List<RootMessageFilter>,
        val protoPreMessageFilter: RootMessageFilter,
        val messagePreFilter: SailfishFilter,
        val metadataPreFilter: SailfishFilter?,
    ) : AbstractCheckTask.Refs(rootEvent, onTaskFinished) {
        val preFilterEvent: Event by lazy {
            Event.start()
                .type("preFiltering")
                .bodyData(protoPreMessageFilter.toReadableBodyCollection())
        }

        val preFilteringResults: MutableMap<MessageID, ComparisonContainer> = LinkedHashMap()

        /**
         * List of filters which haven't matched yet. It is created from the requested filters and reduced after every match
         */
        lateinit var messageFilters: MutableList<MessageFilterContainer>

        val messageFilteringResults: MutableMap<MessageID, ComparisonContainer> = LinkedHashMap()
        val matchedByKeys: MutableSet<MessageFilterContainer> = HashSet(protoMessageFilters.size)
    }
    override val refsKeeper = RefsKeeper(protoPreFilter.toRootMessageFilter().let { protoPreMessageFilter ->
        Refs(
            rootEvent = createRootEvent(),
            onTaskFinished = onTaskFinished,
            protoMessageFilters = protoMessageFilters,
            protoPreMessageFilter = protoPreMessageFilter,
            messagePreFilter = SailfishFilter(
                CONVERTER.fromProtoPreFilter(protoPreMessageFilter),
                protoPreMessageFilter.toCompareSettings()
            ),
            metadataPreFilter = protoPreMessageFilter.metadataFilterOrNull()?.let {
                SailfishFilter(
                    CONVERTER.fromMetadataFilter(it, VerificationUtil.METADATA_MESSAGE_NAME),
                    it.toComparisonSettings()
                )
            }
        )
    })

    private val refs get() = refsKeeper.refs

    private var reordered: Boolean = false

    override fun onStartInit() {
        refs.messageFilters = refs.protoMessageFilters.map {
            MessageFilterContainer(
                it,
                SailfishFilter(CONVERTER.fromProtoPreFilter(it), it.toCompareSettings()),
                it.metadataFilterOrNull()?.let { metadataFilter ->
                    SailfishFilter(CONVERTER.fromMetadataFilter(metadataFilter, VerificationUtil.METADATA_MESSAGE_NAME),
                        metadataFilter.toComparisonSettings())
                }
            )
        }.toMutableList()

        refs.rootEvent.addSubEvent(refs.preFilterEvent)
    }

    override fun Observable<MessageContainer>.taskPipeline(): Observable<MessageContainer> =
        preFilterBy(this, refs.protoPreMessageFilter, refs.messagePreFilter, refs.metadataPreFilter, LOGGER) { preFilterContainer -> // Update pre-filter state
            with(preFilterContainer) {
                refs.preFilterEvent.appendEventsWithVerification(preFilterContainer)
                refs.preFilterEvent.messageID(protoActual.metadata.id)
                refs.preFilteringResults[protoActual.metadata.id] = preFilterContainer
            }
        }

    override fun onNext(messageContainer: MessageContainer) {
        for (index in refs.messageFilters.indices) {
            val messageFilterContainer = refs.messageFilters[index]

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

                refs.messageFilters.removeAt(index)

                refs.messageFilteringResults[messageContainer.protoMessage.metadata.id] = comparisonContainer
                refs.matchedByKeys.add(messageFilterContainer)

                requireNotNull(result.messageResult) {
                    "Message result must not be null because the result said the message is matched by key fields. Filter: " +
                        shortDebugString(messageFilterContainer.protoMessageFilter)
                }
                break
            }
        }

        val expectedMatches = refs.protoMessageFilters.size
        // rule has found complete match for all filters or each filter has found a match by key fields at least
        if (refs.messageFilters.isEmpty() || (refs.matchedByKeys.size == expectedMatches && refs.messageFilteringResults.size >= expectedMatches)) {
            checkComplete()
        }
    }

    override fun completeEvent(taskState: State) {
        refs.preFilterEvent.name("Pre-filtering (filtered ${refs.preFilteringResults.size} / processed $handledMessageCounter) messages")
        fillSequenceEvent()
        fillCheckMessagesEvent()
    }

    override fun name(): String = "Check sequence rule"

    override fun type(): String = "checkSequenceRule"

    override fun setup(rootEvent: Event) {
        rootEvent.bodyData(createMessageBean("Check sequence rule for messages from ${sessionKey.run { "$sessionAlias ($direction direction)"} }"))
    }

    /**
     * Creates events for check messages
     */
    private fun fillCheckMessagesEvent() {
        val checkMessagesEvent = refs.rootEvent.addSubEventWithSamePeriod()
            .name("Check messages")
            .type(CHECK_MESSAGES_TYPE)
            .appendEventWithVerificationsAndFilters(refs.protoMessageFilters, refs.messageFilteringResults.values)
        if (refs.protoMessageFilters.size != refs.messageFilteringResults.size) {
            refs.messageFilteringResults.values.map(ComparisonContainer::protoFilter)
            checkMessagesEvent.status(FAILED)
                .bodyData(createMessageBean("Incorrect number of comparisons (expected ${refs.protoMessageFilters.size} / actual ${refs.messageFilteringResults.size})"))
        } else {
            checkMessagesEvent.bodyData(createMessageBean("Contains comparisons"))
        }
    }

    /**
     * Creates events for check sequence
     */
    private fun fillSequenceEvent() {
        val sequenceTable = TableBuilder<CheckSequenceRow>()
        refs.preFilteringResults.forEach { (messageID: MessageID, comparisonContainer: ComparisonContainer) ->
            val container = refs.messageFilteringResults[messageID]
            sequenceTable.row(
                container?.let {
                    CheckSequenceUtils.createBothSide(it.sailfishActual, it.protoActual.metadata, it.protoFilter, sessionKey.sessionAlias)
                } ?: CheckSequenceUtils.createOnlyActualSide(comparisonContainer.sailfishActual, sessionKey.sessionAlias)
            )
        }
        refs.messageFilters.forEach { messageFilter: MessageFilterContainer ->
            sequenceTable.row(CheckSequenceUtils.createOnlyExpectedSide(messageFilter.protoMessageFilter, sessionKey.sessionAlias))
        }

        refs.rootEvent.addSubEventWithSamePeriod()
            .name("Check sequence (expected ${refs.protoMessageFilters.size} / actual ${refs.preFilteringResults.size} , check order $checkOrder)")
            .type("checkSequence")
            .status(if (refs.protoMessageFilters.size == refs.preFilteringResults.size
                && !(checkOrder && reordered)) PASSED else FAILED)
            .bodyData(MessageBuilder()
                .text("Expected ${refs.protoMessageFilters.size}, Actual ${refs.preFilteringResults.size}" +
                    if (checkOrder)
                        ", " + if (reordered) "Out of order"
                        else "In order"
                    else "")
                .build())
            .bodyData(sequenceTable.build())
    }

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
        const val CHECK_SEQUENCE_TYPE = "checkSequence"
    }
}