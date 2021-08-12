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
package com.exactpro.th2.check1

import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.entities.Checkpoint
import com.exactpro.th2.check1.entities.CheckpointData
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.grpc.*
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.check.CheckRuleTask
import com.exactpro.th2.check1.rule.nomessage.NoMessageCheckTask
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.schema.message.MessageListener
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool

class CollectorService(
    private val messageRouter: MessageRouter<MessageBatch>, private val eventBatchRouter: MessageRouter<EventBatch>, configuration: Check1Configuration
) {

    private val logger = LoggerFactory.getLogger(javaClass.name + '@' + hashCode())

    /**
     * Queue name to subscriber. Messages with different connectivity can be transferred with one queue.
     */
    private val subscriberMonitor: SubscriberMonitor
    private val streamObservable: Observable<StreamContainer>
    private val checkpointSubscriber: CheckpointSubscriber
    private val mqSubject: PublishSubject<MessageBatch>
    private val eventIdToLastCheckTask: MutableMap<CheckTaskKey, AbstractCheckTask> = ConcurrentHashMap()

    private val olderThanDelta = configuration.cleanupOlderThan
    private val olderThanTimeUnit = configuration.cleanupTimeUnit
    private val maxEventBatchContentSize = configuration.maxEventBatchContentSize

    init {
        val limitSize = configuration.messageCacheSize
        mqSubject = PublishSubject.create()

        subscriberMonitor = subscribe(MessageListener { _: String, batch: MessageBatch -> mqSubject.onNext(batch) })
        streamObservable = mqSubject.flatMapIterable(MessageBatch::getMessagesList)
            .groupBy { message -> message.metadata.id.run { SessionKey(connectionId.sessionAlias, direction) } }
            .map { group -> StreamContainer(group.key!!, limitSize, group) }
            .replay().apply { connect() }

        checkpointSubscriber = streamObservable.subscribeWith(CheckpointSubscriber())
    }

    @Throws(InterruptedException::class)
    fun verifyCheckRule(request: CheckRuleRequest): ChainID {
        check(request.hasParentEventId()) { "Parent event id can't be null" }
        val parentEventID: EventID = request.parentEventId
        check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
        val sessionAlias: String = request.connectivityId.sessionAlias

        check(request.kindCase != CheckRuleRequest.KindCase.KIND_NOT_SET) {
            "Either old filter or root filter must be set"
        }
        val filter: RootMessageFilter = if (request.hasRootFilter()) {
            request.rootFilter
        } else {
            request.filter.toRootMessageFilter()
        }
        val direction = directionOrDefault(request.direction)

        val chainID = request.getChainIdOrGenerate()

        val task = CheckRuleTask(request.description, Instant.now(), SessionKey(sessionAlias, direction),
            TaskTimeout(request.timeout, request.messageTimeout), maxEventBatchContentSize,
            filter, parentEventID, streamObservable, eventBatchRouter)

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    @Throws(InterruptedException::class)
    fun verifyCheckSequenceRule(request: CheckSequenceRuleRequest): ChainID {
        check(request.hasParentEventId()) { "Parent event id can't be null" }
        val parentEventID: EventID = request.parentEventId
        check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
        val sessionAlias: String = request.connectivityId.sessionAlias
        val direction = directionOrDefault(request.direction)

        check((request.messageFiltersList.isEmpty() && request.rootMessageFiltersList.isNotEmpty())
                || (request.messageFiltersList.isNotEmpty() && request.rootMessageFiltersList.isEmpty())) {
            "Either messageFilters or rootMessageFilters must be set but not both"
        }

        val chainID = request.getChainIdOrGenerate()

        val protoMessageFilters: List<RootMessageFilter> = if (request.rootMessageFiltersList.isNotEmpty()) {
            request.rootMessageFiltersList
        } else {
            request.messageFiltersList.map { it.toRootMessageFilter() }
        }
        
        val task = SequenceCheckRuleTask(request.description, Instant.now(), SessionKey(sessionAlias, direction),
            TaskTimeout(request.timeout, request.messageTimeout), maxEventBatchContentSize, request.preFilter,
            protoMessageFilters, request.checkOrder, parentEventID, streamObservable, eventBatchRouter)

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    fun verifyNoMessageCheck(request: NoMessageCheckRequest): ChainID {
        check(request.hasParentEventId()) { "Parent event id can't be null" }
        val parentEventID: EventID = request.parentEventId
        check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
        val sessionAlias: String = request.connectivityId.sessionAlias
        val direction = directionOrDefault(request.direction)
        val chainID = request.getChainIdOrGenerate()

        val task = NoMessageCheckTask(
            request.description,
            Instant.now(),
            SessionKey(sessionAlias, direction),
            TaskTimeout(request.timeout, request.messageTimeout),
            maxEventBatchContentSize,
            request.preFilter,
            parentEventID,
            streamObservable,
            eventBatchRouter
        )

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    private fun MessageFilter.toRootMessageFilter(): RootMessageFilter {
        return RootMessageFilter.newBuilder()
                .setMessageType(this.messageType)
                .setComparisonSettings(this.comparisonSettings.toRootComparisonSettings())
                .setMessageFilter(this)
                .build()
    }

    private fun ComparisonSettings.toRootComparisonSettings(): RootComparisonSettings {
        return RootComparisonSettings.newBuilder()
                .addAllIgnoreFields(this.ignoreFieldsList)
                .build()
    }


    private fun directionOrDefault(direction: Direction) =
        if (direction == Direction.UNRECOGNIZED) Direction.FIRST else direction

    private fun AbstractCheckTask.addToChainOrBegin(
            value: AbstractCheckTask?,
            checkpoint: com.exactpro.th2.common.grpc.Checkpoint
    ): Unit = value?.subscribeNextTask(this) ?: begin(checkpoint)

    private fun CheckRuleRequest.getChainIdOrGenerate(): ChainID {
        return if (hasChainId()) {
            chainId
        } else {
            generateChainID()
        }
    }

    private fun CheckSequenceRuleRequest.getChainIdOrGenerate(): ChainID {
        return if (hasChainId()) {
            chainId
        } else {
            generateChainID()
        }
    }

    private fun NoMessageCheckRequest.getChainIdOrGenerate(): ChainID {
        return if (hasChainId()) {
            chainId
        } else {
            generateChainID()
        }
    }

    private fun generateChainID() = ChainID.newBuilder().setId(EventUtils.generateUUID()).build()

    private fun cleanupTasksOlderThan(delta: Long, unit: ChronoUnit = ChronoUnit.SECONDS) {
        val now = Instant.now()
        eventIdToLastCheckTask.values.removeIf { task ->
            val endTime = task.endTime
            when {
                !olderThan(now, delta, unit, endTime) -> false
                task.tryShutdownExecutor() -> {
                    logger.info("Removed task ${task.description} ($endTime) from tasks map")
                    true
                }
                else -> {
                    logger.warn("Task ${task.description} can't be removed because it has a continuation")
                    false
                }
            }
        }
    }

    private fun olderThan(now: Instant?, delta: Long, unit: ChronoUnit, endTime: Instant?) =
        endTime != null && unit.between(endTime, now) > delta

    @Throws(JsonProcessingException::class)
    private fun sendEvents(parentEventID: EventID, event: Event) {
        logger.debug("Sending event thee id '{}' parent id '{}'", event.id, parentEventID)

        val batch = EventBatch.newBuilder()
            .setParentEventId(parentEventID)
            .addAllEvents(event.toProtoEvents(parentEventID.id))
            .build()

        ForkJoinPool.commonPool().execute {
            try {
                eventBatchRouter.send(batch, "publish", "event")
                if (logger.isDebugEnabled) {
                    logger.debug("Sent event batch '{}'", shortDebugString(batch))
                }
            } catch (e: Exception) {
                logger.error("Can not send event batch '{}'", shortDebugString(batch), e)
            }
        }
    }

    fun createCheckpoint(request: CheckpointRequestOrBuilder): Checkpoint {
        val rootEvent = Event.start()
            .name("Checkpoint")
            .type("Checkpoint")
            .description(request.description)
        return try {
            val checkpoint = checkpointSubscriber.createCheckpoint()
            rootEvent.endTimestamp()
                .bodyData(EventUtils.createMessageBean("Checkpoint id '${checkpoint.id}'"))
            checkpoint.sessionKeyToCheckpointData.forEach { (sessionKey: SessionKey, checkpointData: CheckpointData) ->
                val messageID = sessionKey.toMessageID(checkpointData.sequence)
                rootEvent.messageID(messageID)
                    .addSubEventWithSamePeriod()
                    .name("Checkpoint for session alias '${sessionKey.sessionAlias}' direction '${sessionKey.direction}' sequence '${checkpointData.sequence}'")
                    .type("Checkpoint for session")
                    .messageID(messageID)
            }
            checkpoint
        } finally {
            try {
                if (request.hasParentEventId()) {
                    sendEvents(request.parentEventId, rootEvent)
                } else {
                    logger.warn("Parent id missed in request")
                }
            } catch (e: Exception) {
                logger.error("Sending events '{}' with a parent '{}' failed ",
                    rootEvent, request.parentEventId, e)
            }
        }
    }

    fun close() {
        try {
            subscriberMonitor.unsubscribe()
        } catch (e: IOException) {
            logger.error("Close subscriber failure", e)
        }
        mqSubject.onComplete()
    }

    private fun subscribe(listener: MessageListener<MessageBatch>): SubscriberMonitor {
        return checkNotNull(messageRouter.subscribeAll(listener)) { "Can not subscribe to queues" }
    }

    private fun SessionKey.toMessageID(sequence: Long) = MessageID.newBuilder()
        .setConnectionId(ConnectionID.newBuilder()
            .setSessionAlias(sessionAlias)
            .build())
        .setSequence(sequence)
        .setDirection(direction)
        .build()
}
