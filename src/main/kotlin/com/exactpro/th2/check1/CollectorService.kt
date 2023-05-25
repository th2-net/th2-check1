/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.check1.grpc.CheckpointRequestOrBuilder
import com.exactpro.th2.check1.grpc.NoMessageCheckRequest
import com.exactpro.th2.check1.metrics.BufferMetric
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.RuleFactory
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.message.MessageWrapper
import com.exactpro.th2.common.utils.message.proto.ProtoMessageWrapper
import com.exactpro.th2.common.utils.message.transport.TransportMessageWrapper
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import com.exactpro.th2.common.grpc.Checkpoint as GrpcCheckpoint

class CollectorService(
    protoMessageRouter: MessageRouter<MessageBatch>,
    transportMessageRouter: MessageRouter<GroupBatch>,
    private val eventBatchRouter: MessageRouter<EventBatch>,
    configuration: Check1Configuration
) {

    private val logger = LoggerFactory.getLogger(javaClass.name + '@' + hashCode())

    /**
     * Queue name to subscriber. Messages with different connectivity can be transferred with one queue.
     */
    private val protoSubscriberMonitor: SubscriberMonitor
    private val transportSubscriberMonitor: SubscriberMonitor
    private val streamObservable: Observable<StreamContainer>
    private val checkpointSubscriber: CheckpointSubscriber
    private val mqSubject: PublishSubject<MessageWrapper>
    private val eventIdToLastCheckTask: MutableMap<CheckTaskKey, AbstractCheckTask> = ConcurrentHashMap()

    private val olderThanDelta = configuration.cleanupOlderThan
    private val olderThanTimeUnit = configuration.cleanupTimeUnit
    private val defaultAutoSilenceCheck: Boolean = configuration.isAutoSilenceCheckAfterSequenceRule

    private var ruleFactory: RuleFactory

    init {
        BufferMetric.configure(configuration)

        val limitSize = configuration.messageCacheSize
        mqSubject = PublishSubject.create()
        protoSubscriberMonitor =
            checkNotNull(protoMessageRouter.subscribeAll({ _: DeliveryMetadata, batch: MessageBatch ->
                batch.messagesList.forEach {
                    mqSubject.onNext(ProtoMessageWrapper(it))
                }
            })) { "Can not subscribe for listening protobuf messages" }
        transportSubscriberMonitor =
            checkNotNull(transportMessageRouter.subscribeAll({ _: DeliveryMetadata, batch: GroupBatch ->
                val book = batch.book
                val sessionGroup = batch.sessionGroup
                batch.groups.asSequence()
                    .flatMap(MessageGroup::messages)
                    .forEach { message ->
                        when (message) {
                            is ParsedMessage -> mqSubject.onNext(TransportMessageWrapper(message, book, sessionGroup))
                            else -> error("Transport group contains not parsed message $message")
                        }
                    }
            })) { "Can not subscribe for listening transport messages" }
        streamObservable = mqSubject.groupBy { wrapper ->
            wrapper.id.run {
                SessionKey(wrapper.id.bookName, connectionId.sessionAlias, direction)
            }.also(BufferMetric::processMessage)
        }
            .map { group -> StreamContainer(group.key!!, limitSize, group) }
            .replay().apply { connect() }

        checkpointSubscriber = streamObservable.subscribeWith(CheckpointSubscriber())

        ruleFactory = RuleFactory(configuration, streamObservable, eventBatchRouter)
    }

    @Throws(InterruptedException::class)
    fun verifyCheckRule(request: CheckRuleRequest): ChainID {
        val chainID = request.getChainIdOrGenerate()

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            val task = ruleFactory.createCheckRule(request, value != null)
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    @Throws(InterruptedException::class)
    fun verifyCheckSequenceRule(request: CheckSequenceRuleRequest): ChainID {
        val chainID = request.getChainIdOrGenerate()

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)
        val silenceCheck = if (request.hasSilenceCheck()) request.silenceCheck.value else defaultAutoSilenceCheck

        val silenceCheckTask: AbstractCheckTask? = if (silenceCheck) {
            ruleFactory.createSilenceCheck(request, olderThanTimeUnit.duration.toMillis() * olderThanDelta)
        } else {
            null
        }

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            val task = ruleFactory.createSequenceCheckRule(request, value != null)
            task.apply { addToChainOrBegin(value, request.checkpoint) }
                .run { silenceCheckTask?.also { subscribeNextTask(it) } ?: this }
        }
        return chainID
    }

    fun verifyNoMessageCheck(request: NoMessageCheckRequest): ChainID {
        val chainID = request.getChainIdOrGenerate()

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            val task = ruleFactory.createNoMessageCheckRule(request, value != null)
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    private fun AbstractCheckTask.addToChainOrBegin(value: AbstractCheckTask?, checkpoint: GrpcCheckpoint) {
        val realCheckpoint = if (checkpoint === GrpcCheckpoint.getDefaultInstance()) {
            null
        } else {
            checkpoint
        }
        value?.subscribeNextTask(this) ?: begin(realCheckpoint)
    }

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
            .addAllEvents(event.toListProto(parentEventID))
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
                val messageID = sessionKey.toMessageID(checkpointData)
                rootEvent.messageID(messageID)
                    .addSubEventWithSamePeriod()
                    .name("Checkpoint for book name '${sessionKey.bookName}', session alias '${sessionKey.sessionAlias}', direction '${sessionKey.direction}' sequence '$checkpointData.sequence'")
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
                logger.error(
                    "Sending events '{}' with a parent '{}' failed ",
                    rootEvent, request.parentEventId, e
                )
            }
        }
    }

    fun close() {
        runCatching(protoSubscriberMonitor::unsubscribe)
            .onFailure { logger.error("Close protobuf subscriber failure", it) }
        runCatching(transportSubscriberMonitor::unsubscribe)
            .onFailure { logger.error("Close transport subscriber failure", it) }
        mqSubject.onComplete()
    }

    private fun SessionKey.toMessageID(data: CheckpointData) = MessageID.newBuilder()
        .setBookName(bookName)
        .setConnectionId(
            ConnectionID
                .newBuilder()
                .setSessionAlias(sessionAlias)
                .build()
        )
        .setTimestamp(requireNotNull(data.timestamp) { "timestamp is not set for session $this" })
        .setSequence(data.sequence)
        .setDirection(direction)
        .build()
}
