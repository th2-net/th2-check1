/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.verifier

import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.eventstore.grpc.AsyncEventStoreServiceService
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest
import com.exactpro.th2.infra.grpc.ConnectionID
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.schema.grpc.router.GrpcRouter
import com.exactpro.th2.schema.message.MessageListener
import com.exactpro.th2.schema.message.MessageRouter
import com.exactpro.th2.schema.message.SubscriberMonitor
import com.exactpro.th2.verifier.cfg.CollectorServiceConfiguration
import com.exactpro.th2.verifier.grpc.ChainID
import com.exactpro.th2.verifier.grpc.CheckRuleRequest
import com.exactpro.th2.verifier.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.verifier.grpc.CheckpointRequestOrBuilder
import com.exactpro.th2.verifier.rule.AbstractCheckTask
import com.exactpro.th2.verifier.rule.check.CheckRuleTask
import com.exactpro.th2.verifier.rule.sequence.SequenceCheckRuleTask
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.Objects.requireNonNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import java.util.function.Consumer

class CollectorService(
    private val messageRouter: MessageRouter<MessageBatch>, private val eventBatchRouter: MessageRouter<EventBatch>, configuration: CollectorServiceConfiguration
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

    init {
        val limitSize = configuration.messageBufferLimit
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
        val parentEventID: EventID = requireNonNull(request.parentEventId, "Parent event id can't be null")
        val sessionAlias: String = requireNonNull(request.connectivityId.sessionAlias, "Session alias can't be null")
        val filter: MessageFilter = requireNonNull(request.filter, "Message filter can't be null")
        val direction = directionOrDefault(request.direction)

        val chainID = request.getChainIdOrGenerate()

        val task = CheckRuleTask(request.description, Instant.now(), SessionKey(sessionAlias, direction), request.timeout, filter,
            parentEventID, streamObservable, eventBatchRouter)

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    @Throws(InterruptedException::class)
    fun verifyCheckSequenceRule(request: CheckSequenceRuleRequest): ChainID {
        val parentEventID: EventID = requireNonNull(request.parentEventId, "Parent event id can't be null")
        val sessionAlias: String = requireNonNull(request.connectivityId.sessionAlias, "Session alias can't be null")
        val direction = directionOrDefault(request.direction)

        val chainID = request.getChainIdOrGenerate()

        val task = SequenceCheckRuleTask(request.description, Instant.now(), SessionKey(sessionAlias, direction), request.timeout, request.preFilter,
            request.messageFiltersList, request.checkOrder, parentEventID, streamObservable, eventBatchRouter)

        cleanupTasksOlderThan(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }
        return chainID
    }

    private fun directionOrDefault(direction: Direction) =
        if (direction == Direction.UNRECOGNIZED) Direction.FIRST else direction

    private fun AbstractCheckTask.addToChainOrBegin(
        value: AbstractCheckTask?,
        checkpoint: com.exactpro.th2.infra.grpc.Checkpoint
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

    private fun generateChainID() = ChainID.newBuilder().setId(EventUtils.generateUUID()).build()

    private fun cleanupTasksOlderThan(delta: Long, unit: ChronoUnit = ChronoUnit.SECONDS) {
        val now = Instant.now()
        for ((key, task) in eventIdToLastCheckTask.entries) {
            val endTime = task.endTime
            if (olderThan(now, delta, unit, endTime)) {
                logger.debug("Remove task ${task.description} ($endTime) from tasks map")
                if (eventIdToLastCheckTask.remove(key, task)) {
                    runCatching { task.shutdownExecutor() }
                        .onFailure { logger.error("Cannot shutdown scheduler for task '${task.description}'", it) }
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
            checkpoint.asMap().forEach { (sessionKey: SessionKey, sequence: Long) ->
                val messageID = sessionKey.toMessageID(sequence)
                rootEvent.messageID(messageID)
                    .addSubEventWithSamePeriod()
                    .name("Checkpoint for session alias '${sessionKey.sessionAlias}' direction '${sessionKey.direction}' sequence '$sequence'")
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
        return messageRouter.subscribeAll(listener, "subscribe", "first", "parsed") ?: throw IllegalStateException("Can not subscribe to queues")
    }

    private fun SessionKey.toMessageID(sequence: Long) = MessageID.newBuilder()
        .setConnectionId(ConnectionID.newBuilder()
            .setSessionAlias(sessionAlias)
            .build())
        .setSequence(sequence)
        .setDirection(direction)
        .build()
}