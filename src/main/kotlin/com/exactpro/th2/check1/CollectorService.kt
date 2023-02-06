/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1

import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.entities.Checkpoint
import com.exactpro.th2.check1.entities.CheckpointData
import com.exactpro.th2.check1.grpc.ChainID
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.check1.grpc.CheckpointRequestOrBuilder
import com.exactpro.th2.check1.metrics.BufferMetric
import com.exactpro.th2.check1.grpc.NoMessageCheckRequest
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.RuleFactory
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RequestStatus
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
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ForkJoinPool
import com.exactpro.th2.common.grpc.Checkpoint as GrpcCheckpoint
import com.exactpro.th2.common.message.toJson
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong

class CollectorService(
    private val messageRouter: MessageRouter<MessageBatch>,
    private val eventBatchRouter: MessageRouter<EventBatch>,
    private val configuration: Check1Configuration
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
    private val ruleIdToResult: MutableMap<Long, CompletableFuture<Pair<EventStatus, Instant>>> = ConcurrentHashMap()
    private val ruleIdCounter = AtomicLong()

    private val olderThanDelta = configuration.cleanupOlderThan
    private val olderThanTimeUnit = configuration.cleanupTimeUnit
    private val defaultAutoSilenceCheck: Boolean = configuration.isAutoSilenceCheckAfterSequenceRule

    private var ruleFactory: RuleFactory

    init {
        BufferMetric.configure(configuration)

        val limitSize = configuration.messageCacheSize
        mqSubject = PublishSubject.create()

        subscriberMonitor = subscribe(MessageListener { _: String, batch: MessageBatch -> mqSubject.onNext(batch) })
        streamObservable = mqSubject.flatMapIterable(MessageBatch::getMessagesList)
                .groupBy { message ->
                    message.metadata.id.run {
                        SessionKey(connectionId.sessionAlias, direction)
                    }.also(BufferMetric::processMessage)
                }
            .map { group -> StreamContainer(group.key!!, limitSize, group) }
            .replay().apply { connect() }

        checkpointSubscriber = streamObservable.subscribeWith(CheckpointSubscriber())

        ruleFactory = RuleFactory(configuration, streamObservable, eventBatchRouter)
    }

    private fun prepareStoringResults(storeResult: Boolean): Triple<Long, CompletableFuture<Pair<EventStatus, Instant>>?, ((EventStatus) -> Unit)> {
        return if (storeResult) {
            val future = CompletableFuture<Pair<EventStatus, Instant>>()
            val ruleId = ruleIdCounter.incrementAndGet()
            Triple(ruleId, future) { future.complete(it to Instant.now()) }
        } else {
            Triple(0L, null, AbstractCheckTask.EMPTY_STATUS_CONSUMER)
        }
    }

    @Throws(InterruptedException::class)
    fun verifyCheckRule(request: CheckRuleRequest): Pair<Long, ChainID> {
        val chainID = request.getChainIdOrGenerate()
        val (ruleId, resultFuture, onTaskFinished) = prepareStoringResults(request.storeResult)

        cleanupTasksAndResults(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, prevTask ->
            ruleFactory.createCheckRule(request, prevTask != null, onTaskFinished)
                .apply { addToChainOrBegin(prevTask, request.checkpoint) }
        }

        if (resultFuture != null) {
            ruleIdToResult[ruleId] = resultFuture
        }

        return ruleId to chainID
    }

    @Throws(InterruptedException::class)
    fun verifyCheckSequenceRule(request: CheckSequenceRuleRequest): Pair<Long, ChainID> {
        val chainID = request.getChainIdOrGenerate()
        val (ruleId, resultFuture, onTaskFinished) = prepareStoringResults(request.storeResult)

        cleanupTasksAndResults(olderThanDelta, olderThanTimeUnit)
        val silenceCheck = if (request.hasSilenceCheck()) request.silenceCheck.value else defaultAutoSilenceCheck

        val (silenceCheckTask, silenceFuture) = if (silenceCheck) {
            val (_, resultFutureSilence, onTaskFinishedSilence) = prepareStoringResults(request.storeResult)
            ruleFactory.createSilenceCheck(
                request,
                olderThanTimeUnit.duration.toMillis() * olderThanDelta,
                onTaskFinishedSilence
            ) to resultFutureSilence
        } else {
            null to null
        }

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, prevTask ->
            ruleFactory.createSequenceCheckRule(request, prevTask != null, onTaskFinished)
                .apply { addToChainOrBegin(prevTask, request.checkpoint) }
                .run { silenceCheckTask?.also { subscribeNextTask(it) } ?: this }
        }

        if (resultFuture != null) {
            ruleIdToResult[ruleId] = if (silenceFuture == null)
                resultFuture
            else
                resultFuture.thenApply { if (it.first == EventStatus.SUCCESS) silenceFuture.get() else it }
        }

        return ruleId to chainID
    }

    fun verifyNoMessageCheck(request: NoMessageCheckRequest): Pair<Long, ChainID> {
        val chainID = request.getChainIdOrGenerate()
        val (ruleId, resultFuture, onTaskFinished) = prepareStoringResults(request.storeResult)

        cleanupTasksAndResults(olderThanDelta, olderThanTimeUnit)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, prevTask ->
            ruleFactory.createNoMessageCheckRule(request, prevTask != null, onTaskFinished)
                .apply { addToChainOrBegin(prevTask, request.checkpoint) }
        }

        if (resultFuture != null) {
            ruleIdToResult[ruleId] = resultFuture
        }

        return ruleId to chainID
    }

    enum class RuleResult(
        val requestStatus: RequestStatus.Status = RequestStatus.Status.ERROR,
        val message: String? = null,
        val status: EventStatus? = null
    ) {
        PASSED(RequestStatus.Status.SUCCESS, status = EventStatus.SUCCESS),
        FAILED(RequestStatus.Status.SUCCESS, status = EventStatus.FAILED),
        TIMEOUT(message = "Timeout expired"),
        NOT_FOUND(message = "No rule with specified id found"),
        ERROR(message = "Failed to retrieve rule result due to internal error. See logs for more details.");
    }

    fun getRuleResult(id: Long, timeoutNano: Long): RuleResult {
        val statusFuture = ruleIdToResult[id]

        if (statusFuture == null) {
            logger.debug("No rule with specified id found")
            return RuleResult.NOT_FOUND
        }

        return try {
            val (status, _) = statusFuture.get(timeoutNano, TimeUnit.NANOSECONDS)
            when (status) {
                EventStatus.SUCCESS -> RuleResult.PASSED
                EventStatus.FAILED -> RuleResult.FAILED
                else -> {
                    logger.error("Invalid rule status: $status")
                    RuleResult.ERROR
                }
            }
        } catch (e: TimeoutException) {
            logger.debug("Timeout expired")
            RuleResult.TIMEOUT
        } catch (e: Exception) {
            logger.error("Failed to retrieve rule result: ", e)
            RuleResult.ERROR
        }
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

    private fun cleanupTasksAndResults(delta: Long, unit: ChronoUnit = ChronoUnit.SECONDS) {
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

        ruleIdToResult.values.removeIf { future ->
            val (_, endTime) = future.getNow(null) ?: return@removeIf false
            return@removeIf olderThan(now, delta, unit, endTime)
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
        val event: Event = Event.start()
        val checkpoint = checkpointSubscriber.createCheckpoint()
        publishCheckpoint(request, checkpoint, event)
        return checkpoint
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

    private fun publishCheckpoint(request: CheckpointRequestOrBuilder, checkpoint: Checkpoint, event: Event) {
        if (!request.hasParentEventId()) {
            if (logger.isWarnEnabled) {
                logger.warn("Parent id missed in request {}", request.toJson())
            }
            return
        }
        val rootEvent: Event = event
            .name("Checkpoint")
            .type("Checkpoint")
            .description(request.description)
            .endTimestamp()
            .bodyData(EventUtils.createMessageBean("Checkpoint id '${checkpoint.id}'"))
        if (!configuration.enableCheckpointEventsPublication) {
            rootEvent.bodyData(EventUtils.createMessageBean("Checkpoints publication is disabled. Check the component configuration to enable it"))
        }
        checkpoint.sessionKeyToCheckpointData.forEach { (sessionKey: SessionKey, checkpointData: CheckpointData) ->
            val messageID = sessionKey.toMessageID(checkpointData.sequence)
            rootEvent.messageID(messageID)
            if (configuration.enableCheckpointEventsPublication) {
                rootEvent.addSubEventWithSamePeriod()
                    .name("Checkpoint for session alias '${sessionKey.sessionAlias}' direction '${sessionKey.direction}' sequence '${checkpointData.sequence}'")
                    .type("Checkpoint for session")
                    .messageID(messageID)
            }
        }
        try {
            sendEvents(request.parentEventId, rootEvent)
        } catch (e: Exception) {
            logger.error(
                "Sending events '{}' with a parent '{}' failed ",
                rootEvent, request.parentEventId, e
            )
        }
    }
}