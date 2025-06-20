/*
 * Copyright 2020-2025 Exactpro (Exactpro Systems Limited)
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
import com.exactpro.th2.check1.grpc.NoMessageCheckRequest
import com.exactpro.th2.check1.metrics.BufferMetric
import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.rule.OnTaskFinished
import com.exactpro.th2.check1.rule.RuleFactory
import com.exactpro.th2.check1.utils.ExecutorPool
import com.exactpro.th2.check1.utils.generateChainID
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.ConnectionID
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.MessageBatch
import com.exactpro.th2.common.grpc.MessageID
import com.exactpro.th2.common.grpc.RequestStatus
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.DeliveryMetadata
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.common.schema.message.SubscriberMonitor
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.GroupBatch
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.MessageGroup
import com.exactpro.th2.common.schema.message.impl.rabbitmq.transport.ParsedMessage
import com.exactpro.th2.common.utils.event.logId
import com.exactpro.th2.common.utils.message.MessageHolder
import com.exactpro.th2.common.utils.message.ProtoMessageHolder
import com.exactpro.th2.common.utils.message.TransportMessageHolder
import com.fasterxml.jackson.core.JsonProcessingException
import io.reactivex.rxjava3.core.Observable
import io.reactivex.rxjava3.subjects.PublishSubject
import mu.KotlinLogging
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.max
import com.exactpro.th2.common.grpc.Checkpoint as GrpcCheckpoint

class CollectorService(
    protoMessageRouter: MessageRouter<MessageBatch>,
    transportMessageRouter: MessageRouter<GroupBatch>,
    private val eventBatchRouter: MessageRouter<EventBatch>,
    private val configuration: Check1Configuration,
    private val waitEvent: WaitEvent,
) {

    private val kLogger = KotlinLogging.logger(javaClass.name + '@' + hashCode())

    /**
     * Queue name to subscriber. Messages with different connectivity can be transferred with one queue.
     */
    private val protoSubscriberMonitor: SubscriberMonitor?
    private val transportSubscriberMonitor: SubscriberMonitor?
    private val streamObservable: Observable<StreamContainer>
    private val checkpointSubscriber: CheckpointSubscriber
    private val mqSubject: PublishSubject<MessageHolder>
    private val eventIdToLastCheckTask: MutableMap<CheckTaskKey, AbstractCheckTask> = ConcurrentHashMap()
    private val ruleIdToResult: MutableMap<Long, CompletableFuture<RuleResultMetadata>> = ConcurrentHashMap()
    private val ruleIdCounter = AtomicLong()

    private val olderThanDelta = configuration.cleanupOlderThan
    private val olderThanTimeUnit = configuration.cleanupTimeUnit
    private val defaultAutoSilenceCheck: Boolean = configuration.isAutoSilenceCheckAfterSequenceRule
    private val ruleExecutorPool = ExecutorPool("rule-executor", configuration.rulesExecutionThreads)
    private val ruleFactory: RuleFactory
    private var nextCleanupTime: Instant = Instant.now()

    init {
        BufferMetric.configure(configuration)

        val limitSize = configuration.messageCacheSize
        mqSubject = PublishSubject.create()
        protoSubscriberMonitor = runCatching {
            checkNotNull(protoMessageRouter.subscribeAll({ _: DeliveryMetadata, batch: MessageBatch ->
                batch.messagesList.forEach {
                    mqSubject.onNext(ProtoMessageHolder(it))
                }
            }))
        }.onFailure {
            kLogger.warn(it) { "Can not subscribe for listening protobuf messages" }
        }.getOrNull()
        transportSubscriberMonitor = runCatching {
            checkNotNull(transportMessageRouter.subscribeAll({ _: DeliveryMetadata, batch: GroupBatch ->
                val book = batch.book
                val sessionGroup = batch.sessionGroup
                batch.groups.asSequence()
                    .flatMap(MessageGroup::messages)
                    .forEach { message ->
                        when (message) {
                            is ParsedMessage -> mqSubject.onNext(TransportMessageHolder(message, book, sessionGroup))
                            else -> error("Transport group contains not parsed message $message")
                        }
                    }
            }))
        }.onFailure {
            kLogger.warn(it) { "Can not subscribe for listening transport messages" }
        }.getOrNull()

        if (protoSubscriberMonitor == null && transportSubscriberMonitor == null) {
            error("Subscribe pin should be declared at least one of protobuf or transport protocols")
        }

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

    private data class StoringResult(
        val id: Long,
        val resultFuture: CompletableFuture<RuleResultMetadata>?,
        val onTaskFinished: OnTaskFinished,
    )

    private fun prepareStoringResults(storeResult: Boolean): StoringResult {
        return if (storeResult) {
            val future = CompletableFuture<RuleResultMetadata>()
            val ruleId = ruleIdCounter.incrementAndGet()
            StoringResult(ruleId, future) { status, eventId -> future.complete(RuleResultMetadata(status,Instant.now(), eventId)) }
        } else {
            EMPTY_STORING_RESULT
        }
    }

    @JvmOverloads
    fun verifyCheckRule(request: CheckRuleRequest, eventId: EventID? = null, defaultChainID: ChainID? = null): RuleAndChainIds {
        cleanupTasksAndResults(olderThanDelta, olderThanTimeUnit)

        val chainID = if (request.hasChainId()) request.chainId else defaultChainID ?: generateChainID()
        val (ruleId, resultFuture, onTaskFinished) = prepareStoringResults(request.storeResult)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            val task = ruleFactory.createCheckRule(request, value != null, eventId, onTaskFinished)
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }

        if (resultFuture != null) {
            ruleIdToResult[ruleId] = resultFuture
        }

        return RuleAndChainIds(ruleId, chainID)
    }

    @JvmOverloads
    fun verifyCheckSequenceRule(request: CheckSequenceRuleRequest, eventId: EventID? = null,  defaultChainID: ChainID? = null): RuleAndChainIds {
        cleanupTasksAndResults(olderThanDelta, olderThanTimeUnit)

        val chainID = if (request.hasChainId()) request.chainId else defaultChainID ?: generateChainID()
        val (ruleId, resultFuture, onTaskFinished) = prepareStoringResults(request.storeResult)
        val silenceCheck = if (request.hasSilenceCheck()) request.silenceCheck.value else defaultAutoSilenceCheck

        val (silenceCheckTask, silenceFuture) = if (silenceCheck) {
            val (_, resultFutureSilence, _) = prepareStoringResults(request.storeResult)
            ruleFactory.createSilenceCheck(
                request,
                olderThanTimeUnit.duration.toMillis() * olderThanDelta,
                eventId,
                onTaskFinished
            ) to resultFutureSilence
        } else {
            null to null
        }

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, prevTask ->
            ruleFactory.createSequenceCheckRule(request, prevTask != null, eventId, onTaskFinished)
                .apply { addToChainOrBegin(prevTask, request.checkpoint) }
                .run { silenceCheckTask?.also { subscribeNextTask(it) } ?: this }
        }

        if (resultFuture != null) {
            ruleIdToResult[ruleId] = if (silenceFuture == null)
                resultFuture
            else
                resultFuture.thenCompose { if (it.status == EventStatus.SUCCESS) silenceFuture else resultFuture }
        }
        return RuleAndChainIds(ruleId, chainID)
    }

    @JvmOverloads
    fun verifyNoMessageCheck(request: NoMessageCheckRequest,  eventId: EventID? = null, defaultChainID: ChainID? = null): RuleAndChainIds {
        cleanupTasksAndResults(olderThanDelta, olderThanTimeUnit)

        val chainID = if (request.hasChainId()) request.chainId else defaultChainID ?: generateChainID()
        val (ruleId, resultFuture, onTaskFinished) = prepareStoringResults(request.storeResult)

        eventIdToLastCheckTask.compute(CheckTaskKey(chainID, request.connectivityId)) { _, value ->
            val task = ruleFactory.createNoMessageCheckRule(request, value != null, eventId, onTaskFinished)
            task.apply { addToChainOrBegin(value, request.checkpoint) }
        }

        if (resultFuture != null) {
            ruleIdToResult[ruleId] = resultFuture
        }

        return RuleAndChainIds(ruleId, chainID)
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
            kLogger.debug { "No rule with specified id found" }
            return RuleResult.NOT_FOUND
        }

        return try {
            val awaitStart = System.nanoTime()
            val result: RuleResultMetadata = statusFuture.get(timeoutNano, TimeUnit.NANOSECONDS)
            when (result.status) {
                EventStatus.SUCCESS -> RuleResult.PASSED
                EventStatus.FAILED -> {
                    if (result.eventId == null) {
                        kLogger.error { "Await event storing can't be executed for null event id" }
                        RuleResult.ERROR
                    } else {
                        if (waitEvent.wait(result.eventId, Duration.ofNanos(max(0, timeoutNano - (System.nanoTime() - awaitStart))))) {
                            kLogger.debug { "Waiting '${result.eventId.logId}' event succeed" }
                            RuleResult.FAILED
                        } else {
                            kLogger.error { "Timeout expired for waiting event: ${result.eventId.logId}" }
                            RuleResult.TIMEOUT
                        }
                    }
                }
                else -> {
                    kLogger.error { "Invalid rule status: ${result.status}" }
                    RuleResult.ERROR
                }
            }
        } catch (e: TimeoutException) {
            kLogger.debug(e) { "Timeout expired" }
            RuleResult.TIMEOUT
        } catch (e: Exception) {
            kLogger.error(e) { "Failed to retrieve rule result" }
            RuleResult.ERROR
        }
    }

    private fun AbstractCheckTask.addToChainOrBegin(value: AbstractCheckTask?, checkpoint: GrpcCheckpoint) {
        val realCheckpoint = if (checkpoint === GrpcCheckpoint.getDefaultInstance()) {
            null
        } else {
            checkpoint
        }
        value?.subscribeNextTask(this) ?: begin(realCheckpoint, ruleExecutorPool.get())
    }

    private fun cleanupTasksAndResults(delta: Long, unit: ChronoUnit = ChronoUnit.SECONDS) {
        val now = Instant.now()
        if (now.isBefore(nextCleanupTime)) return

        eventIdToLastCheckTask.values.removeIf { task ->
            val endTime = task.endTime
            when {
                !olderThan(now, delta, unit, endTime) -> false
                else -> {
                    !task.hasNextRule().also { canBeRemoved ->
                        when {
                            canBeRemoved -> kLogger.info { "Removed task ${task.description} ($endTime) from tasks map" }
                                else -> kLogger.debug { "Task ${task.description} can't be removed because it has a continuation" }
                        }
                    }
                }
            }
        }

        ruleIdToResult.values.removeIf { future ->
            val (_, endTime) = future.getNow(null) ?: return@removeIf false
            return@removeIf olderThan(now, delta, unit, endTime)
        }

        nextCleanupTime = now.plusMillis(configuration.minCleanupIntervalMs)
    }

    private fun olderThan(now: Instant?, delta: Long, unit: ChronoUnit, endTime: Instant?) =
        endTime != null && unit.between(endTime, now) > delta

    @Throws(JsonProcessingException::class)
    private fun sendEvents(parentEventID: EventID, event: Event) {
        kLogger.debug { "Sending event thee id '${event.id}' parent id '${parentEventID.toJson()}'" }

        val batch = event.toBatchProto(parentEventID)

        ForkJoinPool.commonPool().execute {
            try {
                eventBatchRouter.send(batch, "publish", "event")
                kLogger.debug { "Sent event batch '${batch.toJson()}'" }
            } catch (e: Exception) {
                kLogger.error(e) { "Can not send event batch '${batch.toJson()}'" }
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
        protoSubscriberMonitor?.let {
            runCatching(protoSubscriberMonitor::unsubscribe)
                .onFailure { kLogger.error(it) { "Close protobuf subscriber failure" } }
        }
        transportSubscriberMonitor?.let {
            runCatching(transportSubscriberMonitor::unsubscribe)
                .onFailure { kLogger.error(it) { "Close transport subscriber failure" } }
            mqSubject.onComplete()
        }
        mqSubject.onComplete()
        ruleExecutorPool.dispose()
    }

    private fun publishCheckpoint(request: CheckpointRequestOrBuilder, checkpoint: Checkpoint, event: Event) {
        if (!request.hasParentEventId()) {
            kLogger.warn { "Parent id missed in request ${request.toJson()}" }
            return
        }
        val rootEvent: Event = event
            .name("Checkpoint")
            .type("Checkpoint")
            .description(request.description)
            .endTimestamp()
            .bodyData(EventUtils.createMessageBean("Checkpoint id '${checkpoint.id}'"))
        try {
            if (!configuration.enableCheckpointEventsPublication) {
                rootEvent.bodyData(EventUtils.createMessageBean("Checkpoints publication is disabled. Check the component configuration to enable it"))
            }
            checkpoint.sessionKeyToCheckpointData.forEach { (sessionKey: SessionKey, checkpointData: CheckpointData) ->
                val messageID = sessionKey.toMessageID(checkpointData)
                rootEvent.messageID(messageID)
                if (configuration.enableCheckpointEventsPublication) {
                    rootEvent.addSubEventWithSamePeriod()
                        .name("Checkpoint for book name '${sessionKey.bookName}', session alias '${sessionKey.sessionAlias}', direction '${sessionKey.direction}' sequence '$checkpointData.sequence'")
                        .type("Checkpoint for session")
                        .messageID(messageID)
                }
            }
        } finally {
            try {
                if (request.hasParentEventId()) {
                    sendEvents(request.parentEventId, rootEvent)
                } else {
                    kLogger.warn {"Parent id missed in request" }
                }
            } catch (e: Exception) {
                kLogger.error(e) {
                    "Sending events '$rootEvent' with a parent '${request.parentEventId.toJson()}' failed "
                }
            }
        }
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

    companion object {
        private val EMPTY_STORING_RESULT = StoringResult(0L, null, AbstractCheckTask.EMPTY_STATUS_CONSUMER)
    }

    data class RuleAndChainIds(val ruleId: Long, val chainID: ChainID)
}

fun interface WaitEvent {
    fun wait(id: EventID, duration: Duration): Boolean
}