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

package com.exactpro.th2.verifier

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.ComparisonUtil
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest
import com.exactpro.th2.infra.grpc.Checkpoint
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.verifier.event.bean.builder.VerificationBuilder
import com.exactpro.th2.verifier.util.VerificationUtil
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.SingleSubject
import java.time.Instant
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ScheduledFuture
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic.AtomicBoolean

abstract class AbstractCheckTask(private val description: String?,
                                 private val timeout: Long,
                                 submitTime: Instant,
                                 private val sessionAlias: String,
                                 private val parentEventID: EventID,
                                 private val messageStream: Observable<StreamContainer>,
                                 private val scheduler: ScheduledThreadPoolExecutor,
                                 private val eventStoreStub: EventStoreServiceFutureStub) : AbstractSessionObserver<SingleCSHIterator>() {

    private val sequenceSubject = SingleSubject.create<Long>()
    private val hasNextTask = AtomicBoolean(false)
    private val eventPublished = AtomicBoolean(false)
    private lateinit var endFuture: ScheduledFuture<*>
    @Volatile
    private var lastSequence = Long.MIN_VALUE

    protected val converter = ProtoToIMessageConverter(DefaultMessageFactoryProxy(), null, null)
    protected val rootEvent: Event = Event.from(submitTime)
        .description(description)

    /**
     * Registers a task as the next task in continuous verification chain. Its {@link #begin} method will be called
     * when current task completes check or timeout is over for it.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @throws IllegalStateException when method is called more than once.
     */
    fun subscribeNextTask(checkTask: AbstractCheckTask) {
        if (hasNextTask.compareAndSet(false, true)) {
            sequenceSubject.subscribe { sequence -> checkTask.begin(sequence!!) }
        } else {
            throw IllegalStateException("Subscription to last sequence for task $description is already executed")
        }
    }

    /**
     * Observe a message sequence from checkpoint.
     * Task subscribe to messages stream with sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param checkpoint message sequence from previous task.
     * @throws IllegalStateException when method is called more than once. TODO: implements
     */
    fun begin(checkpoint: Checkpoint? = null) {
        begin(checkpoint?.getSequence(sessionAlias) ?: DEFAULT_SEQUENCE)
    }

    /**
     * It is called when timeout is over
     */
    protected abstract fun onTimeout()

    /**
     * Emit last sequence to the single for to the next task. It should be called after completed of base check.
     * This method can call {@link #onCompleteTask} if the next task already subscribed
     */
    protected fun checkComplete() {
        try {
            LOGGER.info("Check completed for session alias '{}' with sequence '{}'", sessionAlias, lastSequence)
            dispose()
            endFuture.cancel(true)
            sequenceSubject.onSuccess(lastSequence)
        } finally {
            publishEvent()
        }
    }

    /**
     * Provides feature to define custom filter for observe.
     */
    protected open fun Observable<Message>.taskFilter() : Observable<Message> = this

    /**
     * Observe a message sequence from previous task.
     * Task subscribe to messages stream with sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param sequence message sequence from previous task.
     * @throws IllegalStateException when method is called more than once. TODO: implements
     */
    private fun begin(sequence: Long = DEFAULT_SEQUENCE) {
        LOGGER.info("Check begin for session alias '{}' with sequence '{}' timeout '{}'", sessionAlias, sequence, timeout)
        lastSequence = sequence

        messageStream.observeOn(TASK_SCHEDULER) // Defined scheduler to execution in one thread
            .mapToMessage(sessionAlias, sequence)
            .doOnNext {
                with(it.metadata.id) {
                    lastSequence = this.sequence
                    rootEvent.messageID(this)
                }
            }
            .taskFilter()
            .mapToCSH()
            .subscribe(this)

        endFuture = scheduler.schedule(this::end, timeout, MILLISECONDS)
    }

    /**
     * Disposes task when timeout is over. Task unsubscribe from message stream.
     */
    private fun end() {
        try {
            LOGGER.info("Timeout is over for session alias '{}' with sequence '{}'", sessionAlias, lastSequence)
            dispose()
            sequenceSubject.onSuccess(lastSequence)
            onTimeout()
        } finally {
            publishEvent()
        }
    }

    private fun publishEvent() {
        if (eventPublished.compareAndSet(false, true)) {
            LOGGER.debug("Sending event thee id '{}' parent id '{}'", rootEvent.id, parentEventID)
            val storeRequest = StoreEventBatchRequest.newBuilder()
                .setEventBatch(EventBatch.newBuilder()
                    .setParentEventId(parentEventID)
                    .addAllEvents(rootEvent.toProtoEvents(parentEventID.id))
                    .build())
                .build()
            val future = eventStoreStub.storeEventBatch(storeRequest)

            future.addListener({
                if (LOGGER.isDebugEnabled) {
                    LOGGER.debug("Sent event batch '{}' with result {}", shortDebugString(storeRequest), shortDebugString(future.get()))
                }
            }, { ForkJoinPool.commonPool() })
        } else {
            LOGGER.warn("Event thee id '{}' parent id '{}' is already published", rootEvent.id, parentEventID)
        }
    }

    companion object {
        /**
         * Used for observe messages in one thread.
         * It provides feature to write no thread-safe code in children classes
         */
        val TASK_SCHEDULER = Schedulers.newThread()
        const val DEFAULT_SEQUENCE = Long.MIN_VALUE
    }

    protected fun MessageFilter.toCompareSettings(): ComparatorSettings =
        ComparatorSettings().also { it.metaContainer = VerificationUtil.toMetaContainer(this, false) }

    protected fun Event.appendEventWithVerification(protoMessage: Message, protoMessageFilter: MessageFilter, comparisonResult: ComparisonResult) {
        val verificationComponent = VerificationBuilder()
        comparisonResult.results.forEach { (key: String?, value: ComparisonResult?) ->
            verificationComponent.verification(key, value, protoMessageFilter, true)
        }

        with(protoMessage.metadata) {
            name("Verification '${messageType}' message")
                .status(if (ComparisonUtil.getStatusType(comparisonResult) == StatusType.FAILED) FAILED else PASSED)
                .messageID(id)
                .bodyData(verificationComponent.build())
        }
    }

    private fun Observable<Message>.mapToCSH(): Observable<SingleCSHIterator> =
        map { message -> SingleCSHIterator(converter, message) }

    private fun Observable<StreamContainer>.mapToMessage(sessionAlias: String, sequence: Long): Observable<Message> =
        filter { streamContainer -> streamContainer.sessionAlias == sessionAlias }
            .flatMap(StreamContainer::bufferedMessages)
            .filter { message -> message.metadata.id.sequence > sequence }

    private fun Checkpoint.getSequence(sessionAlias: String): Long {
        val sequence = sessionAliasToDirectionCheckpointMap[sessionAlias]
            ?.directionToSequenceMap?.get(Direction.FIRST.number)

        return when (sequence) {
            is Long -> {
                CollectorServiceA.LOGGER.info("Use sequence '{}' from checkpoint for session '{}'", sequence, sessionAlias)
                sequence
            }
            else -> {
                if (CollectorServiceA.LOGGER.isWarnEnabled) {
                    CollectorServiceA.LOGGER.warn("Checkpoint '{}' doesn't contain sequence for session '{}'", shortDebugString(this), sessionAlias)
                }
                DEFAULT_SEQUENCE
            }
        }
    }
}