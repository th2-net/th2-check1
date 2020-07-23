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

package com.exactpro.th2.verifier.rule

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.ComparisonUtil
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest
import com.exactpro.th2.infra.grpc.Checkpoint
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.verifier.AbstractSessionObserver
import com.exactpro.th2.verifier.CollectorServiceA
import com.exactpro.th2.verifier.DefaultMessageFactoryProxy
import com.exactpro.th2.verifier.StreamContainer
import com.exactpro.th2.verifier.event.bean.builder.VerificationBuilder
import com.exactpro.th2.verifier.rule.sequence.ComparisonContainer
import com.exactpro.th2.verifier.util.VerificationUtil
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.SingleSubject
import java.time.Instant
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

abstract class AbstractCheckTask(
    val description: String?,
    private val timeout: Long,
    submitTime: Instant,
    protected val sessionAlias: String,
    private val parentEventID: EventID,
    private val messageStream: Observable<StreamContainer>,
    private val eventStoreStub: EventStoreServiceFutureStub
) : AbstractSessionObserver<MessageContainer>() {
    protected var handledMessageCounter: Long = 0

    protected val converter = ProtoToIMessageConverter(DefaultMessageFactoryProxy(), null, null)
    protected val rootEvent: Event = Event.from(submitTime)
        .description(description)

    private val sequenceSubject = SingleSubject.create<Long>()
    private val hasNextTask = AtomicBoolean(false)
    private val taskState = AtomicReference(State.CREATED)
    private var _endTime: Instant? = null
    val endTime: Instant?
        get() = _endTime

    protected enum class State {
        CREATED,
        BEGIN,
        DONE,
        PUBLISHED
    }

    private lateinit var endFuture: Disposable

    private var lastSequence = Long.MIN_VALUE

    override fun onStart() {
        super.onStart()

        //Init or re-init variable in TASK_SCHEDULER thread
        handledMessageCounter = 0
    }

    override fun onError(e: Throwable) {
        super.onError(e)

        rootEvent.status(FAILED)
            .bodyData(EventUtils.createMessageBean(e.message))
    }

    /**
     * Registers a task as the next task in continuous verification chain. Its {@link #begin} method will be called
     * when current task completes check or timeout is over for it.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @throws IllegalStateException when method is called more than once.
     */
    fun subscribeNextTask(checkTask: AbstractCheckTask) {
        if (hasNextTask.compareAndSet(false, true)) {
            sequenceSubject.subscribe { sequence -> checkTask.begin(sequence) }
        } else {
            throw IllegalStateException("Subscription to last sequence for task $description is already executed")
        }
    }

    /**
     * Observe a message sequence from checkpoint.
     * Task subscribe to messages stream with sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param checkpoint message sequence from previous task.
     * @throws IllegalStateException when method is called more than once.
     */
    fun begin(checkpoint: Checkpoint? = null) {
        begin(checkpoint?.getSequence(sessionAlias) ?: DEFAULT_SEQUENCE)
    }

    /**
     * It is called when timeout is over and task in not complete yet
     */
    protected abstract fun onTimeout()

    /**
     * Emit last sequence to the single for to the next task. It should be called after completed of base check.
     * This method can call {@link #onCompleteTask} if the next task already subscribed
     */
    protected fun checkComplete() {
        try {
            LOGGER.info("Check completed for session alias '{}' with sequence '{}'", sessionAlias, lastSequence)
            if (taskState.compareAndSet(State.BEGIN, State.DONE)) {
                dispose()
                endFuture.dispose()
                sequenceSubject.onSuccess(lastSequence)
            } else {
                LOGGER.warn("Task for session alias '{}' is already completed. Skip calling 'checkComplete'", sessionAlias)
            }
        } finally {
            publishEvent()
        }
    }

    /**
     * Provides feature to define custom filter for observe.
     */
    protected open fun Observable<MessageContainer>.taskPipeline() : Observable<MessageContainer> = this

    /**
     * Observe a message sequence from previous task.
     * Task subscribe to messages stream with sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param sequence message sequence from previous task.
     * @throws IllegalStateException when method is called more than once.
     */
    private fun begin(sequence: Long = DEFAULT_SEQUENCE) {
        if (!taskState.compareAndSet(State.CREATED, State.BEGIN)) {
            throw IllegalStateException("Task $description already has been started")
        }
        LOGGER.info("Check begin for session alias '{}' with sequence '{}' timeout '{}'", sessionAlias, sequence, timeout)

        messageStream.observeOn(TASK_SCHEDULER) // Defined scheduler to execution in one thread
            .continueObserve(sessionAlias, sequence)
            .doOnNext {
                handledMessageCounter++

                with(it.metadata.id) {
                    lastSequence = this.sequence
                    rootEvent.messageID(this)
                }
            }
            .mapToMessageContainer()
            .taskPipeline()
            .subscribe(this)

        endFuture = Single.timer(timeout, MILLISECONDS, TASK_SCHEDULER)
            .subscribe { _ -> end() }
    }

    /**
     * Disposes task when timeout is over. Task unsubscribe from message stream.
     */
    private fun end() {
        try {
            LOGGER.info("Timeout is over for session alias '{}' with sequence '{}'", sessionAlias, lastSequence)
            if (taskState.compareAndSet(State.BEGIN, State.DONE)) {
                dispose()
                sequenceSubject.onSuccess(lastSequence)
                onTimeout()
            } else {
                LOGGER.warn("Task for session alias '{}' is already completed. Skip calling 'onTimeout'", sessionAlias)
            }
        } finally {
            publishEvent()
        }
    }

    private fun publishEvent() {
        val currentState = taskState.compareAndExchange(State.DONE, State.PUBLISHED)
        if (currentState != State.PUBLISHED) {
            _endTime = Instant.now()
            LOGGER.debug("Sending event thee id '{}' parent id '{}'", rootEvent.id, parentEventID)
            val storeRequest = StoreEventBatchRequest.newBuilder()
                .setEventBatch(EventBatch.newBuilder()
                    .setParentEventId(parentEventID)
                    .addAllEvents(rootEvent.toProtoEvents(parentEventID.id))
                    .build())
                .build()
            val future = eventStoreStub.storeEventBatch(storeRequest)

            future.addListener(
                Runnable {
                    if (LOGGER.isDebugEnabled) {
                        LOGGER.debug("Sent event batch '{}' with result {}", shortDebugString(storeRequest), shortDebugString(future.get()))
                    }
                },
                RESPONSE_EXECUTOR
            )
            if (currentState != State.DONE) {
                LOGGER.error("Event tree id '{}' parent id '{}' is published in unexpected state '{}'", rootEvent.id, parentEventID, currentState)
            }
        } else {
            LOGGER.warn("Event tree id '{}' parent id '{}' is already published", rootEvent.id, parentEventID)
        }
    }

    companion object {
        /**
         * Used for observe messages in one thread.
         * It provides feature to write no thread-safe code in children classes
         */
        val TASK_SCHEDULER = Schedulers.newThread()
        const val DEFAULT_SEQUENCE = Long.MIN_VALUE

        private val RESPONSE_EXECUTOR = ForkJoinPool.commonPool()
    }

    protected fun MessageFilter.toCompareSettings(): ComparatorSettings =
        ComparatorSettings().also {
            it.metaContainer = VerificationUtil.toMetaContainer(this, false)
        }

    protected fun Event.appendEventWithVerification(protoMessage: Message, protoMessageFilter: MessageFilter, comparisonResult: ComparisonResult): Event {
        val verificationComponent = VerificationBuilder()
        comparisonResult.results.forEach { (key: String?, value: ComparisonResult?) ->
            verificationComponent.verification(key, value, protoMessageFilter, true)
        }

        with(protoMessage.metadata) {
            name("Verification '${messageType}' message")
                .status(if (comparisonResult.getStatusType() == StatusType.FAILED) FAILED else PASSED)
                .messageID(id)
                .bodyData(verificationComponent.build())
        }
        return this
    }

    protected fun Event.appendEventWithVerifications(comparisonContainers: Collection<ComparisonContainer>): Event {
        for (comparisonContainer in comparisonContainers) {
            addSubEventWithSamePeriod().appendEventWithVerification(comparisonContainer.protoActual, comparisonContainer.protoFilter, comparisonContainer.comparisonResult!!)
        }
        return this
    }

    protected fun ComparisonResult.getStatusType(): StatusType = ComparisonUtil.getStatusType(this)

    private fun Observable<Message>.mapToMessageContainer(): Observable<MessageContainer> =
        map { message -> MessageContainer(message, converter.fromProtoMessage(message, false)) }

    /**
     * Filters incoming {@link StreamContainer} via session alias and then
     * filters message which sequence grete than passed
     */
    private fun Observable<StreamContainer>.continueObserve(sessionAlias: String, sequence: Long): Observable<Message> =
        filter { streamContainer -> streamContainer.sessionAlias == sessionAlias }
            .flatMap(StreamContainer::bufferedMessages)
            .filter { message -> message.metadata.id.sequence > sequence }

    private fun Checkpoint.getSequence(sessionAlias: String): Long {
        val sequence = sessionAliasToDirectionCheckpointMap[sessionAlias]
            ?.directionToSequenceMap?.get(Direction.FIRST.number)

        if (sequence == null) {
            if (LOGGER.isWarnEnabled) {
                LOGGER.warn("Checkpoint '{}' doesn't contain sequence for session '{}'", shortDebugString(this), sessionAlias)
            }
        } else {
            LOGGER.info("Use sequence '{}' from checkpoint for session '{}'", sequence, sessionAlias)
        }

        return sequence ?: DEFAULT_SEQUENCE
    }
}