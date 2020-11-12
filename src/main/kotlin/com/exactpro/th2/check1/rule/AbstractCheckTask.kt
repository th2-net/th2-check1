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

package com.exactpro.th2.check1.rule

import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.ComparisonUtil
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.check1.AbstractSessionObserver
import com.exactpro.th2.check1.DefaultMessageFactoryProxy
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.event.bean.builder.VerificationBuilder
import com.exactpro.th2.check1.rule.sequence.ComparisonContainer
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.google.protobuf.TextFormat.shortDebugString
import io.reactivex.Observable
import io.reactivex.Single
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.SingleSubject
import java.time.Instant
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Implements common logic for check task.
 *
 * **Class in not thread-safe**
 */
abstract class AbstractCheckTask(
        val description: String?,
        private val timeout: Long,
        submitTime: Instant,
        protected val sessionKey: SessionKey,
        private val parentEventID: EventID,
        private val messageStream: Observable<StreamContainer>,
        private val eventBatchRouter: MessageRouter<EventBatch>
) : AbstractSessionObserver<MessageContainer>() {
    protected var handledMessageCounter: Long = 0

    protected val converter = ProtoToIMessageConverter(DefaultMessageFactoryProxy(), null, null)
    protected val rootEvent: Event = Event.from(submitTime)
        .description(description)

    private val sequenceSubject = SingleSubject.create<Long>()
    private val hasNextTask = AtomicBoolean(false)
    private val taskState = AtomicReference(State.CREATED)

    /**
     * Used for observe messages in one thread.
     * It provides feature to write no thread-safe code in children classes.
     *
     * Executor is shared between connected tasks.
     */
    private lateinit var executorService: ExecutorService
    private var _endTime: Instant? = null
    val endTime: Instant?
        get() = _endTime

    protected enum class State {
        CREATED,
        BEGIN,
        TIMEOUT,
        COMPLETED,
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
        end("Error ${e.message} received in message stream")
    }

    /**
     * Shutdowns the executor that is used to perform this task.
     *
     * @throws IllegalStateException if this task has connected task
     */
    fun shutdownExecutor() {
        if (hasNextTask.get()) {
            throw IllegalStateException("Cannot shutdown executor for task '$description' that has connected task")
        }
        executorService.shutdown()
    }

    /**
     * Registers a task as the next task in continuous verification chain. Its [begin] method will be called
     * when current task completes check or timeout is over for it.
     * The scheduler for current task will be passed to the next task.
     *
     * This method should be called only once otherwise it throws IllegalStateException.
     * @throws IllegalStateException when method is called more than once.
     */
    fun subscribeNextTask(checkTask: AbstractCheckTask) {
        if (hasNextTask.compareAndSet(false, true)) {
            val executor =
                if (executorService.isShutdown) {
                    LOGGER.warn("Executor has been shutdown before next task has been subscribed. Create a new one")
                    createExecutorService()
                } else {
                    executorService
                }
            sequenceSubject.subscribe { sequence -> checkTask.begin(sequence, executor) }
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
        begin(checkpoint?.getSequence(sessionKey) ?: DEFAULT_SEQUENCE)
    }

    /**
     * It is called when timeout is over and task in not complete yet
     */
    protected open fun onTimeout() {}

    /**
     * Marks the task as successfully completed. If task timeout had been exited and then the task was marked as successfully completed
     * the task will be considered as successfully completed because it had actually found that it should
     */
    protected fun checkComplete() {

        LOGGER.info("Check completed for session alias '{}' with sequence '{}'", sessionKey, lastSequence)
        val prevValue = taskState.getAndSet(State.COMPLETED)
        dispose()
        endFuture.dispose()

        if (prevValue == State.TIMEOUT) {
            LOGGER.info("Task '{}' for session alias '{}' is completed right after timeout exited. Consider it as completed", description, sessionKey)
        } else {
            LOGGER.debug("Task '{}' for session alias '{}' is completed normally", description, sessionKey)
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
     * @param executorService executor to schedule pipeline execution.
     * @throws IllegalStateException when method is called more than once.
     */
    private fun begin(sequence: Long = DEFAULT_SEQUENCE, executorService: ExecutorService = createExecutorService()) {
        if (!taskState.compareAndSet(State.CREATED, State.BEGIN)) {
            throw IllegalStateException("Task $description already has been started")
        }
        LOGGER.info("Check begin for session alias '{}' with sequence '{}' timeout '{}'", sessionKey, sequence, timeout)
        this.executorService = executorService
        val scheduler = Schedulers.from(executorService)

        messageStream.observeOn(scheduler) // Defined scheduler to execution in one thread to avoid race-condition.
            .doFinally(this::taskFinished) // will be executed if the source is complete or an error received or the timeout is exited.

            // All sources above will be disposed on this scheduler.
            //
            // This method should be called as closer as possible
            // to the actual dispose you want to execute on this scheduler
            // because other operations are executed on the same single-thread scheduler.
            //
            // If we move [Observable#unsubscribeOn] after them they won't be disposed until scheduler is free.
            // In the worst-case scenario, it might never happen.
            .unsubscribeOn(scheduler)
            .continueObserve(sessionKey, sequence)
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

        endFuture = Single.timer(timeout, MILLISECONDS, Schedulers.computation())
            .subscribe { _ -> end("Timeout is exited") }
    }

    private fun taskFinished() {
        try {
            val currentState = taskState.get()
            LOGGER.info("Finishes task '$description' in state $currentState")
            if (currentState == State.TIMEOUT) {
                callOnTimeoutCallback()
            }
            publishEvent()
            LOGGER.info("Task '$description' has been finished")
        } catch (ex: Exception) {
            LOGGER.error("Cannot finish task '$description'", ex)
        } finally {
            sequenceSubject.onSuccess(lastSequence)
        }
    }

    private fun callOnTimeoutCallback() {
        try {
            onTimeout()
        } catch (ex: Exception) {
            LOGGER.error("Cannot execute 'onTimeout' method", ex)
        }
    }

    private fun createExecutorService(): ExecutorService = Executors.newSingleThreadExecutor()

    /**
     * Disposes task when timeout is over or message stream is completed normally or with an exception.
     * Task unsubscribe from message stream.
     *
     * @param reason the cause why task must be stopped
     */
    private fun end(reason: String) {
        if (taskState.compareAndSet(State.BEGIN, State.TIMEOUT)) {
            LOGGER.info("Stop task for session alias '{}' with sequence '{}' because: {}", sessionKey, lastSequence, reason)
            dispose()
            endFuture.dispose()
        } else {
            LOGGER.debug("Task for session alias '{}' is already completed. Ignore 'end' method call with reason: {}", sessionKey, reason)
        }
    }

    override fun onComplete() {
        super.onComplete()
        end("Message stream is completed")
    }

    /**
     * Prepare the root event or children events for publication.
     * This method is invoked in [State.PUBLISHED] state.
     */
    protected open fun completeEvent(canceled: Boolean) {}

    /**
     * Publishes event to the [eventStoreStub].
     */
    private fun publishEvent() {
        val prevState = taskState.getAndSet(State.PUBLISHED)
        if (prevState != State.PUBLISHED) {
            completeEvent(prevState == State.TIMEOUT)
            _endTime = Instant.now()
            LOGGER.debug("Sending event tree id '{}' parent id '{}'", rootEvent.id, parentEventID)
           val batch = EventBatch.newBuilder()
               .setParentEventId(parentEventID)
               .addAllEvents(rootEvent.toProtoEvents(parentEventID.id))
               .build()

            RESPONSE_EXECUTOR.execute {
                try {
                    eventBatchRouter.send(batch, "publish", "event")
                    if (LOGGER.isDebugEnabled) {
                        LOGGER.debug("Sent event batch '{}'", shortDebugString(batch))
                    }
                } catch (e: Exception) {
                    LOGGER.error("Can not send event batch '{}'", shortDebugString(batch), e)
                }
            }
        } else {
            LOGGER.debug("Event tree id '{}' parent id '{}' is already published", rootEvent.id, parentEventID)
        }
    }

    companion object {
        const val DEFAULT_SEQUENCE = Long.MIN_VALUE

        private val RESPONSE_EXECUTOR = ForkJoinPool.commonPool()
    }

    protected fun MessageFilter.toCompareSettings(): ComparatorSettings =
        ComparatorSettings().also {
            it.metaContainer = VerificationUtil.toMetaContainer(this, false)
            it.ignoredFields = this.comparisonSettings.ignoreFieldsList?.toSet() ?: emptySet()
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
    private fun Observable<StreamContainer>.continueObserve(sessionKey: SessionKey, sequence: Long): Observable<Message> =
        filter { streamContainer -> streamContainer.sessionKey == sessionKey }
            .flatMap(StreamContainer::bufferedMessages)
            .filter { message -> message.metadata.id.sequence > sequence }

    private fun Checkpoint.getSequence(sessionKey: SessionKey): Long {
        val sequence = sessionAliasToDirectionCheckpointMap[sessionKey.sessionAlias]
            ?.directionToSequenceMap?.get(sessionKey.direction.number)

        if (sequence == null) {
            if (LOGGER.isWarnEnabled) {
                LOGGER.warn("Checkpoint '{}' doesn't contain sequence for session '{}'", shortDebugString(this), sessionKey)
            }
        } else {
            LOGGER.info("Use sequence '{}' from checkpoint for session '{}'", sequence, sessionKey)
        }

        return sequence ?: DEFAULT_SEQUENCE
    }
}