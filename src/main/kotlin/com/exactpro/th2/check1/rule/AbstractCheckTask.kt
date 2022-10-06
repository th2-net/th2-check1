/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.th2.check1.AbstractSessionObserver
import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.entities.CheckpointData
import com.exactpro.th2.check1.entities.RuleConfiguration
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.event.bean.builder.VerificationBuilder
import com.exactpro.th2.check1.exception.RuleInternalException
import com.exactpro.th2.check1.metrics.RuleMetric
import com.exactpro.th2.check1.util.VerificationUtil
import com.exactpro.th2.check1.utils.convert
import com.exactpro.th2.check1.utils.getStatusType
import com.exactpro.th2.check1.utils.isAfter
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toJavaDuration
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.message.toReadableBodyCollection
import com.exactpro.th2.common.schema.message.MessageRouter
import com.exactpro.th2.sailfish.utils.FilterSettings
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter
import com.exactpro.th2.sailfish.utils.ProtoToIMessageConverter.createParameters
import com.google.protobuf.TextFormat.shortDebugString
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
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
 * @param maxEventBatchContentSize max size in bytes of summary events content in a batch
 * **Class in not thread-safe**
 */
abstract class AbstractCheckTask(
    private val ruleConfiguration: RuleConfiguration,
    private val submitTime: Instant,
    protected val sessionKey: SessionKey,
    private val parentEventID: EventID,
    private val messageStream: Observable<StreamContainer>,
    private val eventBatchRouter: MessageRouter<EventBatch>
) : AbstractSessionObserver<MessageContainer>() {

    protected open class Refs(val rootEvent: Event)

    protected class RefsKeeper<T : Refs>(refs: T) {
        private var refsNullable: T? = refs
        val refs: T get() = refsNullable ?: error("Requesting references after references has been erased.")
        fun eraseRefs() {
            refsNullable = null
        }
    }

    protected abstract val refsKeeper: RefsKeeper<out Refs>
    private val refs get() = refsKeeper.refs

    val description: String? = ruleConfiguration.description
    private val taskTimeout: TaskTimeout = ruleConfiguration.taskTimeout

    protected var handledMessageCounter: Long = 0

    private val sequenceSubject = SingleSubject.create<Legacy>()
    private val hasNextTask = AtomicBoolean(false)
    private val taskState = AtomicReference(State.CREATED)
    @Volatile
    private var streamCompletedState = State.STREAM_COMPLETED
    @Volatile
    private var completed = false
    protected var isParentCompleted: Boolean? = null
        private set

    /**
     * Used for observe messages in one thread.
     * It provides the feature to write no thread-safe code in children classes.
     *
     * Executor is shared between connected tasks.
     */
    private lateinit var executorService: ExecutorService
    private var _endTime: Instant? = null
    val endTime: Instant?
        get() = _endTime

    protected enum class State(val callOnTimeoutCallback: Boolean) {
        CREATED(false),
        BEGIN(false),
        TIMEOUT(true),
        MESSAGE_TIMEOUT(true),
        TASK_COMPLETED(false),
        STREAM_COMPLETED(true),
        PUBLISHED(false),
        ERROR(false)
    }

    @Volatile
    private lateinit var endFuture: Disposable

    private var lastSequence = DEFAULT_SEQUENCE
    private var checkpointTimeout: Timestamp? = null
    private var lastMessageTimestamp: Timestamp? = null
    private var untrusted: Boolean = false
    private var hasMessagesInTimeoutInterval: Boolean = false
    private var bufferContainsStartMessage: Boolean = false
    private var isDefaultSequence: Boolean = false

    @Volatile
    protected var started = false

    protected fun createRootEvent() = Event.from(submitTime).description(description)

    final override fun onStart() {
        super.onStart()
        started = true

        //Init or re-init variable in TASK_SCHEDULER thread
        handledMessageCounter = 0

        onStartInit()
    }

    protected abstract fun onStartInit()

    override fun onError(e: Throwable) {
        super.onError(e)

        refs.rootEvent.status(FAILED)
            .bodyData(EventUtils.createMessageBean(e.message))
        end(State.ERROR, "Error ${e.message} received in message stream")
    }

    /**
     * Shutdown the executor that is used to perform this task in case it doesn't have a next task
     * @return true if the task doesn't have a next task, otherwise it will return false
     */
    fun tryShutdownExecutor(): Boolean {
        if (hasNextTask.get()) {
            LOGGER.warn("Cannot shutdown executor for task '$description' that has a connected task")
            return false
        }
        executorService.shutdown()
        return true
    }

    /**
     * Registers a task as the next task in the continuous verification chain. Its [begin] method will be called
     * when the current task completes its check or if the timeout is over for it.
     * The scheduler for the current task will be passed to the next task.
     *
     * This method should be called only once otherwise it throws IllegalStateException.
     * @throws IllegalStateException when method is called more than once.
     */
    fun subscribeNextTask(checkTask: AbstractCheckTask) {
        if (hasNextTask.compareAndSet(false, true)) {
            onChainedTaskSubscription()
            sequenceSubject.subscribe { legacy ->
                val executor = if (legacy.executorService.isShutdown) {
                    LOGGER.warn("Executor has been shutdown before next task has been subscribed. Create a new one")
                    createExecutorService()
                } else {
                    legacy.executorService
                }
                legacy.sequenceData.apply {
                    checkTask.begin(lastSequence, lastMessageTimestamp, executor, PreviousExecutionData(untrusted, completed))
                }
            }
            LOGGER.info("Task {} ({}) subscribed to task {} ({})", checkTask.description, checkTask.hashCode(), description, hashCode())
        } else {
            error("Subscription to last sequence for task $description (${hashCode()}) is already executed, subscriber ${checkTask.description} (${checkTask.hashCode()})")
        }
    }

    /**
     * Observe a message sequence from the checkpoint.
     * Task subscribe to messages stream with its sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param checkpoint message sequence and checkpoint timestamp from previous task.
     * @throws IllegalStateException when method is called more than once.
     */
    fun begin(checkpoint: Checkpoint? = null) {
        val checkpointData = checkpoint?.getCheckpointData(sessionKey)
        begin(checkpointData?.sequence ?: DEFAULT_SEQUENCE, checkpointData?.timestamp)
    }

    /**
     * Callback when another task is subscribed to the result of the current task
     */
    protected open fun onChainedTaskSubscription() {}

    /**
     * It is called when the timeout is over and the task is not complete yet
     */
    protected open fun onTimeout() {}

    /**
     * Marks the task as successfully completed. If the task timeout, message timeout or stream had been exited and then
     * the task was marked as successfully completed the task will be considered as successfully completed because
     * it had actually found that it should
     */
    protected fun checkComplete() {

        LOGGER.info("Check completed for session alias '{}' with sequence '{}'", sessionKey, lastSequence)
        val prevValue = taskState.getAndSet(State.TASK_COMPLETED)
        dispose()
        endFuture.dispose()
        completed = true

        when (prevValue) {
            State.TIMEOUT -> {
                LOGGER.info("Task '{}' for session alias '{}' is completed right after timeout exited. Consider it as completed", description, sessionKey)
            }
            State.MESSAGE_TIMEOUT -> {
                LOGGER.info("Task '{}' for session alias '{}' is completed right after message timeout exited. Consider it as completed", description, sessionKey)
            }
            State.STREAM_COMPLETED -> {
                LOGGER.info("Task '{}' for session alias '{}' is completed right after the end of streaming messages. Consider it as completed", description, sessionKey)
            }
            else -> {
                LOGGER.debug("Task '{}' for session alias '{}' is completed normally", description, sessionKey)
            }
        }
    }

    /**
     * Provides a feature to define custom filter for observe.
     */
    protected open fun Observable<MessageContainer>.taskPipeline() : Observable<MessageContainer> = this

    /**
     * @return `true` if another task has been subscribed to the result of the current task.
     * Otherwise, returns `false`
     */
    protected fun hasNextTask(): Boolean = hasNextTask.get()

    protected abstract fun name(): String
    protected abstract fun type(): String
    protected abstract fun setup(rootEvent: Event)

    /**
     * Observe a message sequence from the previous task.
     * Task subscribe to messages stream with sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param sequence message sequence from the previous task.
     * @param checkpointTimestamp checkpoint timestamp from the previous task
     * @param executorService executor to schedule pipeline execution.
     * @param untrusted flag is guarantee that the previous sequence data is correct
     * @param parentTaskCompleted indicates whether the parent task was completed normally. `null` if no parent task exists.
     * @throws IllegalStateException when method is called more than once.
     */
    private fun begin(
        sequence: Long = DEFAULT_SEQUENCE,
        checkpointTimestamp: Timestamp? = null,
        executorService: ExecutorService = createExecutorService(),
        previousExecutionData: PreviousExecutionData = PreviousExecutionData.DEFAULT
    ) {
        configureRootEvent()
        isParentCompleted = previousExecutionData.completed
        if (!taskState.compareAndSet(State.CREATED, State.BEGIN)) {
            error("Task $description already has been started")
        }
        LOGGER.info("Check begin for session alias '{}' with sequence '{}' and task timeout '{}'", sessionKey, sequence, taskTimeout)
        RuleMetric.incrementActiveRule(type())
        this.lastSequence = sequence
        this.executorService = executorService
        this.untrusted = previousExecutionData.untrusted
        this.checkpointTimeout = calculateCheckpointTimeout(checkpointTimestamp, taskTimeout.messageTimeout)
        this.isDefaultSequence = sequence == DEFAULT_SEQUENCE
        val scheduler = Schedulers.from(executorService)

        endFuture = Single.timer(taskTimeout.timeout, MILLISECONDS, Schedulers.computation())
            .subscribe { _ -> end(State.TIMEOUT, "Timeout is exited") }

        try {
            messageStream.observeOn(scheduler) // Defined scheduler to execution in one thread to avoid race-condition.
                    .doFinally(this::taskFinished) // will be executed if the source is complete or an error received or the timeout is exited.

                    // All sources above will be disposed on this scheduler.
                    //
                    // This method should be called as close as possible
                    // to the actual dispose that you want to execute on this scheduler
                    // because other operations are executed on the same single-thread scheduler.
                    //
                    // If we move [Observable#unsubscribeOn] after them, they won't be disposed until the scheduler is free.
                    // In the worst-case scenario, it might never happen.
                    .unsubscribeOn(scheduler)
                    .continueObserve(sessionKey, sequence)
                    .doOnNext {
                        handledMessageCounter++

                        with(it.metadata.id) {
                            refs.rootEvent.messageID(this)
                        }
                    }
                    .takeWhileMessagesInTimeout()
                    .mapToMessageContainer()
                    .taskPipeline()
                    .subscribe(this)
        } catch (exception: Exception) {
            LOGGER.error("An internal error occurred while executing rule", exception)
            refs.rootEvent.addSubEventWithSamePeriod()
                    .name("An error occurred while executing rule")
                    .type("internalError")
                    .status(FAILED)
                    .exception(exception, true)
            taskFinished()
            throw RuleInternalException("An internal error occurred while executing rule", exception)
        }
    }

    private fun taskFinished() {
        try {
            val currentState = taskState.get()
            LOGGER.info("Finishes task '$description' in state ${currentState.name}")
            if (currentState.callOnTimeoutCallback) {
                callOnTimeoutCallback()
            }
            publishEvent()
            LOGGER.info("Task '$description' has been finished")
        } catch (ex: Exception) {
            val message = "Cannot finish task '$description'"
            LOGGER.error(message, ex)
            eventBatchRouter.send(EventBatch.newBuilder()
                .setParentEventId(parentEventID)
                .addEvents(Event.start()
                    .name("Check rule $description problem")
                    .type("Exception")
                    .status(FAILED)
                    .bodyData(EventUtils.createMessageBean(message))
                    .bodyData(EventUtils.createMessageBean(ex.message))
                    .toProto(parentEventID))
                .build())
        } finally {
            RuleMetric.decrementActiveRule(type())
            refsKeeper.eraseRefs()
            sequenceSubject.onSuccess(Legacy(executorService, SequenceData(lastSequence, lastMessageTimestamp, !hasMessagesInTimeoutInterval)))
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
     * Disposes the task when the timeout is over or the message stream is completed normally or with an exception.
     * Task unsubscribe from the message stream.
     *
     * @param state of the stopped task
     * @param reason the cause why a task must be stopped
     */
    private fun end(state: State, reason: String) {
        if (taskState.compareAndSet(State.BEGIN, state)) {
            LOGGER.info("Stop task for session alias '{}' with sequence '{}' because: {}", sessionKey, lastSequence, reason)
            dispose()
            endFuture.dispose()
        } else {
            LOGGER.debug("Task for session alias '{}' is already completed. Ignore 'end' method call with reason: {}", sessionKey, reason)
        }
    }

    override fun onComplete() {
        super.onComplete()
        end(streamCompletedState, "Message stream is completed")
    }

    /**
     * Prepare the root event or children events for publication.
     * This method is invoked in [State.PUBLISHED] state.
     */
    protected open fun completeEvent(taskState: State) {}

    protected open val skipPublication: Boolean = false

    protected fun isCheckpointLastReceivedMessage(): Boolean = bufferContainsStartMessage && !hasMessagesInTimeoutInterval

    /**
     * Publishes the event to [eventBatchRouter].
     */
    private fun publishEvent() {
        val prevState = taskState.getAndSet(State.PUBLISHED)
        if (prevState != State.PUBLISHED) {
            val hasError = completeEventOrReportError(prevState)
            _endTime = Instant.now()

            if (skipPublication && !hasError) {
                LOGGER.info("Skip event publication for task ${type()} '$description' (${hashCode()})")
                return
            }
            val batches = refs.rootEvent.disperseToBatches(ruleConfiguration.maxEventBatchContentSize, parentEventID)

            RESPONSE_EXECUTOR.execute {
                batches.forEach { batch ->
                    LOGGER.debug("Sending event batch parent id '{}'", parentEventID.id)
                    try {
                        eventBatchRouter.send(batch)
                        if (LOGGER.isDebugEnabled) {
                            LOGGER.debug("Sent event batch '{}'", shortDebugString(batch))
                        }
                    } catch (e: Exception) {
                        LOGGER.error("Can not send event batch '{}'", shortDebugString(batch), e)
                    }
                }
            }
        } else {
            LOGGER.debug("Event tree id '{}' parent id '{}' is already published", refs.rootEvent.id, parentEventID)
        }
    }

    private fun completeEventOrReportError(prevState: State): Boolean {
        return try {
            if (started) {
                completeEvent(prevState)
                doAfterCompleteEvent()
                false
            } else {
                LOGGER.error("Check task was not started.")
                refs.rootEvent.addSubEventWithSamePeriod()
                    .name("Check failed: task timeout elapsed before the check task was started. Please, check component resources for throttling or intensive GC")
                    .type("taskNotStarted")
                    .status(FAILED)
                true
            }
        } catch (e: Exception) {
            LOGGER.error("Result event cannot be completed", e)
            refs.rootEvent.addSubEventWithSamePeriod()
                    .name("Check result event cannot build completely")
                    .type("eventNotComplete")
                    .bodyData(EventUtils.createMessageBean("An unexpected exception has been thrown during result check build"))
                    .bodyData(EventUtils.createMessageBean(e.message))
                    .status(FAILED)
            true
        }
    }

    private fun configureRootEvent() {
        refs.rootEvent.name(name()).type(type())
        setup(refs.rootEvent)
    }

    private fun doAfterCompleteEvent() {
        if (untrusted) {
            fillUntrustedExecutionEvent()
        } else if (!isDefaultSequence && !bufferContainsStartMessage) {
            if (hasMessagesInTimeoutInterval) {
                fillEmptyStartMessageEvent()
            } else {
                fillMissedStartMessageAndMessagesInIntervalEvent()
            }
        }
    }

    private fun fillUntrustedExecutionEvent() {
        refs.rootEvent.addSubEvent(
            Event.start()
                .name("The current check is untrusted because the start point of the check interval has been selected approximately")
                .status(FAILED)
                .type("untrustedExecution")
        )
    }

    private fun fillMissedStartMessageAndMessagesInIntervalEvent() {
        refs.rootEvent.addSubEvent(
            Event.start()
                .name("Check cannot be executed because buffer for session alias '${sessionKey.sessionAlias}' and direction '${sessionKey.direction}' contains neither message in the requested check interval with sequence '$lastSequence' and checkpoint timestamp '${checkpointTimeout?.toJson()}'")
                .status(FAILED)
                .type("missedMessagesInInterval")
        )
    }

    private fun fillEmptyStartMessageEvent() {
        refs.rootEvent.addSubEvent(
            Event.start()
                .name("Buffer for session alias '${sessionKey.sessionAlias}' and direction '${sessionKey.direction}' doesn't contain starting message, but contains several messages in the requested check interval")
                .status(FAILED)
                .type("missedStartMessage")
        )
    }

    internal fun matchFilter(
        messageContainer: MessageContainer,
        messageFilter: SailfishFilter,
        metadataFilter: SailfishFilter?,
        matchNames: Boolean = true,
        significant: Boolean = true
    ): AggregatedFilterResult {
        val metadataComparisonResult: ComparisonResult? = metadataFilter?.let {
            MessageComparator.compare(
                messageContainer.metadataMessage,
                it.message, it.comparatorSettings,
                matchNames
            )?.also { comparisonResult ->
                LOGGER.debug("Metadata comparison result\n {}", comparisonResult)
            }
        }
        if (metadataFilter != null && metadataComparisonResult == null) {
            if (LOGGER.isDebugEnabled) {
                LOGGER.debug("Metadata for message {} does not match the filter by key fields. Skip message checking",
                        messageContainer.protoMessage.metadata.id.toJson())
            }
            return AggregatedFilterResult.EMPTY
        }
        val comparisonResult: ComparisonResult? = messageFilter.let {
            MessageComparator.compare(messageContainer.sailfishMessage, it.message, it.comparatorSettings, matchNames)
        }

        if (comparisonResult == null) {
            LOGGER.debug("Comparison result for the message '{}' with the message `{}` does not match the filter by key fields or message type",
                    messageContainer.sailfishMessage.name, messageFilter.message.name)
        } else {
            LOGGER.debug("Compare message '{}' result\n{}", messageContainer.sailfishMessage.name, comparisonResult)
        }

        return if (comparisonResult != null || metadataComparisonResult != null) {
            if (significant) {
                messageContainer.protoMessage.metadata.apply {
                    lastSequence = id.sequence
                    lastMessageTimestamp = timestamp
                }
            }
            AggregatedFilterResult(comparisonResult, metadataComparisonResult)
        } else {
            AggregatedFilterResult.EMPTY
        }
    }

    companion object {
        const val DEFAULT_SEQUENCE = Long.MIN_VALUE
        private val RESPONSE_EXECUTOR = ForkJoinPool.commonPool()
        @JvmField
        val CONVERTER = ProtoToIMessageConverter(VerificationUtil.FACTORY_PROXY, null, null, createParameters().setUseMarkerForNullsInMessage(true))
    }

    protected fun RootMessageFilter.metadataFilterOrNull(): MetadataFilter? =
        if (hasMetadataFilter()) metadataFilter else null

    protected fun RootMessageFilter.toCompareSettings(): ComparatorSettings =
        ComparatorSettings().also {
            it.metaContainer = VerificationUtil.toMetaContainer(this.messageFilter, false)
            it.ignoredFields = this.comparisonSettings.ignoreFieldsList.toSet()

            it.isCheckSimpleCollectionsOrder = if (comparisonSettings.hasCheckSimpleCollectionsOrder()) {
                comparisonSettings.checkSimpleCollectionsOrder.value
            } else {
                ruleConfiguration.defaultCheckSimpleCollectionsOrder
            }

            if (this.comparisonSettings.checkRepeatingGroupOrder) {
                it.isCheckGroupsOrder = true
            } else {
                it.isKeepResultGroupOrder = true
            }
        }

    protected fun MessageFilter.toCompareSettings(): ComparatorSettings =
        ComparatorSettings().also {
            it.metaContainer = VerificationUtil.toMetaContainer(this, false)
        }

    protected fun MetadataFilter.toComparisonSettings(): ComparatorSettings =
        ComparatorSettings().also {
            it.metaContainer = VerificationUtil.toMetaContainer(this)
        }

    protected fun Event.appendEventWithVerification(protoMessage: Message, protoFilter: RootMessageFilter, comparisonResult: ComparisonResult): Event {
        val verificationComponent = VerificationBuilder()
        comparisonResult.results.forEach { (key: String?, value: ComparisonResult?) ->
            verificationComponent.verification(key, value, protoFilter.messageFilter, true)
        }

        with(protoMessage.metadata) {
            name("Verification '${messageType}' message")
                .type("Verification")
                .status(if (comparisonResult.getStatusType() == StatusType.FAILED) FAILED else PASSED)
                .messageID(id)
                .bodyData(verificationComponent.build())
            if (protoFilter.hasDescription()) {
                description(protoFilter.description.value)
            }
        }
        return this
    }

    protected fun Event.appendEventWithVerification(metadata: MessageMetadata, metadataFilter: MetadataFilter, comparisonResult: ComparisonResult): Event {
        val verificationComponent = VerificationBuilder()
        comparisonResult.results.forEach { (key: String?, value: ComparisonResult?) ->
            verificationComponent.verification(key, value, metadataFilter)
        }

        with(metadata) {
            name("Verification '${messageType}' metadata")
                    .type("Verification")
                    .status(if (comparisonResult.getStatusType() == StatusType.FAILED) FAILED else PASSED)
                    .messageID(id)
                    .bodyData(verificationComponent.build())
        }
        return this
    }

    protected fun Event.appendEventWithVerificationsAndFilters(protoMessageFilters: Collection<RootMessageFilter>, comparisonContainers: Collection<ComparisonContainer>): Event {
        for (messageFilter in protoMessageFilters) {
            val comparisonContainer = comparisonContainers.firstOrNull { it.protoFilter === messageFilter }
            comparisonContainer?.let {
                appendEventsWithVerification(it)
            } ?: appendEventsWithFilter(messageFilter)
        }
        return this
    }

    protected fun Event.appendEventsWithFilter(rootMessageFilter: RootMessageFilter): Event = this.apply {
        addSubEventWithSamePeriod()
            .name("Message filter")
            .type("Filter")
            .status(FAILED)
            .bodyData(rootMessageFilter.toReadableBodyCollection())
    }

    protected fun Event.appendEventsWithVerification(comparisonContainer: ComparisonContainer): Event = this.apply {
        val protoFilter = comparisonContainer.protoFilter
        addSubEventWithSamePeriod()
            .appendEventWithVerification(comparisonContainer.protoActual, protoFilter, comparisonContainer.result.messageResult!!)
        if (protoFilter.hasMetadataFilter()) {
            addSubEventWithSamePeriod()
                .appendEventWithVerification(comparisonContainer.protoActual.metadata, protoFilter.metadataFilter, comparisonContainer.result.metadataResult!!)
        }
    }

    protected fun ProtoToIMessageConverter.fromProtoPreFilter(protoPreMessageFilter: RootMessageFilter,
                                                              messageName: String = protoPreMessageFilter.messageType): IMessage {
        val filterSettings = protoPreMessageFilter.comparisonSettings.run {
            FilterSettings().apply {
                decimalPrecision = if (this@run.decimalPrecision.isBlank()) {
                    ruleConfiguration.decimalPrecision
                } else {
                    this@run.decimalPrecision.toDouble()
                }
                timePrecision = if (this@run.hasTimePrecision()) {
                    this@run.timePrecision.toJavaDuration()
                } else {
                    ruleConfiguration.timePrecision
                }
                isCheckNullValueAsEmpty = ruleConfiguration.isCheckNullValueAsEmpty
            }
        }

        return fromProtoFilter(protoPreMessageFilter.messageFilter, filterSettings, messageName)
    }

    private fun Observable<Message>.mapToMessageContainer(): Observable<MessageContainer> =
        map { message -> MessageContainer(message, CONVERTER.fromProtoMessage(message, false)) }

    /**
     * Filters incoming {@link StreamContainer} via session alias and then
     * filters the message which its sequence is greater than passed
     */
    private fun Observable<StreamContainer>.continueObserve(sessionKey: SessionKey, sequence: Long): Observable<Message> =
        filter { streamContainer -> streamContainer.sessionKey == sessionKey }
            .flatMap(StreamContainer::bufferedMessages)
            .filter { message ->
                if (message.metadata.id.sequence == sequence) {
                    bufferContainsStartMessage = true
                }
                message.metadata.id.sequence > sequence
            }

    private fun Checkpoint.getCheckpointData(sessionKey: SessionKey): CheckpointData {
        val checkpointData = sessionAliasToDirectionCheckpointMap[sessionKey.sessionAlias]
            ?.directionToCheckpointDataMap?.get(sessionKey.direction.number)

        if (checkpointData == null) {
            if (LOGGER.isWarnEnabled) {
                LOGGER.warn("Checkpoint '{}' doesn't contain checkpoint data for session '{}'", this.toJson(), sessionKey)
            }
            val sequence = sessionAliasToDirectionCheckpointMap[sessionKey.sessionAlias]
                ?.directionToSequenceMap?.get(sessionKey.direction.number)
            if (sequence == null) {
                if (LOGGER.isWarnEnabled) {
                    LOGGER.warn("Checkpoint '{}' doesn't contain sequence for session '{}'", this.toJson(), sessionKey)
                }
                return CheckpointData(DEFAULT_SEQUENCE)
            }
            return CheckpointData(sequence)
        }

        return checkpointData.convert().apply {
            LOGGER.info("Use sequence '{}' from checkpoint for session '{}'", sequence, sessionKey)
        }
    }

    private fun checkOnMessageTimeout(timestamp: Timestamp): Boolean {
        return checkpointTimeout == null || checkpointTimeout!!.isAfter(timestamp) || checkpointTimeout == timestamp
    }

    /**
     * Provides the ability to stop observing if a message timeout is set.
     */
    private fun Observable<Message>.takeWhileMessagesInTimeout() : Observable<Message> =
        takeWhile {
            checkOnMessageTimeout(it.metadata.timestamp).also { continueObservation ->
                hasMessagesInTimeoutInterval = hasMessagesInTimeoutInterval or continueObservation
                if (!continueObservation) {
                    streamCompletedState = State.MESSAGE_TIMEOUT
                }
            }
        }

    private fun calculateCheckpointTimeout(timestamp: Timestamp?, messageTimeout: Long): Timestamp? =
        if (timestamp != null && messageTimeout > 0) {
            Timestamps.add(timestamp, Durations.fromMillis(messageTimeout))
        } else {
            null
        }

    private data class Legacy(val executorService: ExecutorService, val sequenceData: SequenceData)
    private data class SequenceData(val lastSequence: Long, val lastMessageTimestamp: Timestamp?, val untrusted: Boolean)
    private data class PreviousExecutionData(
        /**
         * `true` if the previous rule in the chain marked as untrusted
         */
        val untrusted: Boolean = false,
        /**
         * `true` if previous rule has been completed normally. Otherwise, `false`
         *
         * `null` if there is no previous rule in chain
         */
        val completed: Boolean? = null
    ) {
        companion object {
            val DEFAULT = PreviousExecutionData()
        }
    }
}