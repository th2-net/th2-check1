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
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.verifier.CollectorServiceA.Companion
import com.exactpro.th2.verifier.event.bean.builder.VerificationBuilder
import com.exactpro.th2.verifier.util.VerificationUtil
import com.google.protobuf.TextFormat
import io.reactivex.Observable
import io.reactivex.Scheduler
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.SingleSubject
import java.lang.IllegalStateException
import java.time.Instant
import java.util.concurrent.locks.ReadWriteLock
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

abstract class AbstractCheckTask(private val description: String,
                                    private val startTime: Instant,
                                    private val sessionAlias: String,
                                    private val messageStream: Observable<StreamContainer>) : AbstractSessionObserver<SingleCSHIterator>() {


    private val sequenceSubject = SingleSubject.create<Long>()

    private val nextTaskLock = ReentrantReadWriteLock()
    private var _hasNextTask: Boolean = false
    protected val hasNextTask: Boolean
        get() = nextTaskLock.read { sequenceSubject.hasObservers() }

    protected val converter = ProtoToIMessageConverter(DefaultMessageFactoryProxy(), null, null)
    protected val rootEvent = Event.from(startTime)
        .description(description)

    /**
     * This method should be called only once otherwise it throws IllegalStateException.
     * @throws IllegalStateException when method is called more than once.
     */
    fun subscribeToLastSequence(checkTask: AbstractCheckTask) {
        nextTaskLock.write {
            if (_hasNextTask) {
                throw IllegalStateException("Subscription to last sequence for task $description is already executed")
            }
            sequenceSubject.subscribe(checkTask::observeSequence)
            _hasNextTask = true
        }
    }

    /**
     * Disposes task when timeout is over. Task unsubscribe from message stream.
     */
    fun disposeOnTimeout() {
        dispose()
    }

    /**
     * Observe a message sequence from checkpoint or previous task.
     * Task subscribe to messages stream with sequence after call.
     * This method should be called only once otherwise it throws IllegalStateException.
     * @param sequence message sequence from checkpoint or previous task.
     * @throws IllegalStateException when method is called more than once. TODO: implements
     */
    private fun observeSequence(sequence: Long) {
        messageStream.observeOn(TASK_SCHEDULER)
            .mapToMessage(sessionAlias, sequence)
            .mapToCSH()
            .subscribe(this)
    }

    protected fun MessageFilter.toCompareSettings() : ComparatorSettings =
        ComparatorSettings().also { it.metaContainer = VerificationUtil.toMetaContainer(this, false) }

    protected fun Observable<Message>.mapToCSH() : Observable<SingleCSHIterator> =
        map{ message -> SingleCSHIterator(converter, message) }

    protected fun Event.appendEventWithVerification(protoMessage: Message, protoMessageFilter: MessageFilter, comparisonResult: ComparisonResult) {
        val verificationComponent = VerificationBuilder()
        comparisonResult.results.forEach {
            (key: String?, value: ComparisonResult?) ->
                verificationComponent.verification(key, value, protoMessageFilter, true) }

        with(protoMessage.metadata) {
            name("Verification '${messageType}' message")
                .status(if (ComparisonUtil.getStatusType(comparisonResult) == StatusType.FAILED) FAILED else PASSED)
                .messageID(id)
                .bodyData(verificationComponent.build())
        }
    }

    private fun Observable<StreamContainer>.mapToMessage(sessionAlias : String, sequence: Long) : Observable<Message> =
        filter{ streamContainer -> streamContainer.sessionAlias == sessionAlias }
            .flatMap(StreamContainer::bufferedMessages)
            .filter{ message -> message.metadata.id.sequence > sequence }

    private fun com.exactpro.th2.infra.grpc.Checkpoint.getSequence(sessionAlias: String) : Long {
        val sequence = sessionAliasToDirectionCheckpointMap[sessionAlias]
            ?.directionToSequenceMap?.get(Direction.FIRST.number)

        return when (sequence) {
            is Long -> {
                CollectorServiceA.LOGGER.info("Use sequence '{}' from checkpoint for session '{}'", sequence, sessionAlias)
                sequence
            }
            else -> {
                if (CollectorServiceA.LOGGER.isWarnEnabled) {
                    CollectorServiceA.LOGGER.warn("Checkpoint '{}' doesn't contain sequence for session '{}'", TextFormat.shortDebugString(this), sessionAlias)
                }
                Long.MIN_VALUE
            }
        }
    }

    companion object {
        val TASK_SCHEDULER = Schedulers.newThread()
    }
}