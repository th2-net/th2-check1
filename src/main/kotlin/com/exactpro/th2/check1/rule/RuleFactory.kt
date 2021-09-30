/*
 * Copyright 2021-2021 Exactpro (Exactpro Systems Limited)
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

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.entities.TaskTimeout
import com.exactpro.th2.check1.exception.RuleCreationException
import com.exactpro.th2.check1.exception.RuleInternalException
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.check1.grpc.NoMessageCheckRequest
import com.exactpro.th2.check1.grpc.PreFilter
import com.exactpro.th2.check1.rule.check.CheckRuleTask
import com.exactpro.th2.check1.rule.nomessage.NoMessageCheckTask
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.check1.rule.sequence.SilenceCheckTask
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.grpc.Checkpoint
import com.exactpro.th2.common.grpc.ComparisonSettings
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.MessageFilter
import com.exactpro.th2.common.grpc.RootComparisonSettings
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.schema.message.MessageRouter
import com.google.protobuf.GeneratedMessageV3
import io.reactivex.Observable
import mu.KotlinLogging
import org.slf4j.Logger
import java.time.Instant
import java.util.concurrent.ForkJoinPool

class RuleFactory(
    configuration: Check1Configuration,
    private val streamObservable: Observable<StreamContainer>,
    private val eventBatchRouter: MessageRouter<EventBatch>
) {
    private val maxEventBatchContentSize = configuration.maxEventBatchContentSize
    private val defaultRuleExecutionTimeout = configuration.ruleExecutionTimeout

    fun createCheckRule(request: CheckRuleRequest): CheckRuleTask =
            ruleCreation(request.parentEventId) {
                checkAndCreateRule {
                    check(request.hasParentEventId()) { "Parent event id can't be null" }
                    check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
                    val sessionAlias: String = request.connectivityId.sessionAlias
                    val sessionKey = SessionKey(sessionAlias, directionOrDefault(request.direction))
                    checkMessageTimeout(request.messageTimeout) { checkCheckpoint(request.checkpoint, sessionKey) }

                    check(request.kindCase != CheckRuleRequest.KindCase.KIND_NOT_SET) {
                        "Either old filter or root filter must be set"
                    }
                    val filter: RootMessageFilter = if (request.hasRootFilter()) {
                        request.rootFilter
                    } else {
                        request.filter.toRootMessageFilter()
                    }.also { it.validateRootMessageFilter() }
                    val direction = directionOrDefault(request.direction)

                    CheckRuleTask(
                            request.description,
                            Instant.now(),
                            sessionKey,
                            createTaskTimeout(request.timeout, request.messageTimeout),
                            maxEventBatchContentSize,
                            filter,
                            request.parentEventId,
                            streamObservable,
                            eventBatchRouter
                    )
                }
                onErrorEvent {
                    Event.start()
                            .name("Check rule cannot be created")
                            .type("checkRuleCreation")
                }
            }

    fun createSequenceCheckRule(request: CheckSequenceRuleRequest): SequenceCheckRuleTask =
            ruleCreation(request.parentEventId) {
                checkAndCreateRule {
                    check(request.hasParentEventId()) { "Parent event id can't be null" }
                    check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
                    val sessionAlias: String = request.connectivityId.sessionAlias
                    val sessionKey = SessionKey(sessionAlias, directionOrDefault(request.direction))
                    checkMessageTimeout(request.messageTimeout) { checkCheckpoint(request.checkpoint, sessionKey) }

                    check((request.messageFiltersList.isEmpty() && request.rootMessageFiltersList.isNotEmpty())
                            || (request.messageFiltersList.isNotEmpty() && request.rootMessageFiltersList.isEmpty())) {
                        "Either messageFilters or rootMessageFilters must be set but not both"
                    }

                    val protoMessageFilters: List<RootMessageFilter> = request.rootMessageFiltersList.ifEmpty {
                        request.messageFiltersList.map { it.toRootMessageFilter() }
                    }.onEach { it.validateRootMessageFilter() }

                    SequenceCheckRuleTask(
                            request.description,
                            Instant.now(),
                            sessionKey,
                            createTaskTimeout(request.timeout, request.messageTimeout),
                            maxEventBatchContentSize,
                            request.preFilter,
                            protoMessageFilters,
                            request.checkOrder,
                            request.parentEventId,
                            streamObservable,
                            eventBatchRouter
                    )
                }
                onErrorEvent {
                    Event.start()
                            .name("Sequence check rule cannot be created")
                            .type("sequenceCheckRuleCreation")
                }
            }

    fun createNoMessageCheckRule(request: NoMessageCheckRequest): NoMessageCheckTask =
            ruleCreation(request.parentEventId) {
                checkAndCreateRule {
                    check(request.hasParentEventId()) { "Parent event id can't be null" }
                    val parentEventID: EventID = request.parentEventId
                    check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
                    val sessionAlias: String = request.connectivityId.sessionAlias
                    val sessionKey = SessionKey(sessionAlias, directionOrDefault(request.direction))
                    checkMessageTimeout(request.messageTimeout) { checkCheckpoint(request.checkpoint, sessionKey) }

                    NoMessageCheckTask(
                            request.description,
                            Instant.now(),
                            sessionKey,
                            createTaskTimeout(request.timeout, request.messageTimeout),
                            maxEventBatchContentSize,
                            request.preFilter,
                            parentEventID,
                            streamObservable,
                            eventBatchRouter
                    )
                }
                onErrorEvent {
                    Event.start()
                            .name("Check rule cannot be created")
                            .type("checkRuleCreation")
                }
            }

    fun createSilenceCheck(
        request: CheckSequenceRuleRequest,
        timeout: Long
    ): SilenceCheckTask {
        return ruleCreation(request.parentEventId) {
            checkAndCreateRule {
                check(timeout > 0) { "timeout must be greater that zero" }
                val sessionAlias: String = request.connectivityId.sessionAlias
                val sessionKey = SessionKey(sessionAlias, directionOrDefault(request.direction))
                SilenceCheckTask(
                    request.preFilter,
                    request.description.takeIf(String::isNotEmpty),
                    createTaskTimeout(timeout),
                    maxEventBatchContentSize,
                    Instant.now(),
                    sessionKey,
                    request.parentEventId,
                    streamObservable,
                    eventBatchRouter
                )
            }
            onErrorEvent {
                Event.start()
                    .name("Auto silence check rule cannot be created")
                    .type("checkRuleCreation")
            }
        }
    }

    private inline fun <R : AbstractCheckTask> ruleCreation(parentEventId: EventID, block: RuleCreationContext<R>.() -> Unit): R {
        val ruleCreationContext = RuleCreationContext<R>().apply(block)
        try {
            return ruleCreationContext.action()
        } catch (e: RuleInternalException) {
            throw e
        } catch (e: Exception) {
            val rootEvent = ruleCreationContext.event()
            rootEvent.addSubEventWithSamePeriod()
                    .name("An error occurred while creating rule")
                    .type("ruleCreationException")
                    .exception(e, true)
                    .status(Event.Status.FAILED)
            publishEvents(rootEvent, parentEventId)
            throw RuleCreationException("An error occurred while creating rule", e)
        }
    }

    private fun MessageFilter.toRootMessageFilter(): RootMessageFilter {
        return RootMessageFilter.newBuilder()
                .setMessageType(this.messageType)
                .setComparisonSettings(this.comparisonSettings.toRootComparisonSettings())
                .setMessageFilter(this)
                .build()
    }

    private fun RootMessageFilter.validateRootMessageFilter() {
        check(this.messageType.isNotBlank()) { "Rule cannot be executed because the message filter does not contain 'message type'" }
    }

    private fun ComparisonSettings.toRootComparisonSettings(): RootComparisonSettings {
        return RootComparisonSettings.newBuilder()
                .addAllIgnoreFields(this.ignoreFieldsList)
                .build()
    }

    private fun directionOrDefault(direction: Direction) =
            if (direction == Direction.UNRECOGNIZED) Direction.FIRST else direction

    private fun publishEvents(event: Event, parentEventId: EventID) {
        if (parentEventId == EventID.getDefaultInstance()) {
            return
        }

        val batch = EventBatch.newBuilder()
                .addAllEvents(event.toListProto(parentEventId))
                .build()
        RESPONSE_EXECUTOR.execute {
            try {
                eventBatchRouter.send(batch)
                if (LOGGER.isDebugEnabled) {
                    LOGGER.debug("Sent event batch '{}'", batch.toJson())
                }
            } catch (e: Exception) {
                LOGGER.error("Can not send event batch '{}'", batch.toJson(), e)
            }
        }
    }

    private fun checkCheckpoint(checkpoint: Checkpoint, sessionKey: SessionKey) {
        check(checkpoint !== Checkpoint.getDefaultInstance()) { "Request doesn't contain a checkpoint" }
        with(sessionKey) {
            val directionCheckpoint = checkpoint.sessionAliasToDirectionCheckpointMap[sessionAlias]
            checkNotNull(directionCheckpoint) { "The checkpoint doesn't contain a direction checkpoint with session alias '$sessionAlias'" }
            val checkpointData = directionCheckpoint.directionToCheckpointDataMap[direction.number]
            checkNotNull(checkpointData) { "The direction checkpoint doesn't contain a checkpoint data with direction '$direction'" }
            with(checkpointData) {
                check(sequence > 0L) { "The checkpoint data has incorrect sequence number '$sequence'" }
                check(this.hasTimestamp()) { "The checkpoint data doesn't contain timestamp" }
            }
        }
    }

    private fun checkMessageTimeout(messageTimeout: Long, checkpointCheckAction: () -> Unit) {
        when {
            messageTimeout > 0 -> checkpointCheckAction()
            messageTimeout < 0 -> error("Message timeout cannot be negative")
        }
    }

    private fun createTaskTimeout(timeout: Long, messageTimeout: Long = 0): TaskTimeout {
        val newRuleTimeout = if (timeout <= 0) {
            LOGGER.info("Rule execution timeout is less than or equal to zero, used default rule execution timeout '$defaultRuleExecutionTimeout'")
            defaultRuleExecutionTimeout
        } else {
            timeout
        }
        return TaskTimeout(newRuleTimeout, messageTimeout)
    }

    private class RuleCreationContext<R : AbstractCheckTask> {
        lateinit var action: () -> R
        lateinit var event: () -> Event

        fun checkAndCreateRule(block: () -> R) {
            action = block
        }

        fun onErrorEvent(block: () -> Event) {
            event = block
        }
    }

    companion object {
        private val LOGGER: Logger = KotlinLogging.logger { }
        private val RESPONSE_EXECUTOR = ForkJoinPool.commonPool()
    }
}