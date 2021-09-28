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
import com.exactpro.th2.check1.exception.RuleCreationException
import com.exactpro.th2.check1.exception.RuleInternalException
import com.exactpro.th2.check1.grpc.CheckRuleRequest
import com.exactpro.th2.check1.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.check1.rule.check.CheckRuleTask
import com.exactpro.th2.check1.rule.sequence.SequenceCheckRuleTask
import com.exactpro.th2.common.event.Event
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
    private val maxEventBatchContentSize: Int,
    private val streamObservable: Observable<StreamContainer>,
    private val eventBatchRouter: MessageRouter<EventBatch>
) {

    fun createCheckRule(request: CheckRuleRequest): CheckRuleTask =
            ruleCreation(request, request.parentEventId) {
                checkAndCreateRule { request ->
                    check(request.hasParentEventId()) { "Parent event id can't be null" }
                    check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
                    val sessionAlias: String = request.connectivityId.sessionAlias

                    check(request.kindCase != CheckRuleRequest.KindCase.KIND_NOT_SET) {
                        "Either old filter or root filter must be set"
                    }
                    val filter: RootMessageFilter = if (request.hasRootFilter()) {
                        request.rootFilter
                    } else {
                        request.filter.toRootMessageFilter()
                    }
                    val direction = directionOrDefault(request.direction)

                    CheckRuleTask(
                            request.description,
                            Instant.now(),
                            SessionKey(sessionAlias, direction),
                            request.timeout,
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
            ruleCreation(request, request.parentEventId) {
                checkAndCreateRule { request ->
                    check(request.hasParentEventId()) { "Parent event id can't be null" }
                    check(request.connectivityId.sessionAlias.isNotEmpty()) { "Session alias cannot be empty" }
                    val sessionAlias: String = request.connectivityId.sessionAlias
                    val direction = directionOrDefault(request.direction)

                    check((request.messageFiltersList.isEmpty() && request.rootMessageFiltersList.isNotEmpty())
                            || (request.messageFiltersList.isNotEmpty() && request.rootMessageFiltersList.isEmpty())) {
                        "Either messageFilters or rootMessageFilters must be set but not both"
                    }

                    val protoMessageFilters: List<RootMessageFilter> = request.rootMessageFiltersList.ifEmpty {
                        request.messageFiltersList.map { it.toRootMessageFilter() }
                    }

                    SequenceCheckRuleTask(
                            request.description,
                            Instant.now(),
                            SessionKey(sessionAlias, direction),
                            request.timeout,
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


    private inline fun <T : GeneratedMessageV3, R : AbstractCheckTask> ruleCreation(request: T, parentEventId: EventID, block: RuleCreationContext<T, R>.() -> Unit): R {
        val ruleCreationContext = RuleCreationContext<T, R>().apply(block)
        try {
            return ruleCreationContext.action(request)
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
        check(this.messageType.isNotBlank()) { "Rule cannot be executed because the message filter does not contain 'message type'" }
        return RootMessageFilter.newBuilder()
                .setMessageType(this.messageType)
                .setComparisonSettings(this.comparisonSettings.toRootComparisonSettings())
                .setMessageFilter(this)
                .build()
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

    private class RuleCreationContext<T : GeneratedMessageV3, R : AbstractCheckTask> {
        lateinit var action: (T) -> R
        lateinit var event: () -> Event

        fun checkAndCreateRule(block: (T) -> R) {
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