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

import com.exactpro.th2.RabbitMqSubscriber
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.configuration.MicroserviceConfiguration
import com.exactpro.th2.configuration.Th2Configuration.QueueNames
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceFutureStub
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest
import com.exactpro.th2.infra.grpc.ConnectionID
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.verifier.grpc.CheckRuleRequest
import com.exactpro.th2.verifier.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.verifier.grpc.CheckpointRequestOrBuilder
import com.exactpro.th2.verifier.rule.AbstractCheckTask
import com.exactpro.th2.verifier.rule.check.CheckRuleTask
import com.exactpro.th2.verifier.rule.sequence.SequenceCheckRuleTask
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.protobuf.TextFormat.shortDebugString
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import io.grpc.ManagedChannelBuilder
import io.reactivex.Observable
import io.reactivex.subjects.PublishSubject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Instant
import java.util.Objects.requireNonNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool

class CollectorServiceA(configuration: MicroserviceConfiguration) {
    private val logger = LoggerFactory.getLogger(javaClass.name + '@' + hashCode())

    /**
     * Queue name to subscriber. Messages with different connectivity can be transferred with one queue.
     */
    private val subscribers: Collection<RabbitMqSubscriber>
    private val eventStoreStub: EventStoreServiceFutureStub
    private val streamObservable: Observable<StreamContainer>
    private val checkpointSubscriber: CheckpointSubscriber
    private val mqSubject: PublishSubject<ByteArray>
    private val eventIdToLastCheckTask: MutableMap<CheckTaskKey, AbstractCheckTask> = ConcurrentHashMap()

    @Throws(InterruptedException::class)
    fun verifyCheckRule(request: CheckRuleRequest) {
        val parentEventID: EventID = requireNonNull(request.parentEventId, "Parent event id can't be null")
        val sessionAlias: String = requireNonNull(request.connectivityId.sessionAlias, "Session alias cant't be null")
        val filter: MessageFilter = requireNonNull(request.filter, "Message filter can't be null")

        val task = CheckRuleTask(request.description, Instant.now(), sessionAlias, request.timeout, filter,
            parentEventID, streamObservable, eventStoreStub)

        eventIdToLastCheckTask.compute(CheckTaskKey(request.parentEventId, request.connectivityId)) { _, value ->
            if (value != null) {
                value.subscribeNextTask(task)
            } else {
                task.begin(request.checkpoint)
            }
            task
        }

        // TODO: try to remove prune tasks
    }

    @Throws(InterruptedException::class)
    fun verifyCheckSequenceRule(request: CheckSequenceRuleRequest) {
        val parentEventID: EventID = requireNonNull(request.parentEventId, "Parent event id can't be null")
        val sessionAlias: String = requireNonNull(request.connectivityId.sessionAlias, "Session alias cant't be null")

        val task = SequenceCheckRuleTask(request.description, Instant.now(), sessionAlias, request.timeout, request.preFilter,
            request.messageFiltersList, request.checkOrder, parentEventID, streamObservable, eventStoreStub)

        eventIdToLastCheckTask.compute(CheckTaskKey(request.parentEventId, request.connectivityId)) { _, value ->
            if (value != null) {
                value.subscribeNextTask(task)
            } else {
                task.begin(request.checkpoint)
            }
            task
        }
    }

    @Throws(JsonProcessingException::class)
    private fun sendEvents(parentEventID: EventID, event: Event) {
        logger.debug("Sending event thee id '{}' parent id '{}'", event.id, parentEventID)
        val storeRequest = StoreEventBatchRequest.newBuilder()
            .setEventBatch(EventBatch.newBuilder()
                .setParentEventId(parentEventID)
                .addAllEvents(event.toProtoEvents(parentEventID.id))
                .build())
            .build()
        val future = eventStoreStub.storeEventBatch(storeRequest)
        future.addListener(Runnable {
            if (logger.isDebugEnabled) {
                logger.debug("Sent event batch '{}' with result {}", shortDebugString(storeRequest), parentEventID, future.get())
            }
        }, ForkJoinPool.commonPool())
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
        for (subscriber in subscribers) {
            try {
                subscriber.close()
            } catch (e: IOException) {
                logger.error("Close subscriber failure", e)
            }
        }
        mqSubject.onComplete()
    }

    private fun subscribe(configuration: MicroserviceConfiguration, deliverCallback: DeliverCallback): List<RabbitMqSubscriber> {
        return configuration.th2.connectivityQueueNames.values
            .groupBy(QueueNames::getExchangeName, QueueNames::getInQueueName)
            .map {
                val exchangeName = it.key
                val queueNames = it.value.toTypedArray()
                RabbitMqSubscriber(exchangeName, deliverCallback, null, *queueNames).apply {
                    with(configuration.rabbitMQ) {
                        startListening(host, virtualHost, port, username, password, "Verify")
                    }
                }
            }
    }

    init {
        // TODO get limit size from configuration
        val limitSize = 1000
        mqSubject = PublishSubject.create()

        subscribers = subscribe(configuration, DeliverCallback { _: String, delivery: Delivery -> mqSubject.onNext(delivery.body) })
        streamObservable = mqSubject.map(MessageBatch::parseFrom)
            .flatMapIterable(MessageBatch::getMessagesList)
            .groupBy { message -> message.metadata.id.connectionId.sessionAlias }
            .map { group -> StreamContainer(group.key!!, limitSize, group) }
            .replay().apply { connect() }

        checkpointSubscriber = streamObservable.subscribeWith(CheckpointSubscriber())

        val th2Configuration = configuration.th2
        eventStoreStub = EventStoreServiceGrpc.newFutureStub(ManagedChannelBuilder.forAddress(
            th2Configuration.th2EventStorageGRPCHost, th2Configuration.th2EventStorageGRPCPort)
            .usePlaintext().build())
    }

    private fun SessionKey.toMessageID(sequence: Long) = MessageID.newBuilder()
        .setConnectionId(ConnectionID.newBuilder()
            .setSessionAlias(sessionAlias)
            .build())
        .setSequence(sequence)
        .setDirection(direction)
        .build()

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmField
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}