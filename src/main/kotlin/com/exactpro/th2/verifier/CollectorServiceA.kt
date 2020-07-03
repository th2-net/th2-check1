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

import com.exactpro.sf.aml.script.actions.WaitAction
import com.exactpro.sf.common.messages.IMessage
import com.exactpro.sf.common.util.Pair
import com.exactpro.sf.comparison.ComparatorSettings
import com.exactpro.sf.comparison.ComparisonResult
import com.exactpro.sf.comparison.ComparisonUtil
import com.exactpro.sf.comparison.MessageComparator
import com.exactpro.sf.scriptrunner.StatusType
import com.exactpro.sf.services.ICSHIterator
import com.exactpro.th2.MessageWrapper
import com.exactpro.th2.ProtoToIMessageConverter
import com.exactpro.th2.RabbitMqSubscriber
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.Event.Status.FAILED
import com.exactpro.th2.common.event.Event.Status.PASSED
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.bean.builder.MessageBuilder
import com.exactpro.th2.common.event.bean.builder.TableBuilder
import com.exactpro.th2.common.message.message
import com.exactpro.th2.configuration.MicroserviceConfiguration
import com.exactpro.th2.configuration.Th2Configuration.QueueNames
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc
import com.exactpro.th2.eventstore.grpc.EventStoreServiceGrpc.EventStoreServiceBlockingStub
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest
import com.exactpro.th2.infra.grpc.ConnectionID
import com.exactpro.th2.infra.grpc.Direction
import com.exactpro.th2.infra.grpc.Direction.FIRST
import com.exactpro.th2.infra.grpc.EventBatch
import com.exactpro.th2.infra.grpc.EventID
import com.exactpro.th2.infra.grpc.Message
import com.exactpro.th2.infra.grpc.MessageBatch
import com.exactpro.th2.infra.grpc.MessageFilter
import com.exactpro.th2.infra.grpc.MessageID
import com.exactpro.th2.verifier.CollectorService.Result
import com.exactpro.th2.verifier.event.CheckSequenceUtils
import com.exactpro.th2.verifier.event.bean.CheckSequenceRow
import com.exactpro.th2.verifier.event.bean.builder.VerificationBuilder
import com.exactpro.th2.verifier.grpc.CheckRuleRequest
import com.exactpro.th2.verifier.grpc.CheckSequenceRuleRequest
import com.exactpro.th2.verifier.grpc.CheckpointRequestOrBuilder
import com.exactpro.th2.verifier.util.VerificationUtil
import com.fasterxml.jackson.core.JsonProcessingException
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.TextFormat
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
import java.util.ArrayList
import java.util.Arrays
import java.util.LinkedHashMap
import java.util.Objects.requireNonNull
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.function.Consumer

class CollectorServiceA(configuration: MicroserviceConfiguration) {
    private val logger = LoggerFactory.getLogger(javaClass.name + '@' + hashCode())

    /**
     * Queue name to subscriber. Messages with different connectivity can be transferred with one queue.
     */
    private val subscribers: Collection<RabbitMqSubscriber>
    private val messageCollector: MessageCollector
    private val converter = ProtoToIMessageConverter(DefaultMessageFactoryProxy(), null, null)
    private val eventStoreConnector: EventStoreServiceBlockingStub

    private val streamObservable : Observable<StreamContainer>
    private val checkpointSubscriber : CheckpointSubscriber
    private val mqSubject : PublishSubject<ByteArray>
    private val scheduler = ScheduledThreadPoolExecutor(1)

    @Throws(InvalidProtocolBufferException::class)
    private fun handleIncamingMessage(consumerTag: String, delivery: Delivery) {
        try {
            val batch = MessageBatch.parseFrom(delivery.body)
            messageCollector.handle(batch)
        } catch (e: InvalidProtocolBufferException) {
            if (logger.isErrorEnabled) {
                logger.error("Parse batch failure, tag '{}', body '{}'", consumerTag, Arrays.toString(delivery.body), e)
            }
            throw e
        } catch (e: RuntimeException) {
            if (logger.isErrorEnabled) {
                logger.error("Handle batch problem, tag '{}', body '{}'", consumerTag, Arrays.toString(delivery.body), e)
            }
        }
    }

    @Throws(InterruptedException::class)
    fun verifyCheckRule(checkRuleRequest: CheckRuleRequest, startTime: Instant) {
        val sessionAlias : String = requireNonNull(checkRuleRequest.connectivityId.sessionAlias, "Session alias cant't be null")
//        streamObservable.mapToMessage(sessionAlias, checkRuleRequest.checkpoint.getSequence(sessionAlias))
//        TODO
//
//        val rootEvent = Event.from(startTime)
//            .description(checkRuleRequest.description)
//        try {
//            val checkpoint = Checkpoint.convert(checkRuleRequest.checkpoint)
//            val iterator = messageCollector.createIterator(checkRuleRequest.connectivityId.sessionAlias, FIRST, checkpoint)
//            try {
//                val settings = createComparatorSettings(checkRuleRequest.filter)
//                val comparisonResults = waitMessage(checkRuleRequest.filter,
//                    checkRuleRequest.timeout, iterator as VerifyIterator<*>, settings)
//                logResults(comparisonResults)
//                rootEvent.endTimestamp()
//                if (comparisonResults.isEmpty()) {
//                    rootEvent.name("No message found by target keys")
//                        .status(FAILED)
//                    logger.error("CheckRule failed. No message found by target keys. Request = $checkRuleRequest")
//                    return
//                }
//                val comparisonResultPair = comparisonResults[0]
//                appendEventWithVerification(rootEvent, comparisonResultPair.first as MessageWrapper, checkRuleRequest.filter, comparisonResultPair.second)
//            } finally {
//                messageCollector.removeIterator(checkRuleRequest.connectivityId.sessionAlias, FIRST, iterator)
//            }
//        } catch (e: InterruptedException) {
//            rootEvent.status(FAILED)
//                .bodyData(EventUtils.createMessageBean(e.message))
//            throw e
//        } catch (e: RuntimeException) {
//            rootEvent.status(FAILED)
//                .bodyData(EventUtils.createMessageBean(e.message))
//            throw e
//        } finally {
//            try {
//                sendEvents(checkRuleRequest.parentEventId, rootEvent)
//            } catch (e: JsonProcessingException) {
//                logger.error("Sending events '{}' with parent '{}' failed ",
//                    rootEvent, checkRuleRequest.parentEventId, e)
//            } catch (e: RuntimeException) {
//                logger.error("Sending events '{}' with parent '{}' failed ",
//                    rootEvent, checkRuleRequest.parentEventId, e)
//            }
//        }
    }

    @Throws(InterruptedException::class)
    fun verifyCheckSequenceRule(checkSequenceRuleRequest: CheckSequenceRuleRequest, startTime: Instant?) {
//        val verifyStartTime = Instant.now()
//        val rootEvent = Event.from(startTime)
//            .name("CheckSequenceRule " + checkSequenceRuleRequest.connectivityId.sessionAlias)
//            .description(checkSequenceRuleRequest.description)
//            .type("checkSequenceRule")
//        try {
//            val preMessageFilter = MessageFilter.newBuilder()
//                .setMessageType("PreFilter")
//                .putAllFields(checkSequenceRuleRequest.preFilter.fieldsMap)
//                .build()
//            val preFilter = converter.fromProtoFilter(preMessageFilter, preMessageFilter.messageType)
//            val messageFilters: MutableList<MessageFilter> = ArrayList(checkSequenceRuleRequest.messageFiltersList)
//            val connectivityId = checkSequenceRuleRequest.connectivityId.sessionAlias
//            val checkpoint = Checkpoint.convert(checkSequenceRuleRequest.checkpoint)
//            val iterator = messageCollector.createIterator(checkSequenceRuleRequest.connectivityId.sessionAlias, FIRST, checkpoint)
//            try {
//                //region create events for pre-filtering
//                val preFilterEvent = rootEvent.addSubEvent(Event.from(verifyStartTime))
//                    .type("preFiltering")
//                var reordered = false
//                var handledByPreFilter: Long = 0
//                val preFilteringResults: MutableMap<MessageID, Result> = LinkedHashMap()
//                val messageFilteringResults: MutableMap<MessageID, Result> = LinkedHashMap()
//                val timeout = System.currentTimeMillis() + checkSequenceRuleRequest.timeout
//                while (iterator.hasNext(timeout - System.currentTimeMillis())) {
//                    handledByPreFilter++
//                    val actualMessage = iterator.next()
//                    preFilterEvent.messageID(actualMessage.messageId)
//                    val settingsPreFilter = createComparatorSettings(preMessageFilter)
//                    val preFilteringResult = MessageComparator.compare(actualMessage, preFilter, settingsPreFilter, false)
//                    logger.debug("Prefilter message {}", preFilteringResult)
//                    if (preFilteringResult == null || ComparisonUtil.getStatusType(preFilteringResult) == StatusType.FAILED) {
//                        continue
//                    }
//                    preFilteringResults[actualMessage.messageId] = Result(actualMessage, preMessageFilter, preFilteringResult)
//                    for (messageFilter in messageFilters) {
//                        val settings = createComparatorSettings(messageFilter)
//                        val expectedMessage = converter.fromProtoFilter(messageFilter, messageFilter.messageType)
//                        val comparisonResult = MessageComparator.compare(actualMessage, expectedMessage, settings)
//                        if (comparisonResult != null /* && ComparisonUtil.getStatusType(comparisonResult) != FAILED*/) {
//                            if (messageFilters.indexOf(messageFilter) != 0) {
//                                reordered = true
//                            }
//                            messageFilteringResults[actualMessage.messageId] = Result(actualMessage, messageFilter, comparisonResult)
//                            messageFilters.remove(messageFilter)
//                            iterator.updateCheckPoint()
//                            break
//                        }
//                    }
//                }
//                appendEventWithVerifications(preFilterEvent, verifyStartTime, preFilteringResults.values)
//                    .name("Pre-filtering (filtered " + preFilteringResults.size + " / processed " + handledByPreFilter + ") messages")
//                    .endTimestamp()
//                //endregion
//
//                //region create events for check sequence
//                val sequenceTable = TableBuilder<CheckSequenceRow>()
//                preFilteringResults.forEach { (messageID: MessageID?, actualResult: Result) ->
//                    val filtredResult = messageFilteringResults[messageID]
//                    if (filtredResult != null) {
//                        sequenceTable.row(CheckSequenceUtils.createBothSide(filtredResult, connectivityId))
//                    } else {
//                        sequenceTable.row(CheckSequenceUtils.createOnlyActualSide(actualResult.actual, connectivityId))
//                    }
//                }
//                messageFilters.forEach(Consumer { messageFilter: MessageFilter? -> sequenceTable.row(CheckSequenceUtils.createOnlyExpectedSide(messageFilter, connectivityId)) })
//                rootEvent.addSubEvent(Event.from(verifyStartTime)
//                    .name("Check sequence (expected " + checkSequenceRuleRequest.messageFiltersList.size
//                        + " / actual " + preFilteringResults.size
//                        + " , check order " + checkSequenceRuleRequest.checkOrder + ')')
//                    .type("checkSequence")
//                    .status(if (checkSequenceRuleRequest.messageFiltersList.size == preFilteringResults.size
//                        && !(checkSequenceRuleRequest.checkOrder && reordered)) PASSED else FAILED)
//                    .bodyData(MessageBuilder()
//                        .text("Expected " + checkSequenceRuleRequest.messageFiltersList.size +
//                            ", Actual " + preFilteringResults.size +
//                            if (checkSequenceRuleRequest.checkOrder) ", " + (if (reordered) "Out of order" else "In order") else "")
//                        .build())
//                    .bodyData(sequenceTable.build()))
//                    .endTimestamp()
//                //endregion
//
//                //region create events for check messages
//                val checkMessagesEvent = rootEvent.addSubEvent(Event.from(verifyStartTime))
//                    .name("Check messages")
//                    .type("checkMessages")
//                if (checkSequenceRuleRequest.messageFiltersList.size != messageFilteringResults.size) {
//                    checkMessagesEvent.status(FAILED)
//                        .bodyData(EventUtils.createMessageBean("Incorrect number of comparisons (expected " + checkSequenceRuleRequest.messageFiltersList.size
//                            + " / " + "actual " + messageFilteringResults.size + ')'))
//                } else {
//                    checkMessagesEvent.bodyData(EventUtils.createMessageBean("Contains comparisons"))
//                }
//                appendEventWithVerifications(checkMessagesEvent, verifyStartTime, messageFilteringResults.values)
//                    .endTimestamp()
//                //endregion
//            } finally {
//                messageCollector.removeIterator(checkSequenceRuleRequest.connectivityId.sessionAlias, FIRST, iterator)
//            }
//        } catch (e: InterruptedException) {
//            rootEvent.status(FAILED)
//                .bodyData(EventUtils.createMessageBean(e.message))
//        } catch (e: RuntimeException) {
//            rootEvent.status(FAILED)
//                .bodyData(EventUtils.createMessageBean(e.message))
//        } finally {
//            try {
//                sendEvents(checkSequenceRuleRequest.parentEventId, rootEvent)
//            } catch (e: JsonProcessingException) {
//                logger.error("Sending events '{}' with parent '{}' failed ",
//                    rootEvent, checkSequenceRuleRequest.parentEventId, e)
//            } catch (e: RuntimeException) {
//                logger.error("Sending events '{}' with parent '{}' failed ",
//                    rootEvent, checkSequenceRuleRequest.parentEventId, e)
//            }
//        }
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
        val response = eventStoreConnector.storeEventBatch(storeRequest)
        if (logger.isDebugEnabled) {
            logger.debug("Sent event batch '{}' with result {}", shortDebugString(storeRequest), parentEventID, response)
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
        mqSubject.onComplete();
    }

    fun createCheckpoint(request: CheckpointRequestOrBuilder): Checkpoint {
        val rootEvent = Event.start()
            .name("Checkpoint")
            .type("Checkpoint")
            .description(request.description)
        return try {
            val checkpoint = checkpointSubscriber.createCheckpoint()
            rootEvent.endTimestamp()
                .bodyData(EventUtils.createMessageBean("Checkpoint id '" + checkpoint.id + '\''))
            checkpoint.asMap().forEach { (sessionKey: SessionKey, sequence: Long) ->
                val messageID = createMessageID(sessionKey, sequence)
                rootEvent.messageID(messageID)
                    .addSubEventWithSamePeriod()
                    .name("Checkpoint for session alias '" + sessionKey.sessionAlias + "' direction '" + sessionKey.direction + "' sequence '" + sequence + '\'')
                    .type("Checpoint for session")
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
            } catch (e: JsonProcessingException) {
                logger.error("Sending events '{}' with a parent '{}' failed ",
                    rootEvent, request.parentEventId, e)
            } catch (e: RuntimeException) {
                logger.error("Sending events '{}' with a parent '{}' failed ",
                    rootEvent, request.parentEventId, e)
            }
        }
    }

    @Throws(InterruptedException::class)
    private fun waitMessage(filter: MessageFilter, timeout: Long,
                            iterator: ICSHIterator<IMessage>, settings: ComparatorSettings): List<Pair<IMessage, ComparisonResult>> {
        val convertedFilter = converter.fromProtoFilter(filter, filter.messageType)
        //logger.debug("Converted filter {}", convertedFilter);
        return WaitAction.waitMessage(settings, convertedFilter,
            iterator, timeout, emptyList(), false)
    }

    private fun logResults(comparisonResults: Iterable<Pair<IMessage, ComparisonResult>>) {
        if (logger.isDebugEnabled) {
            comparisonResults.forEach(Consumer { pair: Pair<IMessage, ComparisonResult> ->
                logger.debug("'{}': {}",
                    pair.first.name, pair.second)
            })
        }
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

    companion object {
        @Suppress("JAVA_CLASS_ON_COMPANION")
        @JvmStatic
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)

        /**
         * Generates children events and attaches to parent event
         * @param verifyStartTime is time of start for children events
         * @param comparisonResults is iterable for generate children events
         * @return parent event
         */
        private fun appendEventWithVerifications(parentEvent: Event, verifyStartTime: Instant, comparisonResults: Iterable<Result>): Event {
            for (result in comparisonResults) {
                appendEventWithVerification(parentEvent.addSubEvent(Event.from(verifyStartTime)),
                    result.actual as MessageWrapper, result.messageFilter, result.comparisonResult)
            }
            return parentEvent
        }

        private fun createMessageID(sessionKey: SessionKey, sequence: Long): MessageID {
            return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder()
                    .setSessionAlias(sessionKey.sessionAlias)
                    .build())
                .setSequence(sequence)
                .setDirection(sessionKey.direction)
                .build()
        }

        private fun appendEventWithVerification(event: Event, actualMessage: MessageWrapper, messageFilter: MessageFilter, comparisonResult: ComparisonResult) {
            val verificationComponent = VerificationBuilder()
            if (!comparisonResult.results.isEmpty()) {
                comparisonResult.results.forEach { (key: String?, value: ComparisonResult?) -> verificationComponent.verification(key, value, messageFilter, true) }
            }
            event.name("Verification '" + actualMessage.name + "' message")
                .status(if (ComparisonUtil.getStatusType(comparisonResult) == StatusType.FAILED) FAILED else PASSED)
                .messageID(actualMessage.messageId)
                .bodyData(verificationComponent.build())
        }

    }

    init {
        // TODO get limit size from configuration
        val limitSize = 1000
        messageCollector = MessageCollector(limitSize)

        mqSubject = PublishSubject.create()

        subscribers = subscribe(configuration, DeliverCallback { _: String, delivery: Delivery -> mqSubject.onNext(delivery.body) })
        streamObservable = mqSubject.map(MessageBatch::parseFrom)
            .flatMapIterable(MessageBatch::getMessagesList)
            .groupBy { message -> message.metadata.id.connectionId.sessionAlias }
            .map { group -> StreamContainer(group.key!!, limitSize, group) }
            .replay().apply { connect() }

        checkpointSubscriber = CheckpointSubscriber().apply {
            streamObservable.subscribe(this)
        }

        val th2Configuration = configuration.th2
        eventStoreConnector = EventStoreServiceGrpc.newBlockingStub(ManagedChannelBuilder.forAddress(
            th2Configuration.th2EventStorageGRPCHost, th2Configuration.th2EventStorageGRPCPort)
            .usePlaintext().build())
    }
}