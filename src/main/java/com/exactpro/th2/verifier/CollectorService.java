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
package com.exactpro.th2.verifier;

import static com.exactpro.sf.comparison.ComparisonUtil.getStatusType;
import static com.exactpro.sf.scriptrunner.StatusType.FAILED;
import static com.exactpro.th2.common.event.EventUtils.createMessageBean;
import static com.exactpro.th2.verifier.event.CheckSequenceUtils.createBothSide;
import static com.exactpro.th2.verifier.event.CheckSequenceUtils.createOnlyActualSide;
import static com.exactpro.th2.verifier.event.CheckSequenceUtils.createOnlyExpectedSide;
import static com.exactpro.th2.verifier.util.VerificationUtil.toMetaContainer;
import static java.lang.String.format;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.sf.aml.script.actions.WaitAction;
import com.exactpro.sf.common.messages.IMessage;
import com.exactpro.sf.common.util.Pair;
import com.exactpro.sf.comparison.ComparatorSettings;
import com.exactpro.sf.comparison.ComparisonResult;
import com.exactpro.sf.comparison.MessageComparator;
import com.exactpro.sf.services.ICSHIterator;
import com.exactpro.th2.MessageWrapper;
import com.exactpro.th2.ProtoToIMessageConverter;
import com.exactpro.th2.RabbitMqSubscriber;
import com.exactpro.th2.common.event.Event;
import com.exactpro.th2.common.event.Event.Status;
import com.exactpro.th2.common.event.bean.builder.MessageBuilder;
import com.exactpro.th2.common.event.bean.builder.TableBuilder;
import com.exactpro.th2.configuration.MicroserviceConfiguration;
import com.exactpro.th2.configuration.RabbitMQConfiguration;
import com.exactpro.th2.configuration.Th2Configuration.QueueNames;
import com.exactpro.th2.eventstore.grpc.EventStoreServiceService;
import com.exactpro.th2.eventstore.grpc.Response;
import com.exactpro.th2.eventstore.grpc.StoreEventBatchRequest;
import com.exactpro.th2.infra.grpc.ConnectionID;
import com.exactpro.th2.infra.grpc.Direction;
import com.exactpro.th2.infra.grpc.EventBatch;
import com.exactpro.th2.infra.grpc.EventID;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageFilter;
import com.exactpro.th2.infra.grpc.MessageID;
import com.exactpro.th2.schema.grpc.router.GrpcRouter;
import com.exactpro.th2.schema.message.MessageRouter;
import com.exactpro.th2.schema.message.SubscriberMonitor;
import com.exactpro.th2.verifier.configuration.VerifierConfiguration;
import com.exactpro.th2.verifier.event.bean.CheckSequenceRow;
import com.exactpro.th2.verifier.event.bean.builder.VerificationBuilder;
import com.exactpro.th2.verifier.grpc.CheckRuleRequest;
import com.exactpro.th2.verifier.grpc.CheckSequenceRuleRequest;
import com.exactpro.th2.verifier.grpc.CheckpointRequestOrBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.TextFormat;
import com.rabbitmq.client.DeliverCallback;

public class CollectorService {
    private final Logger logger = LoggerFactory.getLogger(getClass().getName() + '@' + hashCode());

    /**
     * Queue name to subscriber. Messages with different connectivity can be transferred with one queue.
     */
    private final SubscriberMonitor subscriberMonitor;
    private final MessageCollector messageCollector;
    private final ProtoToIMessageConverter converter = new ProtoToIMessageConverter(new DefaultMessageFactoryProxy(), null, null);
    private final EventStoreServiceService eventStoreConnector;

    public CollectorService(MessageRouter<MessageBatch> parsedMessageRouter, GrpcRouter grpcRouter, VerifierConfiguration configuration) throws InterruptedException {
        messageCollector = new MessageCollector(configuration.getCollectorMaxSize());

        subscriberMonitor = parsedMessageRouter.subscribeAll(this::handleIncamingMessage);
        try {
            eventStoreConnector = grpcRouter.getService(EventStoreServiceService.class);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Can not find event store class", e);
        }
    }

    private void handleIncamingMessage(String consumerTag, MessageBatch batch) {
        try {
            messageCollector.handle(batch);
        } catch (RuntimeException e) {
            if (logger.isErrorEnabled()) {
                logger.error("Handle batch problem, tag '{}', body '{}'", consumerTag, batch, e);
            }
        }
    }

    public void verifyCheckRule(CheckRuleRequest checkRuleRequest, Instant startTime) throws InterruptedException {
        Event rootEvent = Event.from(startTime)
                .description(checkRuleRequest.getDescription());
        try {
            Checkpoint checkpoint = Checkpoint.convert(checkRuleRequest.getCheckpoint());
            VerifyIterator<MessageWrapper> iterator = messageCollector.createIterator(checkRuleRequest.getConnectivityId().getSessionAlias(), Direction.FIRST, checkpoint);
            try {
                ComparatorSettings settings = createComparatorSettings(checkRuleRequest.getFilter());
                List<Pair<IMessage, ComparisonResult>> comparisonResults = waitMessage(checkRuleRequest.getFilter(),
                        checkRuleRequest.getTimeout(), (VerifyIterator)iterator, settings);
                logResults(comparisonResults);
                rootEvent.endTimestamp();

                if (comparisonResults.isEmpty()) {
                    rootEvent.name("No message found by target keys")
                            .status(Status.FAILED);
                    logger.error("CheckRule failed. No message found by target keys. Request = " + checkRuleRequest);
                    return;
                }
                Pair<IMessage, ComparisonResult> comparisonResultPair = comparisonResults.get(0);
                appendEventWithVerification(rootEvent, (MessageWrapper)comparisonResultPair.getFirst(), checkRuleRequest.getFilter(), comparisonResultPair.getSecond());
            } finally {
                messageCollector.removeIterator(checkRuleRequest.getConnectivityId().getSessionAlias(), Direction.FIRST, iterator);
            }
        } catch (InterruptedException | RuntimeException e) {
            rootEvent.status(Status.FAILED)
                    .bodyData(createMessageBean(e.getMessage()));
            throw e;
        } finally {
            try {
                sendEvents(checkRuleRequest.getParentEventId(), rootEvent);
            } catch (JsonProcessingException | RuntimeException e) {
                logger.error("Sending events '{}' with parent '{}' failed ",
                        rootEvent, checkRuleRequest.getParentEventId(), e);
            }
        }
    }

    public void verifyCheckSequenceRule(CheckSequenceRuleRequest checkSequenceRuleRequest, Instant startTime) throws InterruptedException {
        Instant verifyStartTime = Instant.now();
        Event rootEvent = Event.from(startTime)
                .name("CheckSequenceRule " + checkSequenceRuleRequest.getConnectivityId().getSessionAlias())
                .description(checkSequenceRuleRequest.getDescription())
                .type("checkSequenceRule");

        try {

            MessageFilter preMessageFilter = MessageFilter.newBuilder()
                    .setMessageType("PreFilter")
                    .putAllFields(checkSequenceRuleRequest.getPreFilter().getFieldsMap())
                    .build();
            IMessage preFilter = converter.fromProtoFilter(preMessageFilter, preMessageFilter.getMessageType());

            List<MessageFilter> messageFilters = new ArrayList<>(checkSequenceRuleRequest.getMessageFiltersList());

            String connectivityId = checkSequenceRuleRequest.getConnectivityId().getSessionAlias();
            Checkpoint checkpoint = Checkpoint.convert(checkSequenceRuleRequest.getCheckpoint());
            VerifyIterator<MessageWrapper> iterator = messageCollector.createIterator(checkSequenceRuleRequest.getConnectivityId().getSessionAlias(), Direction.FIRST, checkpoint);
            try {
                //region create events for pre-filtering
                Event preFilterEvent = rootEvent.addSubEvent(Event.from(verifyStartTime))
                        .type("preFiltering");
                boolean reordered = false;
                long handledByPreFilter = 0;
                Map<MessageID, Result> preFilteringResults = new LinkedHashMap<>();
                Map<MessageID, Result> messageFilteringResults = new LinkedHashMap<>();
                long timeout = System.currentTimeMillis() + checkSequenceRuleRequest.getTimeout();
                while (iterator.hasNext(timeout - System.currentTimeMillis())) {
                    handledByPreFilter++;
                    MessageWrapper actualMessage = iterator.next();
                    preFilterEvent.messageID(actualMessage.getMessageId());

                    ComparatorSettings settingsPreFilter = createComparatorSettings(preMessageFilter);
                    ComparisonResult preFilteringResult = MessageComparator.compare(actualMessage, preFilter, settingsPreFilter, false);
                    logger.debug("Prefilter message {}", preFilteringResult);
                    if (preFilteringResult == null || getStatusType(preFilteringResult) == FAILED) {
                        continue;
                    }
                    preFilteringResults.put(actualMessage.getMessageId(), new Result(actualMessage, preMessageFilter, preFilteringResult));

                    for (MessageFilter messageFilter : messageFilters) {
                        ComparatorSettings settings = createComparatorSettings(messageFilter);
                        IMessage expectedMessage = converter.fromProtoFilter(messageFilter, messageFilter.getMessageType());
                        ComparisonResult comparisonResult = MessageComparator.compare(actualMessage, expectedMessage, settings);
                        if (comparisonResult != null/* && ComparisonUtil.getStatusType(comparisonResult) != FAILED*/) {
                            if (messageFilters.indexOf(messageFilter) != 0) {
                                reordered = true;
                            }
                            messageFilteringResults.put(actualMessage.getMessageId(), new Result(actualMessage, messageFilter, comparisonResult));
                            messageFilters.remove(messageFilter);
                            iterator.updateCheckPoint();
                            break;
                        }
                    }
                }

                appendEventWithVerifications(preFilterEvent, verifyStartTime, preFilteringResults.values())
                        .name("Pre-filtering (filtered " + preFilteringResults.size() + " / processed " + handledByPreFilter + ") messages")
                        .endTimestamp();
                //endregion

                //region create events for check sequence
                TableBuilder<CheckSequenceRow> sequenceTable = new TableBuilder<>();
                preFilteringResults.forEach((messageID, actualResult) -> {
                    Result filtredResult = messageFilteringResults.get(messageID);
                    if (filtredResult != null) {
                        sequenceTable.row(createBothSide(filtredResult, connectivityId));
                    } else {
                        sequenceTable.row(createOnlyActualSide(actualResult.getActual(), connectivityId));
                    }
                });
                messageFilters.forEach(messageFilter -> {
                    sequenceTable.row(createOnlyExpectedSide(messageFilter, connectivityId));
                });

                rootEvent.addSubEvent(Event.from(verifyStartTime)
                        .name("Check sequence (expected " + checkSequenceRuleRequest.getMessageFiltersList().size()
                                + " / actual " + preFilteringResults.size()
                                + " , check order " + checkSequenceRuleRequest.getCheckOrder() + ')')
                        .type("checkSequence")
                        .status(checkSequenceRuleRequest.getMessageFiltersList().size() == preFilteringResults.size()
                                && !(checkSequenceRuleRequest.getCheckOrder() && reordered)
                                ? Status.PASSED
                                : Status.FAILED)
                        .bodyData(new MessageBuilder()
                                .text("Expected " + checkSequenceRuleRequest.getMessageFiltersList().size() +
                                        ", Actual " + preFilteringResults.size() +
                                        (checkSequenceRuleRequest.getCheckOrder() ? ", " + (reordered ? "Out of order" : "In order") : ""))
                                .build())
                        .bodyData(sequenceTable.build()))
                        .endTimestamp();
                //endregion

                //region create events for check messages
                Event checkMessagesEvent = rootEvent.addSubEvent(Event.from(verifyStartTime))
                        .name("Check messages")
                        .type("checkMessages");
                if (checkSequenceRuleRequest.getMessageFiltersList().size() != messageFilteringResults.size()) {
                    checkMessagesEvent.status(Status.FAILED)
                            .bodyData(createMessageBean("Incorrect number of comparisons (expected " + checkSequenceRuleRequest.getMessageFiltersList().size()
                                    + " / " + "actual " + messageFilteringResults.size() + ')'));
                } else {
                    checkMessagesEvent.bodyData(createMessageBean("Contains comparisons"));
                }
                appendEventWithVerifications(checkMessagesEvent, verifyStartTime, messageFilteringResults.values())
                        .endTimestamp();
                //endregion
            } finally {
                messageCollector.removeIterator(checkSequenceRuleRequest.getConnectivityId().getSessionAlias(), Direction.FIRST, iterator);
            }
        } catch (InterruptedException | RuntimeException e) {
            rootEvent.status(Status.FAILED)
                    .bodyData(createMessageBean(e.getMessage()));
        } finally {
            try {
                sendEvents(checkSequenceRuleRequest.getParentEventId(), rootEvent);
            } catch (JsonProcessingException | RuntimeException e) {
                logger.error("Sending events '{}' with parent '{}' failed ",
                        rootEvent, checkSequenceRuleRequest.getParentEventId(), e);
            }
        }
    }

    private void sendEvents(EventID parentEventID, Event event) throws JsonProcessingException {
        logger.debug("Sending event thee id '{}' parent id '{}'", event.getId(), parentEventID);
        StoreEventBatchRequest storeRequest = StoreEventBatchRequest.newBuilder()
                .setEventBatch(EventBatch.newBuilder()
                        .setParentEventId(parentEventID)
                        .addAllEvents(event.toProtoEvents(parentEventID.getId()))
                        .build())
                .build();
        Response response = eventStoreConnector.storeEventBatch(storeRequest);
        if (logger.isDebugEnabled()) {
            logger.debug("Sent event batch '{}' with result {}", TextFormat.shortDebugString(storeRequest), parentEventID, response);
        }
    }

    /**
     * Generates children events and attaches to parent event
     * @param verifyStartTime is time of start for children events
     * @param comparisonResults is iterable for generate children events
     * @return parent event
     */
    private static Event appendEventWithVerifications(Event parentEvent, Instant verifyStartTime, Iterable<Result> comparisonResults) {
        for (Result result : comparisonResults) {
            appendEventWithVerification(parentEvent.addSubEvent(Event.from(verifyStartTime)),
                    (MessageWrapper)result.getActual(), result.getMessageFilter(), result.getComparisonResult());
        }
        return parentEvent;
    }

    public void close() {
        try {
            subscriberMonitor.unsubscribe();
        } catch (Exception e) {
            throw new RuntimeException("Can not unsubscribe from all queues", e);
        }
    }

    public Checkpoint createCheckpoint(CheckpointRequestOrBuilder request) {
        Event rootEvent = Event.start()
                .name("Checkpoint")
                .type("Checkpoint")
                .description(request.getDescription());
        try {
            Checkpoint checkpoint = messageCollector.createCheckpoint();

            rootEvent.endTimestamp()
                    .bodyData(createMessageBean("Checkpoint id '" + checkpoint.getId() + '\''));

            checkpoint.asMap().forEach((sessionKey, sequence) -> {
                MessageID messageID = createMessageID(sessionKey, sequence);
                rootEvent.messageID(messageID)
                        .addSubEventWithSamePeriod()
                        .name("Checkpoint for session alias '" + sessionKey.getSessionAlias() + "' direction '" + sessionKey.getDirection() + "' sequence '" + sequence + '\'')
                        .type("Checpoint for session")
                        .messageID(messageID);
            });
            return checkpoint;
        } finally {
            try {
                if (request.hasParentEventId()) {
                    sendEvents(request.getParentEventId(), rootEvent);
                } else {
                    logger.warn("Parent id missed in request");
                }
            } catch (JsonProcessingException | RuntimeException e) {
                logger.error("Sending events '{}' with a parent '{}' failed ",
                        rootEvent, request.getParentEventId(), e);
            }
        }
    }

    @NotNull
    private static MessageID createMessageID(SessionKey sessionKey, Long sequence) {
        return MessageID.newBuilder()
                .setConnectionId(ConnectionID.newBuilder()
                        .setSessionAlias(sessionKey.getSessionAlias())
                        .build())
                .setSequence(sequence)
                .setDirection(sessionKey.getDirection())
                .build();
    }

    private List<Pair<IMessage, ComparisonResult>> waitMessage(MessageFilter filter, long timeout,
            ICSHIterator<IMessage> iterator, ComparatorSettings settings)
            throws InterruptedException {
        IMessage convertedFilter = converter.fromProtoFilter(filter, filter.getMessageType());
        //logger.debug("Converted filter {}", convertedFilter);
        return WaitAction.waitMessage(settings, convertedFilter,
                iterator, timeout, Collections.emptyList(), false);
    }

    private void logResults(Iterable<Pair<IMessage, ComparisonResult>> comparisonResults) {
        if (logger.isDebugEnabled()) {
            comparisonResults.forEach(pair -> logger.debug("'{}': {}",
                    pair.getFirst().getName(), pair.getSecond()));
        }
    }

    private static void appendEventWithVerification(Event event, MessageWrapper actualMessage, MessageFilter messageFilter, ComparisonResult comparisonResult) {
        VerificationBuilder verificationComponent = new VerificationBuilder();
        if (!comparisonResult.getResults().isEmpty()) {
            comparisonResult.getResults().forEach((key, value) -> verificationComponent.verification(key, value, messageFilter, true));
        }

        event.name("Verification '" + actualMessage.getName() + "' message")
                .status(getStatusType(comparisonResult) == FAILED ? Status.FAILED : Status.PASSED)
                .messageID(actualMessage.getMessageId())
                .bodyData(verificationComponent.build());
    }

    private static ComparatorSettings createComparatorSettings(MessageFilter filter) {
        ComparatorSettings settings = new ComparatorSettings();
        settings.setMetaContainer(toMetaContainer(filter, false));
        return settings;
    }

    public class Result {
        private final IMessage actual;
        private final MessageFilter messageFilter;
        private final ComparisonResult comparisonResult;

        public Result(IMessage actual, MessageFilter expected, ComparisonResult comparisonResult) {
            this.actual = actual;
            this.messageFilter = expected;
            this.comparisonResult = comparisonResult;
        }

        public IMessage getActual() {
            return actual;
        }

        public MessageFilter getMessageFilter() {
            return messageFilter;
        }

        public ComparisonResult getComparisonResult() {
            return comparisonResult;
        }
    }

    private Map<String, RabbitMqSubscriber> subscribe(MicroserviceConfiguration configuration, DeliverCallback deliverCallback) throws InterruptedException {
        RabbitMQConfiguration rabbitMQConfiguration = configuration.getRabbitMQ();
        Map<String, RabbitMqSubscriber> subscribers = new HashMap<>();
        for (Entry<String, QueueNames> connectivityQueueNames
                : configuration.getTh2().getConnectivityQueueNames().entrySet()) {
            String connectivityAlias = connectivityQueueNames.getKey();
            QueueNames queueNames = connectivityQueueNames.getValue();
            try {
                subscribers.computeIfAbsent(queueNames.getInQueueName(), queueName -> {
                    try {
                        var subscriber = new RabbitMqSubscriber(queueNames.getExchangeName(), deliverCallback, null,
                                queueNames.getInQueueName());
                        subscriber.startListening(
                                rabbitMQConfiguration.getHost(),
                                rabbitMQConfiguration.getVirtualHost(),
                                rabbitMQConfiguration.getPort(),
                                rabbitMQConfiguration.getUsername(),
                                rabbitMQConfiguration.getPassword(),
                                "Verify:" + connectivityAlias);
                        logger.info("Subscribed to queue '{}', from connectivity '{}'", queueName, connectivityAlias);
                        return subscriber;
                    } catch (IOException | TimeoutException e) {
                        throw new RuntimeException(format("Subscription to queue '%s' failure, from connectivity '%'",
                                queueName, connectivityAlias), e);
                    }
                });
            } catch (RuntimeException e) {
                logger.error("Subscribing to message queue failure. queue '{}', connectivity '{}'",
                        queueNames.getInQueueName(), connectivityAlias, e);
                throw e;
            }
        }
        return subscribers;
    }
}