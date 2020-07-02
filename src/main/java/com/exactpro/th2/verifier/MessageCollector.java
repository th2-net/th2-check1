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
package com.exactpro.th2.verifier;

import static com.google.protobuf.TextFormat.shortDebugString;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.exactpro.th2.MessageWrapper;
import com.exactpro.th2.ProtoToIMessageConverter;
import com.exactpro.th2.infra.grpc.Direction;
import com.exactpro.th2.infra.grpc.Message;
import com.exactpro.th2.infra.grpc.MessageBatch;
import com.exactpro.th2.infra.grpc.MessageID;
import com.google.protobuf.TextFormat;

public class MessageCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageCollector.class);

    private final ProtoToIMessageConverter converter = new ProtoToIMessageConverter(new DefaultMessageFactoryProxy(), null, null);
    private final ConcurrentMap<SessionKey, MessageBuffer> connectivityMessages = new ConcurrentHashMap<>();
    private final int bufferSize;

    public MessageCollector(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public void handle(MessageBatch batch) {
        for (Message message : batch.getMessagesList()) {
            MessageID messageID = message.getMetadata().getId();
            SessionKey sessionKey = new SessionKey(messageID.getConnectionId().getSessionAlias(), messageID.getDirection());
            convertAndPut(sessionKey, message);
        }
    }

    private void convertAndPut(SessionKey sessionKey, Message message) {
        MessageWrapper messageWrapper = converter.fromProtoMessage(message, false);

        connectivityMessages.compute(sessionKey, (key, value) -> {
            if (value != null) {
                return value;
            }
            LOGGER.info("Created message buffer for session '{}'", key);
            return new MessageBuffer(bufferSize);
        }).put(messageWrapper);
    }

    public Checkpoint createCheckpoint() {
        Map<SessionKey, Long> sessionKeyToSequence = new HashMap<>();
        for (Map.Entry<SessionKey, MessageBuffer> entries : connectivityMessages.entrySet()) {
            sessionKeyToSequence.put(entries.getKey(), entries.getValue().getLastSequence());
        }
        return new Checkpoint(sessionKeyToSequence);
    }

    public VerifyIterator<MessageWrapper> createIterator(String sessionAlias, Direction direction, Checkpoint checkpoint) {
        SessionKey sessionKey = new SessionKey(sessionAlias, direction);
        MessageBuffer messageBuffer = connectivityMessages.get(sessionKey);
        if (messageBuffer == null) {
            throw new RuntimeException("Iterator can't be created for unknown session '" + sessionKey + "'");
        }
        if (checkpoint.contains(sessionKey)) {
            long sequence = checkpoint.getSequence(sessionKey);
            LOGGER.info("Use sequence '{}' from checkpoint for session '{}'", sequence, sessionKey);
            return messageBuffer.createIterator(sequence);
        }
        LOGGER.warn("Checkpoint '{}' doesn't contain sequence for session '{}'", checkpoint, sessionKey);
        return messageBuffer.createIterator();
    }

    public void removeIterator(String sessionAlias, Direction direction, VerifyIterator<MessageWrapper> iterator) {
        SessionKey sessionKey = new SessionKey(sessionAlias, direction);
        MessageBuffer messageBuffer = connectivityMessages.get(sessionKey);
        if (messageBuffer != null) {
            messageBuffer.removeIterator(iterator);
        } else {
            LOGGER.warn("Iterator can't be removed from unknown session '" + sessionKey + "'");
        }
    }

    @ThreadSafe
    private static class MessageBuffer {
        private volatile long lastSequence = Long.MIN_VALUE;
        private final Collection<MessageWrapper> messages;
        private final List<VerifyIterator<MessageWrapper>> iterators;

        private MessageBuffer(int bufferSize) {
            messages = new CircularFifoQueue<>(bufferSize);
            iterators = new CopyOnWriteArrayList<>();
        }

        public void put(MessageWrapper message) {
            synchronized (messages) {
                messages.add(message);
                for (VerifyIterator<MessageWrapper> iterator : iterators) {
                    iterator.onMessage(message);
                }
                lastSequence = message.getMessageId().getSequence();
            }
        }

        public long getLastSequence() {
            return lastSequence;
        }

        public VerifyIterator<MessageWrapper> createIterator(long lastSequence) {
            synchronized (messages) {
                List<MessageWrapper> subCollection = messages.stream()
                        .filter(message -> message.getMessageId().getSequence() > lastSequence)
                        .collect(Collectors.toList());


                VerifyIterator<MessageWrapper> iterator = new VerifyIterator<>(subCollection);
                if (LOGGER.isDebugEnabled() ) {
                    if (subCollection.isEmpty()) {
                        LOGGER.debug("Interator created with empty cache, available '{}'", messages.size());
                    } else {
                        LOGGER.debug("Interator created for messages from '{}' to '{}'... available '{}' used '{}' messages",
                                shortDebugString(subCollection.get(0).getMessageId()), shortDebugString(subCollection.get(subCollection.size() - 1).getMessageId()),
                                messages.size(), subCollection.size());
                    }
                }
                iterators.add(iterator);
                return iterator;
            }
        }

        public VerifyIterator<MessageWrapper> createIterator() {
            synchronized (messages) {
                List<MessageWrapper> copyOfMessages = List.copyOf(messages);
                VerifyIterator<MessageWrapper> iterator = new VerifyIterator<>(copyOfMessages);
                if (LOGGER.isDebugEnabled() ) {
                    LOGGER.debug("Interator created with full cache, available '{}'", copyOfMessages.size());
                }
                iterators.add(iterator);
                return iterator;

            }
        }

        //TODO: Avoid this approach
        public void removeIterator(VerifyIterator<MessageWrapper> iterator) {
            iterators.remove(iterator);
        }
    }
}
