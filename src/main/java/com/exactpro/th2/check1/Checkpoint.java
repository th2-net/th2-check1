/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1;

import com.exactpro.th2.common.grpc.Checkpoint.DirectionCheckpoint;
import com.exactpro.th2.common.grpc.Direction;
import com.google.protobuf.Timestamp;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.driver.core.utils.UUIDs.timeBased;

public class Checkpoint {

    private final String id;
    private final Map<SessionKey, CheckpointData> sessionKeyToCheckpointData;

    private Checkpoint(String id, Map<SessionKey,  CheckpointData> sessionKeyToCheckpointData) {
        this.id = id;
        this.sessionKeyToCheckpointData = Map.copyOf(sessionKeyToCheckpointData);
    }

    public Checkpoint(Map<SessionKey, CheckpointData> sessionKeyToCheckpointData) {
        this(timeBased().toString(), sessionKeyToCheckpointData);
    }

    public String getId() {
        return id;
    }

    public boolean contains(SessionKey sessionKey) {
        return sessionKeyToCheckpointData.containsKey(sessionKey);
    }

    public CheckpointData getCheckpointData(SessionKey sessionKey) {
        return sessionKeyToCheckpointData.get(sessionKey);
    }

    public com.exactpro.th2.common.grpc.Checkpoint convert() {
        Map<String, DirectionCheckpoint.Builder> intermediateMap = new HashMap<>();

        for (Map.Entry<SessionKey, CheckpointData> entry : sessionKeyToCheckpointData.entrySet()) {
            SessionKey sessionKey = entry.getKey();
            intermediateMap.computeIfAbsent(sessionKey.getSessionAlias(), alias -> DirectionCheckpoint.newBuilder())
                    .putDirectionToCheckpointData(sessionKey.getDirection().getNumber(), CheckpointData.convert(entry.getValue()));
        }

        var checkpointBuilder = com.exactpro.th2.common.grpc.Checkpoint.newBuilder()
                .setId(id);
        for (Map.Entry<String, DirectionCheckpoint.Builder> entry : intermediateMap.entrySet()) {
            checkpointBuilder.putSessionAliasToDirectionCheckpoint(entry.getKey(),
                    entry.getValue().build());
        }
        return checkpointBuilder.build();
    }

    public Map<SessionKey, CheckpointData> asMap() {
        return Collections.unmodifiableMap(sessionKeyToCheckpointData);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("sessionKeyToSequence", sessionKeyToCheckpointData)
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Checkpoint other = (Checkpoint)obj;

        return new EqualsBuilder()
                .append(id, other.id)
                .append(sessionKeyToCheckpointData, other.sessionKeyToCheckpointData)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(sessionKeyToCheckpointData)
                .toHashCode();
    }

    public static Checkpoint convert(com.exactpro.th2.common.grpc.Checkpoint protoCheckpoint) {
        Map<SessionKey, CheckpointData> sessionKeyToSequence = new HashMap<>();
        for (Map.Entry<String, DirectionCheckpoint> sessionAliasDirectionCheckpointEntry : protoCheckpoint.getSessionAliasToDirectionCheckpointMap().entrySet()) {
            String sessionAlias = sessionAliasDirectionCheckpointEntry.getKey();
            DirectionCheckpoint directionCheckpoint = sessionAliasDirectionCheckpointEntry.getValue();
            if (directionCheckpoint.getDirectionToCheckpointDataCount() != 0) {
                for (Map.Entry<Integer, com.exactpro.th2.common.grpc.Checkpoint.CheckpointData> directionSequenceEntry : directionCheckpoint.getDirectionToCheckpointDataMap().entrySet()) {
                    SessionKey sessionKey = new SessionKey(sessionAlias, Direction.forNumber(directionSequenceEntry.getKey()));
                    com.exactpro.th2.common.grpc.Checkpoint.CheckpointData checkpointData = directionSequenceEntry.getValue();
                    sessionKeyToSequence.put(sessionKey, new CheckpointData(checkpointData.getSequence(), checkpointData.getTimestamp()));
                }
            } else {
                for (Map.Entry<Integer, Long> directionSequenceEntry : directionCheckpoint.getDirectionToSequenceMap().entrySet()) {
                    SessionKey sessionKey = new SessionKey(sessionAlias, Direction.forNumber(directionSequenceEntry.getKey()));
                    sessionKeyToSequence.put(sessionKey, new CheckpointData(directionSequenceEntry.getValue(), Timestamp.getDefaultInstance()));
                }
            } 
        }
        return new Checkpoint(protoCheckpoint.getId(), sessionKeyToSequence);
    }
}
