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
package com.exactpro.th2.check1;

import com.exactpro.th2.check1.entities.Checkpoint;
import com.exactpro.th2.check1.entities.CheckpointData;
import com.exactpro.th2.check1.utils.ProtoMessageUtilsKt;
import com.exactpro.th2.common.grpc.Direction;
import com.google.protobuf.Timestamp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class CheckpointTest {

    @Test
    public void testConvertation() {
        var origCheckpoint = new Checkpoint(Map.of(
                new SessionKey("A", Direction.FIRST), generateCheckpointData(1L),
                new SessionKey("A", Direction.SECOND), generateCheckpointData(2L),
                new SessionKey("B", Direction.FIRST), generateCheckpointData(3L),
                new SessionKey("B", Direction.SECOND), generateCheckpointData(4L)
        ));

        var protoCheckpoint = ProtoMessageUtilsKt.convert(origCheckpoint);

        var parsedCheckpoint = ProtoMessageUtilsKt.convert(protoCheckpoint);

        Assertions.assertEquals(origCheckpoint, parsedCheckpoint);
    }

    private Checkpoint generateCheckpoint() {
        return new Checkpoint(Map.of(
                new SessionKey("A", Direction.FIRST), generateCheckpointData(1L),
                new SessionKey("A", Direction.FIRST), generateCheckpointData(2L),
                new SessionKey("B", Direction.SECOND), generateCheckpointData(3L),
                new SessionKey("B", Direction.SECOND), generateCheckpointData(4L)
        ));
    }

    private CheckpointData generateCheckpointData(Long sequence, Timestamp timestamp) {
        return new CheckpointData(sequence, timestamp);
    }
    
    private CheckpointData generateCheckpointData(Long sequence) {
        return generateCheckpointData(sequence, Timestamp.getDefaultInstance());
    }
}