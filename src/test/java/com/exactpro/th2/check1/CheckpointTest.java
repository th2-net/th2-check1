/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.exactpro.th2.check1.entities.Checkpoint;
import com.exactpro.th2.check1.entities.CheckpointData;
import com.exactpro.th2.check1.utils.ProtoMessageUtilsKt;
import com.exactpro.th2.common.grpc.Direction;
import com.google.protobuf.Timestamp;

import static com.exactpro.th2.check1.util.UtilsKt.BOOK_NAME;
import static java.util.Map.entry;

public class CheckpointTest {
    private static final String BOOK_NAME_1 = BOOK_NAME + "_1";
    private static final String BOOK_NAME_2 = BOOK_NAME + "_2";

    private static final String SESSION_ALIAS_A = "A";
    private static final String SESSION_ALIAS_B = "B";

    @Test
    public void testConvertation() {
        var origCheckpoint = new Checkpoint(Map.ofEntries(
                entry(new SessionKey(BOOK_NAME_1, SESSION_ALIAS_A, Direction.FIRST), generateCheckpointData(1L)),
                entry(new SessionKey(BOOK_NAME_1, SESSION_ALIAS_A, Direction.SECOND), generateCheckpointData(2L)),
                entry(new SessionKey(BOOK_NAME_1, SESSION_ALIAS_B, Direction.FIRST), generateCheckpointData(3L)),
                entry(new SessionKey(BOOK_NAME_1, SESSION_ALIAS_B, Direction.SECOND), generateCheckpointData(4L)),
                entry(new SessionKey(BOOK_NAME_2, SESSION_ALIAS_A, Direction.FIRST), generateCheckpointData(5L)),
                entry(new SessionKey(BOOK_NAME_2, SESSION_ALIAS_A, Direction.SECOND), generateCheckpointData(6L)),
                entry(new SessionKey(BOOK_NAME_2, SESSION_ALIAS_B, Direction.FIRST), generateCheckpointData(7L)),
                entry(new SessionKey(BOOK_NAME_2, SESSION_ALIAS_B, Direction.SECOND), generateCheckpointData(8L))
        ));
        Assertions.assertEquals(origCheckpoint, ProtoMessageUtilsKt.convert(ProtoMessageUtilsKt.convert(origCheckpoint)));
    }

    private CheckpointData generateCheckpointData(Long sequence) {
        return generateCheckpointData(sequence, Timestamp.getDefaultInstance());
    }

    private CheckpointData generateCheckpointData(Long sequence, Timestamp timestamp) {
        return new CheckpointData(sequence, timestamp);
    }
}