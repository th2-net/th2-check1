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

package com.exactpro.th2.check1.rule.check

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.StreamContainer
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest
import com.exactpro.th2.check1.util.toSimpleFilter
import com.exactpro.th2.common.grpc.Direction
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus
import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.MessageMetadata
import com.exactpro.th2.common.grpc.MetadataFilter
import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.message
import io.reactivex.Observable
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Instant

internal class TestCheckRuleTask : AbstractCheckTaskTest() {
    private fun checkTask(
        messageFilter: RootMessageFilter,
        parentEventID: EventID,
        messageStream: Observable<StreamContainer>
    ) = CheckRuleTask(
        "test",
        Instant.now(),
        SessionKey("test", Direction.FIRST),
        1000,
        1024 * 1024,
        messageFilter,
        parentEventID,
        messageStream,
        clientStub
    )

    @Test
    internal fun eventContentExceedsMaxSize() {
        val streams = createStreams("test", Direction.FIRST, listOf(
            message("TestMsg", Direction.FIRST, "test")
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType("TestMsg")
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL)))
            .build()
        val task = CheckRuleTask(
            "test",
            Instant.now(),
            SessionKey("test", Direction.FIRST),
            1000,
            10,
            filter,
            eventID,
            streams,
            clientStub)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L)

        assertNotNull(eventBatch)
        assertTrue({
            eventBatch.eventsList.none { it.status == EventStatus.FAILED }
        }) {
            "Some events are failed $eventBatch"
        }

//        verify(clientStub, timeout(1000).times(0)).send(any(), any())
    }

    @Test
    internal fun findsMessageByMetadata() {
        val streams = createStreams("test", Direction.FIRST, listOf(
            message("TestMsg", Direction.FIRST, "test")
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType("TestMsg")
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L)

        assertNotNull(eventBatch)
        assertTrue({
            eventBatch.eventsList.none { it.status == EventStatus.FAILED }
        }) {
            "Some events are failed $eventBatch"
        }
    }

    @Test
    internal fun ignoresMessageIfMetadataDoesNotMatchByKeys() {
        val streams = createStreams("test", Direction.FIRST, listOf(
            message("TestMsg", Direction.FIRST, "test")
                .mergeMetadata(MessageMetadata.newBuilder()
                    .putProperties("keyProp", "42")
                    .putProperties("notKeyProp", "2")
                    .build())
                .build()
        ))

        val eventID = EventID.newBuilder().setId("root").build()
        val filter = RootMessageFilter.newBuilder()
            .setMessageType("TestMsg")
            .setMetadataFilter(MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "43".toSimpleFilter(FilterOperation.EQUAL, key = true)))
            .build()
        val task = checkTask(filter, eventID, streams)
        task.begin()

        val eventBatch = awaitEventBatchRequest(1000L)

        assertNotNull(eventBatch)
        val checkFailedEvent = eventBatch.eventsList.firstOrNull {
            it.status == EventStatus.FAILED && it.type == "Check failed"
        }
        assertNotNull(checkFailedEvent) {
            "No failed event $eventBatch"
        }
    }
}