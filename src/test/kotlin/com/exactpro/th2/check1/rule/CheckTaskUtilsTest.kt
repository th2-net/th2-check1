/*
 * Copyright 2021-2024 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.rule

import com.exactpro.th2.check1.rule.AbstractCheckTaskTest.Companion.BOOK_NAME
import com.exactpro.th2.check1.rule.AbstractCheckTaskTest.Companion.SCOPE
import com.exactpro.th2.common.event.Event
import com.exactpro.th2.common.event.EventUtils
import com.exactpro.th2.common.event.EventUtils.createMessageBean
import com.exactpro.th2.common.grpc.EventBatch
import com.exactpro.th2.common.grpc.EventID
import com.exactpro.th2.common.grpc.EventStatus.FAILED
import com.exactpro.th2.common.grpc.EventStatus.SUCCESS
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.time.Instant
import kotlin.test.assertEquals

class CheckTaskUtilsTest {
    private val parentEventId: EventID = EventUtils.toEventID(Instant.now(), BOOK_NAME, SCOPE, "parentEventId")
    private val data = createMessageBean("0123456789".repeat(20))
    private val dataSize = OBJECT_MAPPER.writeValueAsBytes(listOf(data)).size
    private val bigData = createMessageBean("0123456789".repeat(30))
    private val bigDataSize = OBJECT_MAPPER.writeValueAsBytes(listOf(bigData)).size

        @Test
    fun `negative or zero max size`() {
        val rootEvent = Event.start()
        assertAll(
            { assertThrows(IllegalArgumentException::class.java) { rootEvent.disperseToBatches(-1, parentEventId) } },
            { assertThrows(IllegalArgumentException::class.java) { rootEvent.disperseToBatches(0, parentEventId) } }
        )
    }

    @Test
    fun `too low max size`() {
        val rootEvent = Event.start()
            .bodyData(data)

        assertAll(
            { assertThrows(IllegalStateException::class.java) { rootEvent.disperseToBatches(1, parentEventId) } }
        )
    }

    @Test
    fun `every event to distinct batch`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                    .addSubEventWithSamePeriod()
                        .bodyData(data)
            }

        val batches = rootEvent.disperseToBatches(dataSize, parentEventId)
        assertEquals(3, batches.size)
        checkEventStatus(batches, 3, 0)
    }

    @Test
    fun `problem events`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                    .addSubEventWithSamePeriod()
                        .bodyData(bigData)
            }

        val batches = rootEvent.disperseToBatches(dataSize, parentEventId)
        assertEquals(3, batches.size)
        checkEventStatus(batches, 2, 1)
    }

    @Test
    fun `several events at the end of hierarchy`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                addSubEventWithSamePeriod()
                    .bodyData(bigData)
                addSubEventWithSamePeriod()
                    .bodyData(data)
                addSubEventWithSamePeriod()
                    .bodyData(data)
            }

        assertAll(
            {
                val batches = rootEvent.disperseToBatches(dataSize, parentEventId)
                assertEquals(5, batches.size)
                checkEventStatus(batches, 4, 1)
            }, {
                val batches = rootEvent.disperseToBatches(dataSize * 2, parentEventId)
                assertEquals(4, batches.size)
                checkEventStatus(batches, 5, 0)
            }, {
                val batches = rootEvent.disperseToBatches(dataSize * 3, parentEventId)
                assertEquals(3, batches.size)
                checkEventStatus(batches, 5, 0)
            }
        )
    }

    @Test
    fun `batch structure`() {
        val rootEvent = Event.start()
            .bodyData(data)
        val subEvent1 = rootEvent.addSubEventWithSamePeriod()
            .bodyData(data)
        val subEvent2 = rootEvent.addSubEventWithSamePeriod()
            .bodyData(data)

        val batches = rootEvent.disperseToBatches(1024 * 1024, parentEventId)
        assertEquals(2, batches.size)
        checkEventStatus(batches, 3, 0)

        assertFalse(batches[0].hasParentEventId())
        assertEquals(parentEventId, batches[0].eventsList[0].parentId)
        assertEquals(rootEvent.id, batches[0].eventsList[0].id.id)

        assertEquals(rootEvent.id, batches[1].parentEventId.id)
        assertEquals(rootEvent.id, batches[1].eventsList[0].parentId.id)
        assertEquals(subEvent1.id, batches[1].eventsList[0].id.id)
        assertEquals(rootEvent.id, batches[1].eventsList[1].parentId.id)
        assertEquals(subEvent2.id, batches[1].eventsList[1].id.id)
    }

    @Test
    fun `event with children is after the event without children`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data)
                addSubEventWithSamePeriod()
                    .bodyData(data).apply {
                        addSubEventWithSamePeriod()
                            .bodyData(data)
                    }
            }

        val batches = rootEvent.disperseToBatches(dataSize, parentEventId)
        assertEquals(4, batches.size)
        checkEventStatus(batches, 4, 0)
    }

    @Test
    fun `event with children is before the event without children`() {
        val rootEvent = Event.start()
            .bodyData(data).apply {
                addSubEventWithSamePeriod()
                    .bodyData(data).apply {
                        addSubEventWithSamePeriod()
                            .bodyData(data)
                    }
                addSubEventWithSamePeriod()
                    .bodyData(data)
            }

        val batches = rootEvent.disperseToBatches(dataSize, parentEventId)
        assertEquals(4, batches.size)
        checkEventStatus(batches, 4, 0)
    }

    private fun checkEventStatus(batches: List<EventBatch>, successNumber: Int, filedNumber: Int) {
        val events = batches.flatMap(EventBatch::getEventsList)
        assertAll(
            { assertEquals(filedNumber + successNumber, events.size, "number") },
            { assertEquals(filedNumber, events.filter { it.status == FAILED }.size, "success") },
            { assertEquals(successNumber, events.filter { it.status == SUCCESS }.size, "failed") }
        )
    }
}