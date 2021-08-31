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

package com.exactpro.th2.check1.metrics

import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.AbstractMetric
import io.prometheus.client.Gauge
import io.reactivex.Observable
import io.reactivex.disposables.Disposable
import mu.KotlinLogging
import org.openjdk.jol.info.GraphLayout
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

class MemoryVolumeMetric(configuration: Check1Configuration) : AbstractMetric(), AutoCloseable {

    private val lastReceivedMessageMetricTask: Disposable = Observable.interval(configuration.messageSizeCollectionTimeout, TimeUnit.MINUTES)
            .subscribe {
                lastReceivedMessagesMetric.set(lastReceivedMessages.average())
                lastReceivedMessages.clear()
            }
    private val lastReceivedMessages: Queue<Long> = ConcurrentLinkedQueue<Long>()
    private val actualBufferMessages: Queue<Long> = LinkedBlockingQueue<Long>()
    private val maxBufferSize = configuration.messageCacheSize


    fun processMessage(message: Message) {
        if (actualBufferMessages.size >= maxBufferSize) {
            val messageSize = actualBufferMessages.poll()
            actualBufferCount.dec()
            actualBufferSize.dec(messageSize.toDouble())
        }

        val calculatedMessageSize = calculateMessageSize(message)
        actualBufferCount.inc()
        actualBufferSize.inc(calculatedMessageSize.toDouble())
        actualBufferMessages.add(calculatedMessageSize)

        lastReceivedMessages.add(calculatedMessageSize)
    }

    override fun onValueChange(value: Boolean) {
        lastReceivedMessagesMetric.set(if (value) 1.0 else 0.0)
        actualBufferCount.set(if (value) 1.0 else 0.0)
        actualBufferSize.set(if (value) 1.0 else 0.0)
    }

    override fun close() {
        lastReceivedMessageMetricTask.dispose()
    }

    private fun calculateMessageSize(message: Message): Long {
        val parsedInstance = GraphLayout.parseInstance(message)
        if (logger.isTraceEnabled) {
            logger.trace("Foot print for message with id: '{}'\n{}", message.metadata.id.toJson(), parsedInstance.toFootprint())
        }
        return parsedInstance.totalSize()
    }

    companion object {
        private val logger = KotlinLogging.logger {}
        private val lastReceivedMessagesMetric: Gauge = Gauge
                .build("th2_average_size_of_parsed_messages_by_timeout", "Average size of parsed messages received by timeout")
                .register()
        private val actualBufferCount: Gauge = Gauge
                .build("th2_actual_cache_number", "The actual number of caches")
                .register()
        private val actualBufferSize: Gauge = Gauge
                .build("th2_actual_cache_size", "The actual size of the cache")
                .register()
    }
}