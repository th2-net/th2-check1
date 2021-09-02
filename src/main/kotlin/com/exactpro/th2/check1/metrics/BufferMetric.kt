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

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.check1.utils.dec
import com.exactpro.th2.check1.utils.inc
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.message.toJson
import com.exactpro.th2.common.metrics.DEFAULT_DIRECTION_LABEL_NAME
import com.exactpro.th2.common.metrics.DEFAULT_SESSION_ALIAS_LABEL_NAME
import io.prometheus.client.Gauge
import io.reactivex.Observable
import io.reactivex.disposables.Disposable
import mu.KotlinLogging
import org.openjdk.jol.info.GraphLayout
import java.util.Queue
import java.util.LinkedList
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

object BufferMetric : AutoCloseable {
    private val logger = KotlinLogging.logger {}

    private val averageReceivedMessagesSizeMetric: Gauge = Gauge
            .build("th2_check1_average_size_of_parsed_messages_by_timeout", "Average size of parsed messages received by timeout")
            .register()
    private val actualBufferCountMetric: Gauge = Gauge
            .build("th2_check1_actual_cache_number", "The actual number of caches")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .register()
    private val actualBufferSizeMetric: Gauge = Gauge
            .build("th2_check1_actual_cache_size", "The actual size of the cache")
            .labelNames(DEFAULT_SESSION_ALIAS_LABEL_NAME, DEFAULT_DIRECTION_LABEL_NAME)
            .register()
    private val requiredCacheMemorySizeMetric: Gauge = Gauge
            .build("th2_check1_required_cache_memory_size", "Required cache memory size without active cache size")
            .register()

    private lateinit var lastReceivedMessageMetricTask: Disposable
    private val lastReceivedMessages: Queue<Long> = ConcurrentLinkedQueue<Long>()
    private val bufferMessagesSizeBySessionKey: MutableMap<SessionKey, LinkedList<Long>> = ConcurrentHashMap()
    private var maxBufferSize: Int = -1

    fun configure(configuration: Check1Configuration) {
        this.maxBufferSize = configuration.messageCacheSize
        this.lastReceivedMessageMetricTask = createObservableInterval(configuration.messageSizeCollectionTimeout)
    }

    fun processMessage(sessionKey: SessionKey, message: Message) {
        val calculatedMessageSize = calculateMessageSize(message)
        val labels = arrayOf(sessionKey.sessionAlias, sessionKey.direction.name)

        bufferMessagesSizeBySessionKey.compute(sessionKey) { _, bufferedMessageSize ->
            if (bufferedMessageSize == null) {
                incrementStats(calculatedMessageSize, labels)
                return@compute LinkedList<Long>().apply { add(calculatedMessageSize) }
            }

            if (bufferedMessageSize.size >= maxBufferSize) {
                decrementStats(bufferedMessageSize.poll(), labels)
            }

            bufferedMessageSize.add(calculatedMessageSize)
            incrementStats(calculatedMessageSize, labels)

            return@compute bufferedMessageSize
        }

        lastReceivedMessages.add(calculatedMessageSize)
        requiredCacheMemorySizeMetric.set(calculateRequiredMemory())
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

    private fun createObservableInterval(messageSizeCollectionTimeout: Long) =
            Observable.interval(messageSizeCollectionTimeout, TimeUnit.MINUTES)
                    .subscribe {
                        averageReceivedMessagesSizeMetric.set(lastReceivedMessages.average())
                        lastReceivedMessages.clear()
                    }

    private fun calculateRequiredMemory(): Double {
        return bufferMessagesSizeBySessionKey.values.sumByDouble {
            val averageMessageSize = it.average()
            val leftBufferCapacity = maxBufferSize - it.size
            return averageMessageSize * leftBufferCapacity
        }
    }

    private fun incrementStats(messageSize: Long, labels: Array<String>) {
        actualBufferSizeMetric.labels(*labels).inc(messageSize)
        actualBufferCountMetric.labels(*labels).inc()
    }

    private fun decrementStats(messageSize: Long, labels: Array<String>) {
        actualBufferSizeMetric.labels(*labels).dec(messageSize)
        actualBufferCountMetric.labels(*labels).dec()
    }
}