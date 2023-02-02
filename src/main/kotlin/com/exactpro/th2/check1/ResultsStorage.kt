/*
 * Copyright 2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1

import com.exactpro.th2.common.grpc.EventStatus
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicLong

class ResultsStorage(private val cleanupTimeoutSec: Long) : AutoCloseable {
    private val idCounter = AtomicLong()
    private val results: MutableMap<Long, CompletableFuture<EventStatus>> = HashMap()
    private val storageCleanerExecutor = Executors.newSingleThreadScheduledExecutor()

    fun createId(): Long {
        val id = idCounter.incrementAndGet()
        results[id] = CompletableFuture<EventStatus>()
        return id
    }

    fun putResult(id: Long, status: EventStatus) {
        val future = results[id] ?: throw IllegalArgumentException("Non valid id")
        future.complete(status)
        storageCleanerExecutor.schedule({ results.remove(id) }, cleanupTimeoutSec, TimeUnit.SECONDS)
    }

    @Throws(TimeoutException::class)
    fun getResult(id: Long, timeout: Long): EventStatus? = results[id]?.get(timeout, TimeUnit.MILLISECONDS)

    fun removeResult(id: Long) = results.remove(id)

    override fun close() {
        storageCleanerExecutor.shutdownNow()
        storageCleanerExecutor.awaitTermination(5, TimeUnit.SECONDS)
    }
}