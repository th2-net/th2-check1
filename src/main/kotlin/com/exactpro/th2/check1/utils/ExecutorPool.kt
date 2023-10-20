/*
 * Copyright 2020-2023 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.check1.utils

import com.exactpro.th2.common.utils.shutdownGracefully
import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

class ExecutorPool(
    threadName: String,
    nExecutors: Int = 1
): AutoCloseable {
    init {
        require(threadName.isNotBlank()) {
            "'threadName' can't be blank, actual '$threadName'"
        }
        require(nExecutors > 0) {
            "'nExecutors' must be positive, actual '$nExecutors'"
        }
    }

    private val executors = (0 until nExecutors).map { index ->
        ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            LinkedBlockingQueue(),
            ThreadFactoryBuilder().setNameFormat("$threadName-e$index-t%d").build()
        )
    }

    private val firstExecutor: ExecutorService = executors.first()

    private val executorSupplier: () -> ExecutorService = when (nExecutors) {
        1 -> { { firstExecutor } }
        else -> { { executors.minBy { it.taskCount } } }
    }
    fun get(): ExecutorService = executorSupplier.invoke()

    override fun close() {
        executors.forEach {
            it.shutdownGracefully(10, TimeUnit.SECONDS)
        }
    }
}
