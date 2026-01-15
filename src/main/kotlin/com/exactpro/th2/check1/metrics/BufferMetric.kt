/*
 * Copyright 2021-2026 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.metrics

import com.exactpro.th2.check1.SessionKey
import com.exactpro.th2.check1.configuration.Check1Configuration
import com.exactpro.th2.common.metrics.BOOK_NAME_LABEL
import com.exactpro.th2.common.metrics.DIRECTION_LABEL
import com.exactpro.th2.common.metrics.SESSION_ALIAS_LABEL
import io.prometheus.client.Counter
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.min

object BufferMetric {

    private val actualBufferCountMetric: Counter = Counter
            .build("th2_check1_actual_cache_number", "The actual number of messages in caches")
            .labelNames(BOOK_NAME_LABEL, SESSION_ALIAS_LABEL, DIRECTION_LABEL)
            .register()

    private val bufferMessagesSizeBySessionKey: MutableMap<SessionKey, Int> = ConcurrentHashMap()
    private var maxBufferSize: Int = -1

    fun configure(configuration: Check1Configuration) {
        this.maxBufferSize = configuration.messageCacheSize
    }

    fun processMessage(sessionKey: SessionKey) {
        bufferMessagesSizeBySessionKey.compute(sessionKey) { _, old ->
            min(maxBufferSize, (old ?: 0) + 1).also {
                if (it != old) {
                    actualBufferCountMetric
                        .labels(sessionKey.bookName, sessionKey.sessionAlias, sessionKey.direction.name)
                        .inc()
                }
            }
        }
    }
}