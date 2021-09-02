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

import com.exactpro.th2.check1.rule.AbstractCheckTask
import com.exactpro.th2.check1.utils.dec
import com.exactpro.th2.check1.utils.inc
import io.prometheus.client.Gauge
import mu.KotlinLogging
import org.openjdk.jol.info.GraphLayout
import java.util.concurrent.ConcurrentHashMap

object RuleMetric {
    private val logger = KotlinLogging.logger {}

    private val ACTIVE_TASK_COUNTER = Gauge.build("th2_check1_active_tasks_count", "Current active tasks count in execution").register()
    private val ACTIVE_TASK_MEMORY_SIZE = Gauge.build("th2_check1_active_tasks_memory_usage_size", "Memory usage of currently executing tasks").register()

    private val activeRules: MutableMap<AbstractCheckTask, Long> = ConcurrentHashMap()

    fun incrementActiveRule(rule: AbstractCheckTask) {
        ACTIVE_TASK_COUNTER.inc()
        val parsedInstance = GraphLayout.parseInstance(rule)
        if (logger.isTraceEnabled) {
            logger.trace("Foot print for rule: {} ({})\n{}", rule.description, rule.hashCode(), parsedInstance.toFootprint())
        }
        parsedInstance.totalSize().also {
            activeRules[rule] = it
            ACTIVE_TASK_MEMORY_SIZE.inc(it)
        }
    }

    fun decrementActiveRule(rule: AbstractCheckTask) {
        ACTIVE_TASK_COUNTER.dec()
        activeRules.remove(rule)?.also { ACTIVE_TASK_MEMORY_SIZE.dec(it) }
    }
}