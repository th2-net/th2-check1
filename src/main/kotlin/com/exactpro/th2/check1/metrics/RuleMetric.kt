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

import io.prometheus.client.Gauge

object RuleMetric {
    private val ACTIVE_TASK_COUNTER = Gauge.build("th2_check1_active_tasks_count", "Current active tasks count in execution").register()

    fun incrementActiveRule() {
        ACTIVE_TASK_COUNTER.inc()
    }

    fun decrementActiveRule() {
        ACTIVE_TASK_COUNTER.dec()
    }
}