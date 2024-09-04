/*
 * Copyright 2021-2023 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.entities

import java.time.Duration

data class RuleConfiguration(
    val taskTimeout: TaskTimeout,
    val description: String?,
    val timePrecision: Duration,
    val decimalPrecision: Double,
    val maxEventBatchContentSize: Int,
    val isCheckNullValueAsEmpty: Boolean,
    val defaultCheckSimpleCollectionsOrder: Boolean
) {
    init {
        require(!timePrecision.isNegative) { "Time precision cannot be negative" }
        require(decimalPrecision >= 0) { "Decimal precision cannot be negative" }
        require(maxEventBatchContentSize > 0) {
            "'maxEventBatchContentSize' should be greater than zero, actual: $maxEventBatchContentSize"
        }
        with(taskTimeout) {
            require(timeout > 0) {
                "'timeout' should be set or be greater than zero, actual: $timeout"
            }
        }
    }
}