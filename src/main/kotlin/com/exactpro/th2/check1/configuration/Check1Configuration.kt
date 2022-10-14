/*
 * Copyright 2020-2022 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.configuration

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer
import java.time.Duration
import java.time.temporal.ChronoUnit

class Check1Configuration(
    @field:JsonProperty(value = "message-cache-size", defaultValue = "1000")
    val messageCacheSize: Int = 1000,

    @field:JsonProperty(value = "cleanup-older-than", defaultValue = "60")
    val cleanupOlderThan: Long = 60L,

    @field:JsonProperty(value = "max-event-batch-content-size", defaultValue = "1048576")
    val maxEventBatchContentSize: Int = 1048576,

    @field:JsonProperty(value = "cleanup-time-unit", defaultValue = "SECONDS")
    val cleanupTimeUnit: ChronoUnit = ChronoUnit.SECONDS,

    @field:JsonProperty(value = "rule-execution-timeout", defaultValue = "5000")
    val ruleExecutionTimeout: Long = 5000L,

    @field:JsonProperty("auto-silence-check-after-sequence-rule")
    @field:JsonPropertyDescription("The default behavior in case the SequenceCheckRule does not have silenceCheck parameter specified")
    val isAutoSilenceCheckAfterSequenceRule: Boolean = false,

    @field:JsonProperty(value = "decimal-precision", defaultValue = "0.00001")
    val decimalPrecision: Double = 0.00001,

    @field:JsonProperty(value = "time-precision", defaultValue = "PT0.000000001S")
    @field:JsonDeserialize(using = DurationDeserializer::class)
    @field:JsonPropertyDescription("The default time precision value uses java duration format")
    val timePrecision: Duration = Duration.parse("PT0.000000001S"),

    @field:JsonProperty(value = "check-null-value-as-empty")
    val isCheckNullValueAsEmpty: Boolean = false,

    @field:JsonProperty(value = "default-check-simple-collections-order", defaultValue = "true")
    val isDefaultCheckSimpleCollectionsOrder: Boolean = true,

    @field:JsonProperty(value = "enable-checkpoint-events-publication", defaultValue = "true")
    @field:JsonPropertyDescription("Enables events publication if parent event ID is specified in checkpoint request")
    val enableCheckpointEventsPublication: Boolean = true,
)