/*
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.datatype.jsr310.deser.DurationDeserializer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class Check1Configuration {

    @JsonProperty(value="message-cache-size", defaultValue = "1000")
    private int messageCacheSize = 1000;

    @JsonProperty(value="cleanup-older-than", defaultValue = "60")
    private long cleanupOlderThan = 60L;

    @JsonProperty(value="max-event-batch-content-size", defaultValue = "1048576")
    private int maxEventBatchContentSize = 1_048_576;

    @JsonProperty(value="cleanup-time-unit", defaultValue = "SECONDS")
    private ChronoUnit cleanupTimeUnit = ChronoUnit.SECONDS;

    @JsonProperty(value="rule-execution-timeout", defaultValue = "5000")
    private long ruleExecutionTimeout = 5000L;

    @JsonProperty(value="decimal-precision", defaultValue = "0.00001")
    private double decimalPrecision = 0.00001;

    @JsonProperty(value="time-precision", defaultValue = "PT0.000000001S")
    @JsonDeserialize(using = DurationDeserializer.class)
    @JsonPropertyDescription("The default time precision value uses java duration format")
    private Duration timePrecision = Duration.parse("PT0.000000001S");

    public int getMessageCacheSize() {
        return messageCacheSize;
    }

    public long getCleanupOlderThan() {
        return cleanupOlderThan;
    }

    public int getMaxEventBatchContentSize() {
        return maxEventBatchContentSize;
    }

    public ChronoUnit getCleanupTimeUnit() {
        return cleanupTimeUnit;
    }

    public long getRuleExecutionTimeout() {
        return ruleExecutionTimeout;
    }

    public double getDecimalPrecision() {
        return decimalPrecision;
    }

    public Duration getTimePrecision() {
        return timePrecision;
    }
}
