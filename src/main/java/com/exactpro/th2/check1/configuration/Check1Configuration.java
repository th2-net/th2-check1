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

import java.time.temporal.ChronoUnit;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Check1Configuration {

    @JsonProperty(value="message-cache-size", defaultValue = "1000")
    private int messageCacheSize = 1000;

    @JsonProperty(value="cleanup-older-than", defaultValue = "60")
    private long cleanupOlderThan = 60L;

    @JsonProperty(value="max-event-batch-content-size", defaultValue = "1048576")
    private int maxEventBatchContentSize = 1_048_576;

    @JsonProperty(value="cleanup-time-unit", defaultValue = "SECONDS")
    private ChronoUnit cleanupTimeUnit = ChronoUnit.SECONDS;

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
}
