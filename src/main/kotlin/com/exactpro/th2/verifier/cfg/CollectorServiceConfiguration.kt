/*
 * Copyright 2020-2020 Exactpro (Exactpro Systems Limited)
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
package com.exactpro.th2.verifier.cfg

import com.exactpro.th2.configuration.MicroserviceConfiguration
import java.lang.System.getenv
import java.time.temporal.ChronoUnit

/**
 * The default delta between task end time and current time.
 * If the real delta is greater than this value the task will be removed from the list of the tasks.
 *
 * Note: corresponds to use with the default value for [CollectorServiceConfiguration#cleanupTimeUnit] that is [ChronoUnit#SECONDS]
 */
const val DEFAULT_DELTA = 60L

/**
 * Default size of the message buffer for each session alias
 */
const val DEFAULT_MESSAGE_BUFFER = 1000

const val VERIFIER_CLEANUP_OLDER_THAN = "TH2_VERIFIER_CLEANUP_OLDER_THAN"
const val VERIFIER_CLEANUP_TIME_UNIT = "TH2_VERIFIER_CLEANUP_TIME_UNIT"
const val VERIFIER_MESSAGE_BUFFER_LIMIT = "TH2_VERIFIER_MESSAGE_BUFFER_LIMIT"

class CollectorServiceConfiguration(
    val microserviceConfiguration: MicroserviceConfiguration
) {
    val cleanupOlderThan: Long = getenv(VERIFIER_CLEANUP_OLDER_THAN)?.toLong() ?: DEFAULT_DELTA
    val cleanupTimeUnit: ChronoUnit =
        getenv(VERIFIER_CLEANUP_TIME_UNIT)?.run { ChronoUnit.valueOf(toUpperCase()) } ?: ChronoUnit.SECONDS

    val messageBufferLimit: Int = getenv(VERIFIER_MESSAGE_BUFFER_LIMIT)?.toInt() ?: DEFAULT_MESSAGE_BUFFER
}