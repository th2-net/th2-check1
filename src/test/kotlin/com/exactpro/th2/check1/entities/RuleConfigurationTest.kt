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

package com.exactpro.th2.check1.entities

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Duration
import kotlin.test.assertEquals

class RuleConfigurationTest {

    @Test
    fun `check that time precision is negative`() {
        val exception = assertThrows<IllegalArgumentException> {
            RuleConfiguration(
                    TaskTimeout(0, 0), null, Duration.ofSeconds(-1),
                    0.005,
                    1,
                    true
            )
        }
        assertEquals(exception.message, "Time precision cannot be negative")
    }

    @Test
    fun `check that decimal precision is negative`() {
        val exception = assertThrows<IllegalArgumentException> {
            RuleConfiguration(
                    TaskTimeout(0, 0), null, Duration.ofSeconds(1),
                    -0.005,
                    1,
                    true
            )
        }
        assertEquals(exception.message, "Decimal precision cannot be negative")
    }

    @Test
    fun `check that max event batch content size is negative`() {
        val maxEventBatchContentSize = -1

        val exception = assertThrows<IllegalArgumentException> {
            RuleConfiguration(
                    TaskTimeout(0, 0),
                    null,
                    Duration.ofSeconds(1),
                    0.005,
                    maxEventBatchContentSize,
                    true
            )
        }
        assertEquals(exception.message, "'maxEventBatchContentSize' should be greater than zero, actual: $maxEventBatchContentSize")
    }

    @Test
    fun `check that task timeout is negative`() {
        val timeout = -1L

        val exception = assertThrows<IllegalArgumentException> {
            RuleConfiguration(
                    TaskTimeout(timeout, 0),
                    null,
                    Duration.ofSeconds(1),
                    0.005,
                    1,
                    true
            )
        }
        assertEquals(exception.message, "'timeout' should be set or be greater than zero, actual: $timeout")
    }
}