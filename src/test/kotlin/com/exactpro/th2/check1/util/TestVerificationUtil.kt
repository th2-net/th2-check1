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

package com.exactpro.th2.check1.util

import com.exactpro.th2.common.grpc.FilterOperation
import com.exactpro.th2.common.grpc.MetadataFilter
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test

internal class TestVerificationUtil {
    @Test
    internal fun convertsMetadataFilterToMetaContainer() {
        val metaContainer = VerificationUtil.toMetaContainer(
            MetadataFilter.newBuilder()
                .putPropertyFilters("keyProp", "42".toSimpleFilter(FilterOperation.EQUAL, true))
                .build()
        )
        Assertions.assertTrue(metaContainer.hasKeyFields()) {
            "MetaContainer does not have key fields: $metaContainer"
        }
        Assertions.assertEquals(mapOf(
            "keyProp" to false /*not transitive*/
        ), metaContainer.keyFields)
    }
}