/*
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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

package com.exactpro.th2.check1.rule

import com.exactpro.th2.common.grpc.RootMessageFilter
import com.exactpro.th2.common.message.toJson
import io.reactivex.Observable
import org.slf4j.Logger

fun AbstractCheckTask.preFilterBy(
    stream: Observable<MessageContainer>,
    protoPreMessageFilter: RootMessageFilter,
    messagePreFilter: SailfishFilter,
    metadataPreFilter: SailfishFilter?,
    logger: Logger,
    onMatch: (ComparisonContainer) -> Unit
): Observable<MessageContainer> =
    stream.map { messageContainer -> // Compare the message with pre-filter
        if (logger.isDebugEnabled) {
            logger.debug("Pre-filtering message with id: {}", messageContainer.protoMessage.metadata.id.toJson())
        }
        val result = matchFilter(messageContainer, messagePreFilter, metadataPreFilter, matchNames = false, significant = false)
        ComparisonContainer(messageContainer, protoPreMessageFilter, result)
            .takeIf(ComparisonContainer::fullyMatches)
            ?.also(onMatch)
            ?.messageContainer ?: MessageContainer.FAKE
    }.filter(MessageContainer::isNotFake)