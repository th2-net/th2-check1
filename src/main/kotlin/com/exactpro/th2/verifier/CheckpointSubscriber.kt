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

package com.exactpro.th2.verifier

import com.exactpro.th2.infra.grpc.Direction.FIRST
import io.vertx.core.impl.ConcurrentHashSet

class CheckpointSubscriber() : AbstractSessionObserver<StreamContainer>() {
    private val sessionSet : Set<StreamContainer> = ConcurrentHashSet()

    override fun onNext(item: StreamContainer) {
        sessionSet.plus(item)
    }

    fun createCheckpoint() : Checkpoint = Checkpoint(sessionSet.associateBy(
        { session -> SessionKey(session.sessionAlias, FIRST) },
        { session -> session.lastMessage.metadata.id.sequence }))
}